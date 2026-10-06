package metrics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// WALStore writes each sample to the WAL before forwarding it to a head store.
// Reads delegate to the head store, which in all-in-one is a BlockStore fanning out
// to memory and persisted blocks. Safe for concurrent use.
type WALStore struct {
	w        wal.RecordWriter
	store    walHead
	dataDir  string
	walDir   string
	appendMu sync.Mutex // serializes WAL-write+AppendTracked with FlushBlock's checkpoint calculation

	// testBeforeCheckpoint, if non-nil, is called after block I/O completes but
	// just before appendMu is acquired for checkpoint sampling. Used only in
	// tests to synchronize the WriteRecord→AppendTracked race window.
	testBeforeCheckpoint func()
}

// SetTestBeforeCheckpoint installs a hook that fires at the start of the
// checkpoint phase in FlushBlock, after block I/O but before appendMu is
// acquired. Must not be called after concurrent use begins. Tests only.
func (s *WALStore) SetTestBeforeCheckpoint(fn func()) {
	s.testBeforeCheckpoint = fn
}

// walHead is what WALStore needs from the store behind it: tracked appends, the
// generation guard, the WAL fence, a flush, the sealed-chunk backlog, and reads.
// *BlockStore satisfies it in all-in-one; the ingester's HeadStore, whose
// flushes go to another process, satisfies it in split mode.
type walHead interface {
	ReserveGeneration() (int64, error)
	AppendTrackedGen(labels Labels, tsMs int64, val float64, gen int64, walSeg int) error
	OldestHeadSegment() int
	FlushBlock() (bool, error)
	SealedChunkCount() int
	queryStore
	Source
	SealHeadChunks() int
	FlushBlockContext(ctx context.Context) (bool, error)
}

var (
	_ Store   = (*WALStore)(nil)
	_ walHead = (*BlockStore)(nil)
)

// NewWALStore returns a WALStore backed by w for durability and store for storage.
func NewWALStore(w wal.RecordWriter, store walHead, dataDir string) *WALStore {
	return &WALStore{
		w:       w,
		store:   store,
		dataDir: dataDir,
		walDir:  filepath.Join(dataDir, "metrics", "wal"),
	}
}

// Append writes the WAL record first. If the WAL write fails the sample is not
// written to memory and the error is returned. appendMu is held for the entire
// operation so that FlushBlock's checkpoint calculation cannot observe a state
// where the WAL record exists on disk but its chunk's segment has not yet been
// recorded in memory — which would allow a WAL segment containing live head
// data to be deleted.
func (s *WALStore) Append(labels Labels, tsMs int64, value float64) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	// Reserving the generation first is the exhaustion preflight too: it fails
	// before anything is persisted, so repeated rejections cannot grow an
	// undeletable WAL. The record carries the generation so replay restores it
	// exactly; appendMu serializes reserve, write, and append.
	gen, err := s.store.ReserveGeneration()
	if err != nil {
		return err
	}
	walSeg := s.w.SegmentIndex()
	if err := s.w.WriteRecordGen(labelsToWALPairs(labels), tsMs, value, gen); err != nil {
		return err
	}
	return s.store.AppendTrackedGen(labels, tsMs, value, gen, walSeg)
}

func (s *WALStore) SelectSeries(sel Selector) ([]MatchedSeries, error) {
	return s.store.SelectSeries(sel)
}

func (s *WALStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	return s.store.QueryInstant(id, tMs)
}

func (s *WALStore) QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error) {
	return s.store.QueryRange(id, startMs, endMs)
}

func (s *WALStore) LabelNames() []string          { return s.store.LabelNames() }
func (s *WALStore) LabelValues(n string) []string { return s.store.LabelValues(n) }

var _ Source = (*WALStore)(nil)

func (s *WALStore) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	return s.store.Select(ctx, p)
}

func (s *WALStore) SelectLabelNames(ctx context.Context) ([]string, error) {
	return s.store.SelectLabelNames(ctx)
}

func (s *WALStore) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	return s.store.SelectLabelValues(ctx, name)
}

// ErrDrainIncomplete reports a drain that flushed what it sealed but found the
// head non-empty afterwards: something appended during the drain. The
// ingester's write gate (internal/drain) closes before a drain starts, so
// this is the check that the gate held.
var ErrDrainIncomplete = errors.New("head not empty after the drain")

// Drain flushes the whole head, open chunks included, and checkpoints the WAL,
// all within ctx: what an ingester does on its way out, and what the drain
// route does before an ingester is removed, so a removed ingester leaves
// nothing in its WAL that no reader will see (Phase 6.2). Normal flushes take
// only sealed chunks; Drain seals the open ones first, under appendMu so no
// append lands between the seal and the flush. It succeeds only if the head is
// empty at the end. On any error the unflushed data is still in the WAL and
// replays on the next start.
func (s *WALStore) Drain(ctx context.Context) error {
	s.appendMu.Lock()
	s.store.SealHeadChunks()
	s.appendMu.Unlock()
	for s.store.SealedChunkCount() > 0 {
		wrote, err := s.flushBlock(ctx)
		if err != nil {
			return err
		}
		if !wrote {
			break
		}
	}
	if s.store.OldestHeadSegment() >= 0 {
		return ErrDrainIncomplete
	}
	return nil
}

// FlushBlock flushes sealed chunks to a new immutable block and advances the WAL
// checkpoint. The safe deletion boundary is determined by OldestHeadSegment: the
// oldest WAL segment that contains samples for any current head chunk. Segments
// strictly before that boundary are covered entirely by persisted blocks and can
// be deleted. Returns (false, nil) without touching checkpoint or WAL when no
// sealed chunks exist; (true, nil) when a block was written.
func (s *WALStore) FlushBlock() (bool, error) {
	return s.flushBlock(context.Background())
}

// FlushBlockContext is FlushBlock bounded by ctx: the maintenance loop's
// flush, cancelled when the loop stops.
func (s *WALStore) FlushBlockContext(ctx context.Context) (bool, error) {
	return s.flushBlock(ctx)
}

func (s *WALStore) flushBlock(ctx context.Context) (bool, error) {
	wrote, err := s.store.FlushBlockContext(ctx)
	if err != nil {
		return false, fmt.Errorf("walstore: flush block: %w", err)
	}
	if !wrote {
		return false, nil
	}

	if s.testBeforeCheckpoint != nil {
		s.testBeforeCheckpoint()
	}

	// Hold appendMu while computing the checkpoint boundary so no Append can land
	// a WAL record after OldestHeadSegment is sampled but before its chunk's
	// segment is recorded. Without this lock, FlushBlock could compute
	// safeDelete = S while an in-flight Append has already written its record to
	// segment S but not yet called AppendTracked, causing that segment to be deleted.
	s.appendMu.Lock()
	headFence := s.store.OldestHeadSegment()
	var safeDelete int
	if headFence < 0 {
		safeDelete = s.w.SegmentIndex() - 1
	} else {
		safeDelete = headFence - 1
	}
	s.appendMu.Unlock()

	checkpointPath := filepath.Join(s.dataDir, "metrics", "checkpoint")
	if err := os.WriteFile(checkpointPath, []byte(strconv.Itoa(safeDelete)), 0o644); err != nil {
		return false, fmt.Errorf("walstore: write checkpoint: %w", err)
	}

	if err := deleteWALSegmentsUpTo(s.walDir, safeDelete); err != nil {
		return false, fmt.Errorf("walstore: delete covered WAL segments: %w", err)
	}

	return true, nil
}

// WALBytes returns the total on-disk size of the WAL segment directory, used by
// the maintenance loop as a flush trigger.
func (s *WALStore) WALBytes() (int64, error) {
	return wal.DirSize(s.walDir)
}

// SealedChunkCount reports the head's sealed-chunk backlog, the maintenance
// loop's count-based flush trigger.
func (s *WALStore) SealedChunkCount() int { return s.store.SealedChunkCount() }

func labelsToWALPairs(l Labels) []wal.LabelPair {
	m := l.Map()
	pairs := make([]wal.LabelPair, 0, len(m))
	for name, value := range m {
		pairs = append(pairs, wal.LabelPair{Name: name, Value: value})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}
