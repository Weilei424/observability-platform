package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// WALStore writes each sample to the WAL before forwarding it to a BlockStore.
// Reads delegate to the embedded BlockStore, which fans out to memory and
// persisted blocks. Safe for concurrent use.
type WALStore struct {
	w        wal.RecordWriter
	store    *BlockStore
	dataDir  string
	walDir   string
	appendMu sync.Mutex // serializes WAL-write+AppendTracked with FlushBlock's checkpoint calculation
}

var _ Store = (*WALStore)(nil)

// NewWALStore returns a WALStore backed by w for durability and store for storage.
func NewWALStore(w wal.RecordWriter, store *BlockStore, dataDir string) *WALStore {
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
// where the WAL record exists on disk but headSeg has not yet been updated in
// memory — which would allow a WAL segment containing live head data to be deleted.
func (s *WALStore) Append(labels Labels, tsMs int64, value float64) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	walSeg := s.w.SegmentIndex()
	if err := s.w.WriteRecord(labelsToWALPairs(labels), tsMs, value); err != nil {
		return err
	}
	return s.store.AppendTracked(labels, tsMs, value, walSeg)
}

func (s *WALStore) SelectSeries(sel Selector) []MatchedSeries {
	return s.store.SelectSeries(sel)
}

func (s *WALStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	return s.store.QueryInstant(id, tMs)
}

func (s *WALStore) QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error) {
	return s.store.QueryRange(id, startMs, endMs)
}

// FlushBlock flushes sealed chunks to a new immutable block and advances the WAL
// checkpoint. The safe deletion boundary is determined by OldestHeadSegment: the
// oldest WAL segment that contains samples for any current head chunk. Segments
// strictly before that boundary are covered entirely by persisted blocks and can
// be deleted. Returns nil without touching checkpoint or WAL when no sealed chunks
// exist.
func (s *WALStore) FlushBlock() error {
	wrote, err := s.store.FlushBlock()
	if err != nil {
		return fmt.Errorf("walstore: flush block: %w", err)
	}
	if !wrote {
		return nil
	}

	// Hold appendMu while computing the checkpoint boundary so no Append can land
	// a WAL record after OldestHeadSegment is sampled but before headSeg is set.
	// Without this lock, FlushBlock could compute safeDelete = S while an
	// in-flight Append has already written its record to segment S but not yet
	// called AppendTracked, causing that segment to be deleted.
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
		return fmt.Errorf("walstore: write checkpoint: %w", err)
	}

	if err := deleteWALSegmentsUpTo(s.walDir, safeDelete); err != nil {
		return fmt.Errorf("walstore: delete covered WAL segments: %w", err)
	}

	return nil
}

func labelsToWALPairs(l Labels) []wal.LabelPair {
	m := l.Map()
	pairs := make([]wal.LabelPair, 0, len(m))
	for name, value := range m {
		pairs = append(pairs, wal.LabelPair{Name: name, Value: value})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}
