package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// WALStore writes each sample to the WAL before forwarding it to a BlockStore.
// Reads delegate to the embedded BlockStore, which fans out to memory and
// persisted blocks. Safe for concurrent use.
type WALStore struct {
	w       wal.RecordWriter
	store   *BlockStore
	dataDir string
	walDir  string
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
// written to memory and the error is returned.
func (s *WALStore) Append(labels Labels, tsMs int64, value float64) error {
	if err := s.w.WriteRecord(labelsToWALPairs(labels), tsMs, value); err != nil {
		return err
	}
	return s.store.Append(labels, tsMs, value)
}

func (s *WALStore) SelectSeries(sel Selector) []MatchedSeries {
	return s.store.SelectSeries(sel)
}

func (s *WALStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool) {
	return s.store.QueryInstant(id, tMs)
}

func (s *WALStore) QueryRange(id SeriesID, startMs, endMs int64) []Sample {
	return s.store.QueryRange(id, startMs, endMs)
}

// FlushBlock flushes sealed chunks to a new immutable block, writes a checkpoint
// recording the current WAL segment, then deletes WAL segments covered by the
// checkpoint.
func (s *WALStore) FlushBlock() error {
	segIdx := s.w.SegmentIndex()

	if err := s.store.FlushBlock(); err != nil {
		return fmt.Errorf("walstore: flush block: %w", err)
	}

	checkpointPath := filepath.Join(s.dataDir, "metrics", "checkpoint")
	if err := os.WriteFile(checkpointPath, []byte(strconv.Itoa(segIdx)), 0o644); err != nil {
		return fmt.Errorf("walstore: write checkpoint: %w", err)
	}

	if err := deleteWALSegmentsUpTo(s.walDir, segIdx); err != nil {
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
