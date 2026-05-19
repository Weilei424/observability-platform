package metrics

import (
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// WALStore implements Ingester by writing each sample to the WAL before
// appending it to the in-memory store. A WAL write failure prevents the sample
// from reaching memory and is returned to the caller. Reads (SelectSeries,
// QueryInstant, QueryRange) delegate entirely to the embedded MemoryStore.
//
// WALStore is safe for concurrent use. The WAL and MemoryStore each
// synchronise internally; callers may call Append from multiple goroutines.
type WALStore struct {
	w   wal.RecordWriter
	mem *MemoryStore
}

var _ Store = (*WALStore)(nil)

// NewWALStore returns a WALStore backed by w for durability and mem for storage.
func NewWALStore(w wal.RecordWriter, mem *MemoryStore) *WALStore {
	return &WALStore{w: w, mem: mem}
}

// Append writes the WAL record first. If the WAL write fails the sample is not
// written to memory and the error is returned.
func (s *WALStore) Append(labels Labels, tsMs int64, value float64) error {
	if err := s.w.WriteRecord(labelsToWALPairs(labels), tsMs, value); err != nil {
		return err
	}
	return s.mem.Append(labels, tsMs, value)
}

func (s *WALStore) SelectSeries(sel Selector) []MatchedSeries {
	return s.mem.SelectSeries(sel)
}

func (s *WALStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool) {
	return s.mem.QueryInstant(id, tMs)
}

func (s *WALStore) QueryRange(id SeriesID, startMs, endMs int64) []Sample {
	return s.mem.QueryRange(id, startMs, endMs)
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
