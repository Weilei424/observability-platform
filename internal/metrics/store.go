package metrics

import (
	"sort"
	"sync"
)

// Ingester accepts metric samples for storage.
type Ingester interface {
	Append(labels Labels, timestampMs int64, value float64) error
}

// Querier retrieves metric samples from storage.
type Querier interface {
	QueryRange(id SeriesID, startMs, endMs int64) []Sample
}

// Store combines write and read access to metric storage.
type Store interface {
	Ingester
	Querier
}

type memorySeries struct {
	labels  Labels  // retained for label lookup by the query engine (Phase 1.3)
	samples []Sample
}

// MemoryStore is an in-memory Ingester. Samples are kept sorted by
// TimestampMs per series. Safe for concurrent use.
type MemoryStore struct {
	mu     sync.RWMutex
	series map[SeriesID]*memorySeries
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{series: make(map[SeriesID]*memorySeries)}
}

// Append adds a sample to the series identified by labels.
// Out-of-order samples are inserted at the correct timestamp position.
// If a sample already exists at timestampMs, its value is overwritten (last-write-wins).
func (s *MemoryStore) Append(labels Labels, timestampMs int64, value float64) error {
	id := labels.Fingerprint()
	s.mu.Lock()
	defer s.mu.Unlock()

	ms, ok := s.series[id]
	if !ok {
		ms = &memorySeries{labels: labels}
		s.series[id] = ms
	}

	i := sort.Search(len(ms.samples), func(i int) bool {
		return ms.samples[i].TimestampMs >= timestampMs
	})

	if i < len(ms.samples) && ms.samples[i].TimestampMs == timestampMs {
		ms.samples[i].Value = value
		return nil
	}

	ms.samples = append(ms.samples, Sample{})
	copy(ms.samples[i+1:], ms.samples[i:])
	ms.samples[i] = Sample{SeriesID: id, TimestampMs: timestampMs, Value: value}
	return nil
}

// QueryRange returns a copy of samples for series id where startMs <= TimestampMs <= endMs.
func (s *MemoryStore) QueryRange(id SeriesID, startMs, endMs int64) []Sample {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return nil
	}

	lo := sort.Search(len(ms.samples), func(i int) bool {
		return ms.samples[i].TimestampMs >= startMs
	})
	hi := sort.Search(len(ms.samples), func(i int) bool {
		return ms.samples[i].TimestampMs > endMs
	})

	if lo >= hi {
		return []Sample{}
	}
	result := make([]Sample, hi-lo)
	copy(result, ms.samples[lo:hi])
	return result
}
