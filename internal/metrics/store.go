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

// MatchedSeries is a series ID paired with its label set, returned by SelectSeries.
type MatchedSeries struct {
	id     SeriesID
	Labels Labels
}

// SelectSeries returns all series that match sel. Matching requires:
//   - sel.MetricName matches the series __name__ label (skipped when MetricName is "")
//   - every Matcher in sel.Matchers matches the corresponding label value (AND logic)
//
// No label index is used — this is a full scan.
func (s *MemoryStore) SelectSeries(sel Selector) []MatchedSeries {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []MatchedSeries
	for id, ms := range s.series {
		if sel.MetricName != "" {
			name, _ := ms.labels.Get("__name__")
			if name != sel.MetricName {
				continue
			}
		}
		match := true
		for _, m := range sel.Matchers {
			val, ok := ms.labels.Get(m.Name)
			if !ok || val != m.Value {
				match = false
				break
			}
		}
		if match {
			result = append(result, MatchedSeries{id: id, Labels: ms.labels})
		}
	}
	return result
}

// QueryInstant returns the latest sample with TimestampMs <= tMs for the given series.
// Returns (Sample{}, false) if the series does not exist or has no sample at or before tMs.
func (s *MemoryStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return Sample{}, false
	}

	// Find the first index where TimestampMs > tMs; the sample before it is our answer.
	i := sort.Search(len(ms.samples), func(i int) bool {
		return ms.samples[i].TimestampMs > tMs
	})
	if i == 0 {
		return Sample{}, false
	}
	return ms.samples[i-1], true
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
