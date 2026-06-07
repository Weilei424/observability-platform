package metrics

import (
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
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
	labels Labels
	chunks []*chunk.Chunk
}

// MemoryStore is a chunk-backed in-memory Ingester. Samples are encoded using
// Gorilla/XOR compression inside sealed and unsealed chunks. Safe for concurrent use.
type MemoryStore struct {
	mu     sync.RWMutex
	series map[SeriesID]*memorySeries
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{series: make(map[SeriesID]*memorySeries)}
}

// Append adds a sample to the series identified by labels.
// Samples may be appended out of order; the chunk encodes them in insertion order
// and QueryRange sorts on read. For equal timestamps, the last written value wins.
func (s *MemoryStore) Append(labels Labels, timestampMs int64, value float64) error {
	id := labels.Fingerprint()
	s.mu.Lock()
	defer s.mu.Unlock()

	ms, ok := s.series[id]
	if !ok {
		ms = &memorySeries{labels: labels}
		s.series[id] = ms
	}

	// Ensure an open head chunk exists
	if len(ms.chunks) == 0 || ms.chunks[len(ms.chunks)-1].Sealed() {
		ms.chunks = append(ms.chunks, chunk.NewChunk())
	}

	return ms.chunks[len(ms.chunks)-1].Append(timestampMs, value)
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
// For equal timestamps, the sample written last (last-write-wins) is returned.
func (s *MemoryStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return Sample{}, false
	}

	var best Sample
	found := false
	// Chunks are iterated in insertion (oldest-first) order, which ensures that
	// later-inserted samples in later chunks overwrite earlier ones for equal
	// timestamps, preserving last-write-wins semantics.
	for _, c := range ms.chunks {
		if c.NumSamples() == 0 || c.MinTs() > tMs {
			continue
		}
		it := c.Iterator()
		for it.Next() {
			ts, val := it.At()
			// Accept this sample if it is the latest (or equal-ts latest inserted) at or before tMs
			if ts <= tMs && (!found || ts >= best.TimestampMs) {
				best = Sample{SeriesID: id, TimestampMs: ts, Value: val}
				found = true
			}
		}
		if it.Err() != nil {
			// In Phase 3.1 this cannot happen (in-memory buffers are always valid).
			// Once chunks are disk-backed (Phase 3.2), callers should inspect this.
			_ = it.Err()
		}
	}
	return best, found
}

// QueryRange returns samples for series id where startMs <= TimestampMs <= endMs.
// Results are sorted by timestamp. For duplicate timestamps, the last-written value is kept.
// Returns a non-nil empty slice for a known series with no samples in range.
// Returns nil for an unknown series.
func (s *MemoryStore) QueryRange(id SeriesID, startMs, endMs int64) []Sample {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return nil
	}

	result := make([]Sample, 0)
	for _, c := range ms.chunks {
		if c.NumSamples() == 0 || c.MinTs() > endMs || c.MaxTs() < startMs {
			continue
		}
		it := c.Iterator()
		for it.Next() {
			ts, val := it.At()
			if ts >= startMs && ts <= endMs {
				result = append(result, Sample{SeriesID: id, TimestampMs: ts, Value: val})
			}
		}
		if it.Err() != nil {
			_ = it.Err()
		}
	}

	// Stable sort preserves insertion order among equal timestamps (needed for dedup below)
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].TimestampMs < result[j].TimestampMs
	})

	// Dedup: for equal timestamps keep the last occurrence (last-write-wins via stable sort)
	if len(result) > 1 {
		// deduped aliases result's backing array; i always advances ahead of len(deduped),
		// so no element is read after it has been overwritten — safe in-place compaction.
		deduped := result[:1]
		for i := 1; i < len(result); i++ {
			if result[i].TimestampMs == deduped[len(deduped)-1].TimestampMs {
				deduped[len(deduped)-1] = result[i]
			} else {
				deduped = append(deduped, result[i])
			}
		}
		result = deduped
	}

	return result
}

// ChunkCount returns the number of chunks allocated for the given series.
// Useful for verifying chunk boundary behavior in tests.
func (s *MemoryStore) ChunkCount(id SeriesID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ms, ok := s.series[id]
	if !ok {
		return 0
	}
	return len(ms.chunks)
}

// SeriesChunks is a snapshot of one series and its sealed chunks, used
// to transfer data from MemoryStore to a block writer.
type SeriesChunks struct {
	ID     SeriesID
	Labels Labels
	Chunks []*chunk.Chunk
}

// SealedChunksSnapshot returns a snapshot of all sealed chunks across all series.
// The returned chunks are immutable (sealed) and safe to read without holding the lock.
// Returns nil if no sealed chunks exist.
func (s *MemoryStore) SealedChunksSnapshot() []SeriesChunks {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []SeriesChunks
	for id, ms := range s.series {
		var sealed []*chunk.Chunk
		for _, c := range ms.chunks {
			if c.Sealed() {
				sealed = append(sealed, c)
			}
		}
		if len(sealed) > 0 {
			result = append(result, SeriesChunks{
				ID:     id,
				Labels: ms.labels,
				Chunks: sealed,
			})
		}
	}
	return result
}

// DiscardSealedChunks removes all sealed chunks from every series, retaining
// only the unsealed head chunk. Call this only after sealed chunks have been
// safely written to a block.
func (s *MemoryStore) DiscardSealedChunks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ms := range s.series {
		var head []*chunk.Chunk
		for _, c := range ms.chunks {
			if !c.Sealed() {
				head = append(head, c)
			}
		}
		ms.chunks = head
		s.series[id] = ms
	}
}
