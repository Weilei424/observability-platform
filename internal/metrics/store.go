package metrics

import (
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Ingester accepts metric samples for storage.
type Ingester interface {
	Append(labels Labels, timestampMs int64, value float64) error
}

// Querier retrieves metric samples from storage.
type Querier interface {
	QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error)
}

// Store combines write and read access to metric storage.
type Store interface {
	Ingester
	Querier
}

type memorySeries struct {
	labels  Labels
	chunks  []*chunk.Chunk
	headSeg int // WAL segment index when the current head chunk was allocated
}

// MemoryStore is a chunk-backed in-memory Ingester. Samples are encoded using
// Gorilla/XOR compression inside sealed and unsealed chunks. Safe for concurrent use.
type MemoryStore struct {
	mu     sync.RWMutex
	series map[SeriesID]*memorySeries
	idx    *index.MemPostings
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		series: make(map[SeriesID]*memorySeries),
		idx:    index.NewMemPostings(),
	}
}

// Append adds a sample to the series identified by labels.
// Samples may be appended out of order; the chunk encodes them in insertion order
// and QueryRange sorts on read. For equal timestamps, the last written value wins.
func (s *MemoryStore) Append(labels Labels, timestampMs int64, value float64) error {
	return s.appendInternal(labels, timestampMs, value, 0)
}

// AppendTracked is like Append but records walSeg as the WAL segment index in
// which this sample is stored. When a new head chunk is allocated, headSeg is
// set to walSeg so that OldestHeadSegment can return the correct flush boundary.
// Call this from WALStore.Append; replay code uses plain Append.
func (s *MemoryStore) AppendTracked(labels Labels, timestampMs int64, value float64, walSeg int) error {
	return s.appendInternal(labels, timestampMs, value, walSeg)
}

func (s *MemoryStore) appendInternal(labels Labels, timestampMs int64, value float64, walSeg int) error {
	id := labels.Fingerprint()
	s.mu.Lock()
	defer s.mu.Unlock()

	ms, ok := s.series[id]
	if !ok {
		ms = &memorySeries{labels: labels}
		s.series[id] = ms
		s.idx.Add(uint64(id), labelsToIndexPairs(labels))
	}

	// Allocate a new head chunk when none exists or the current one is sealed.
	if len(ms.chunks) == 0 || ms.chunks[len(ms.chunks)-1].Sealed() {
		ms.chunks = append(ms.chunks, chunk.NewChunk())
		ms.headSeg = walSeg
	}

	return ms.chunks[len(ms.chunks)-1].Append(timestampMs, value)
}

// OldestHeadSegment returns the smallest WAL segment index across all series
// whose current head chunk was allocated. Returns -1 when no series has chunks.
// Use this to determine the safe WAL deletion boundary after a block flush.
func (s *MemoryStore) OldestHeadSegment() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	oldest := -1
	for _, ms := range s.series {
		if len(ms.chunks) == 0 {
			continue
		}
		if oldest < 0 || ms.headSeg < oldest {
			oldest = ms.headSeg
		}
	}
	return oldest
}

// SetHeadFence sets headSeg to walSeg for every series that currently has chunks.
// Call this after WAL replay to mark the oldest segment containing head-chunk data,
// so that FlushBlock does not delete WAL segments that cover those head chunks.
func (s *MemoryStore) SetHeadFence(walSeg int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ms := range s.series {
		if len(ms.chunks) > 0 {
			ms.headSeg = walSeg
		}
	}
}

// MatchedSeries is a series ID paired with its label set, returned by SelectSeries.
type MatchedSeries struct {
	id     SeriesID
	Labels Labels
}

// SelectSeries returns all series matching sel, resolved through the in-memory
// label index (postings intersection for equality matchers). The error is always
// nil; it exists to satisfy the queryStore contract shared with BlockStore,
// whose persisted reads can fail.
func (s *MemoryStore) SelectSeries(sel Selector) ([]MatchedSeries, error) {
	ids := s.idx.Select(selectorToIndexMatchers(sel))
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]MatchedSeries, 0, len(ids))
	for _, id := range ids {
		ms, ok := s.series[SeriesID(id)]
		if !ok {
			continue
		}
		result = append(result, MatchedSeries{id: SeriesID(id), Labels: ms.labels})
	}
	return result, nil
}

// LabelNames returns all label names present in memory, sorted ascending.
func (s *MemoryStore) LabelNames() []string { return s.idx.LabelNames() }

// LabelValues returns all values for name present in memory, sorted ascending.
func (s *MemoryStore) LabelValues(name string) []string { return s.idx.LabelValues(name) }

// Cardinality returns distinct counts of series, label names, and label pairs.
func (s *MemoryStore) Cardinality() (series, names, pairs int) {
	return s.idx.SeriesCount(), s.idx.LabelNameCount(), s.idx.LabelPairCount()
}

func labelsToIndexPairs(l Labels) []index.Pair {
	m := l.Map()
	out := make([]index.Pair, 0, len(m))
	for name, val := range m {
		out = append(out, index.Pair{Name: name, Value: val})
	}
	return out
}

// selectorToIndexMatchers folds the selector's MetricName into a __name__
// matcher and appends all equality matchers. An empty selector yields nil,
// which MemPostings.Select treats as "match all".
func selectorToIndexMatchers(sel Selector) []index.Pair {
	var out []index.Pair
	if sel.MetricName != "" {
		out = append(out, index.Pair{Name: "__name__", Value: sel.MetricName})
	}
	for _, m := range sel.Matchers {
		out = append(out, index.Pair{Name: m.Name, Value: m.Value})
	}
	return out
}

// QueryInstant returns the latest sample with TimestampMs <= tMs for the given series.
// Returns (Sample{}, false, nil) if the series does not exist or has no sample at or before tMs.
// For equal timestamps, the sample written last (last-write-wins) is returned.
func (s *MemoryStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return Sample{}, false, nil
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
	}
	return best, found, nil
}

// QueryRange returns samples for series id where startMs <= TimestampMs <= endMs.
// Results are sorted by timestamp. For duplicate timestamps, the last-written value is kept.
// Returns a non-nil empty slice for a known series with no samples in range.
// Returns nil, nil for an unknown series.
func (s *MemoryStore) QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ms, ok := s.series[id]
	if !ok {
		return nil, nil
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

	return result, nil
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

// DiscardSealedChunks removes exactly the chunks listed in toDiscard from their
// respective series. Only chunks pointer-equal to those in the snapshot are
// removed, so any chunk that sealed after the snapshot was taken is preserved.
// Call this only after the listed chunks have been safely written to a block.
func (s *MemoryStore) DiscardSealedChunks(toDiscard []SeriesChunks) {
	remove := make(map[*chunk.Chunk]struct{})
	for _, sc := range toDiscard {
		for _, c := range sc.Chunks {
			remove[c] = struct{}{}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ms := range s.series {
		var keep []*chunk.Chunk
		for _, c := range ms.chunks {
			if _, discard := remove[c]; !discard {
				keep = append(keep, c)
			}
		}
		ms.chunks = keep
		s.series[id] = ms
	}
}
