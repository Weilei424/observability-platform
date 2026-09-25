package metrics

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// ErrGenerationExhausted is returned by Append when the write-generation counter
// has reached chunk.MaxGeneration. It signals an explicit (astronomically unlikely)
// generation-space exhaustion rather than silently rejecting every write.
var ErrGenerationExhausted = errors.New("metrics: write generation space exhausted")

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
	labels Labels
	chunks []*chunk.Chunk
	// chunkSegs[i] is the WAL segment that was current when chunks[i] was
	// allocated. A chunk's samples all live in that segment or later ones, so
	// the minimum over every chunk still in memory — sealed or not — is the
	// oldest segment still holding unflushed data. One value per series was not
	// enough: a chunk sealed during a flush stays in memory, and the head chunk
	// allocated after it would move the fence past its segments.
	chunkSegs []int
}

// MemoryStore is a chunk-backed in-memory Ingester. Samples are encoded using
// Gorilla/XOR compression inside sealed and unsealed chunks. Safe for concurrent use.
type MemoryStore struct {
	mu      sync.RWMutex
	series  map[SeriesID]*memorySeries
	idx     *index.MemPostings
	nextGen int64 // monotonic write-generation assigned to each appended sample
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		series:  make(map[SeriesID]*memorySeries),
		idx:     index.NewMemPostings(),
		nextGen: 1, // generation 0 is reserved for legacy (pre-generation) chunks
	}
}

// EnsureGenFloor raises the write-generation counter so the next assigned
// generation is at least floor. Called at startup with one past the highest
// generation persisted in any block, so replayed and newly appended samples always
// outrank stored block data for last-write-wins.
func (s *MemoryStore) EnsureGenFloor(floor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if floor > s.nextGen {
		s.nextGen = floor
	}
}

// NextGeneration returns the generation the next append will be assigned.
// Every sample already in memory has a smaller one.
func (s *MemoryStore) NextGeneration() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextGen
}

// GenerationExhausted reports whether the write-generation counter has passed
// chunk.MaxGeneration, so no further append can be assigned a valid generation.
// Lets the WAL layer refuse a doomed write before persisting its record.
func (s *MemoryStore) GenerationExhausted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextGen > chunk.MaxGeneration
}

// Append adds a sample to the series identified by labels.
// Samples may be appended out of order; the chunk encodes them in insertion order
// and QueryRange sorts on read. For equal timestamps, the last written value wins.
func (s *MemoryStore) Append(labels Labels, timestampMs int64, value float64) error {
	return s.appendInternal(labels, timestampMs, value, 0)
}

// AppendTracked is like Append but records walSeg as the WAL segment index in
// which this sample is stored. When a new chunk is allocated, walSeg is
// recorded as that chunk's segment so that OldestHeadSegment can return the
// correct flush boundary. Call this from WALStore.Append; replay code uses
// plain Append.
func (s *MemoryStore) AppendTracked(labels Labels, timestampMs int64, value float64, walSeg int) error {
	return s.appendInternal(labels, timestampMs, value, walSeg)
}

func (s *MemoryStore) appendInternal(labels Labels, timestampMs int64, value float64, walSeg int) error {
	id := SeriesID(labels.Hash())
	s.mu.Lock()
	defer s.mu.Unlock()

	// Refuse before any mutation once the counter would exceed the bound, so
	// exhaustion is an explicit error rather than a silently-rejected append.
	if s.nextGen > chunk.MaxGeneration {
		return ErrGenerationExhausted
	}

	ms, ok := s.series[id]
	if !ok {
		ms = &memorySeries{labels: labels}
		s.series[id] = ms
		s.idx.Add(uint64(id), labelsToIndexPairs(labels))
	}

	// Allocate a new head chunk when none exists or the current one is sealed.
	if len(ms.chunks) == 0 || ms.chunks[len(ms.chunks)-1].Sealed() {
		ms.chunks = append(ms.chunks, chunk.NewChunk())
		ms.chunkSegs = append(ms.chunkSegs, walSeg)
	}

	gen := s.nextGen
	s.nextGen++
	return ms.chunks[len(ms.chunks)-1].Append(timestampMs, value, gen)
}

// OldestHeadSegment returns the smallest WAL segment index in which any chunk
// still in memory was allocated, sealed or not. Returns -1 when no series has
// chunks. Use this to determine the safe WAL deletion boundary after a block flush.
func (s *MemoryStore) OldestHeadSegment() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	oldest := -1
	for _, ms := range s.series {
		for _, seg := range ms.chunkSegs {
			if oldest < 0 || seg < oldest {
				oldest = seg
			}
		}
	}
	return oldest
}

// SetHeadFence sets every in-memory chunk's segment to walSeg. Call this after
// WAL replay to mark the oldest segment containing head-chunk data, so that
// FlushBlock does not delete WAL segments that cover those chunks.
func (s *MemoryStore) SetHeadFence(walSeg int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ms := range s.series {
		for i := range ms.chunkSegs {
			ms.chunkSegs[i] = walSeg
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

// EmptyHeadSeriesIDs returns the IDs of series whose head has been fully drained
// (no chunks remain after a flush). Such a series holds no in-memory samples; it is
// still "active" only while some block covers it. The BlockStore uses this to GC
// series whose last block was removed by retention (see gcEmptyHeadSeries).
func (s *MemoryStore) EmptyHeadSeriesIDs() []SeriesID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []SeriesID
	for id, ms := range s.series {
		if len(ms.chunks) == 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// RemoveEmptySeries drops a series and its postings from the in-memory index, but
// only if its head is still empty (guarding against a sample that arrived after the
// caller decided to GC it). Returns true if the series was removed. This keeps
// SelectSeries and Cardinality from reporting a series with no data anywhere.
func (s *MemoryStore) RemoveEmptySeries(id SeriesID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms, ok := s.series[id]
	if !ok || len(ms.chunks) > 0 {
		return false
	}
	delete(s.series, id)
	s.idx.Delete(uint64(id), labelsToIndexPairs(ms.labels))
	return true
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
	for _, c := range ms.chunks {
		if c.NumSamples() == 0 || c.MinTs() > tMs {
			continue
		}
		it := c.Iterator()
		for it.Next() {
			ts, val := it.At()
			if ts > tMs {
				continue
			}
			gen := it.Gen()
			// Latest timestamp wins; for an equal timestamp the higher generation
			// (later write) wins.
			if !found || ts > best.TimestampMs || (ts == best.TimestampMs && gen > best.Gen) {
				best = Sample{SeriesID: id, TimestampMs: ts, Value: val, Gen: gen}
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
				result = append(result, Sample{SeriesID: id, TimestampMs: ts, Value: val, Gen: it.Gen()})
			}
		}
	}

	return sortAndDedup(result), nil
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

// SealedChunkCount returns the total number of sealed chunks across all series,
// used by the maintenance loop as a flush trigger.
func (s *MemoryStore) SealedChunkCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, ms := range s.series {
		for _, c := range ms.chunks {
			if c.Sealed() {
				n++
			}
		}
	}
	return n
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
		var keepSegs []int
		for i, c := range ms.chunks {
			if _, discard := remove[c]; !discard {
				keep = append(keep, c)
				keepSegs = append(keepSegs, ms.chunkSegs[i])
			}
		}
		ms.chunks = keep
		ms.chunkSegs = keepSegs
		s.series[id] = ms
	}
}

var _ Source = (*MemoryStore)(nil)

// Select implements Source over the head. Series come back in index order, the
// same order SelectSeries returns them.
func (s *MemoryStore) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	ids := s.idx.Select(selectorToIndexMatchers(p.Selector))
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SeriesData, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ms, ok := s.series[SeriesID(id)]
		if !ok {
			continue
		}
		if sd, keep := selectFromChunks(ms.labels, ms.chunks, p); keep {
			out = append(out, sd)
		}
	}
	return out, nil
}

// selectFromChunks answers p for one series from its in-memory chunks. keep is
// false when the series has nothing p asks for.
func selectFromChunks(l Labels, chunks []*chunk.Chunk, p SelectParams) (SeriesData, bool) {
	if p.SeriesOnly && p.AnyTime {
		return SeriesData{Labels: l}, true
	}
	if p.MaxT < p.MinT {
		return SeriesData{}, false
	}
	var samples []Sample
	var anchor *Sample
	wantAnchor := p.Anchor && p.MinT > math.MinInt64
	for _, c := range chunks {
		if c.NumSamples() == 0 {
			continue
		}
		if c.MinTs() > p.MaxT || (!wantAnchor && c.MaxTs() < p.MinT) {
			continue
		}
		it := c.Iterator()
		for it.Next() {
			ts, val := it.At()
			switch {
			case ts >= p.MinT && ts <= p.MaxT:
				samples = append(samples, Sample{SeriesID: SeriesID(l.Hash()), TimestampMs: ts, Value: val, Gen: it.Gen()})
			case wantAnchor && ts < p.MinT:
				cand := Sample{SeriesID: SeriesID(l.Hash()), TimestampMs: ts, Value: val, Gen: it.Gen()}
				anchor = laterSample(anchor, &cand)
			}
		}
	}
	if p.SeriesOnly {
		return SeriesData{Labels: l}, len(samples) > 0
	}
	if len(samples) == 0 && anchor == nil {
		return SeriesData{}, false
	}
	if anchor != nil {
		a := *anchor
		anchor = &a
	}
	return SeriesData{Labels: l, Anchor: anchor, Samples: sortAndDedup(samples)}, true
}

// SelectLabelNames implements Source; the head's index never fails.
func (s *MemoryStore) SelectLabelNames(context.Context) ([]string, error) { return s.LabelNames(), nil }

// SelectLabelValues implements Source; the head's index never fails.
func (s *MemoryStore) SelectLabelValues(_ context.Context, name string) ([]string, error) {
	return s.LabelValues(name), nil
}
