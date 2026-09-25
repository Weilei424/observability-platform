package metrics

import (
	"context"
	"fmt"
	"sort"
)

// queryStore is the per-series read contract the engine was built on: one
// series, one instant or range at a time. *MemoryStore, *BlockStore, and
// *WALStore still satisfy it. NewQueryEngine reads a store that also implements
// Source through Select and adapts anything else with perSeriesSource.
type queryStore interface {
	SelectSeries(sel Selector) ([]MatchedSeries, error)
	QueryInstant(id SeriesID, tMs int64) (Sample, bool, error)
	QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error)
	LabelNames() []string
	LabelValues(name string) []string
}

// QueryEngine executes instant and range queries over a Source, reading each
// selector once per query rather than once per series per step.
type QueryEngine struct {
	src Source
}

// NewQueryEngine returns a QueryEngine backed by store. A store that implements
// Source is read through its own Select; any other queryStore is adapted.
func NewQueryEngine(store queryStore) *QueryEngine {
	if src, ok := store.(Source); ok {
		return &QueryEngine{src: src}
	}
	return &QueryEngine{src: perSeriesSource{s: store}}
}

// NewQueryEngineFromSource returns a QueryEngine over src. The querier uses it
// with Merge(ingester, store), neither of which is a queryStore.
func NewQueryEngineFromSource(src Source) *QueryEngine {
	return &QueryEngine{src: src}
}

// InstantSample is a single series value at the query instant.
type InstantSample struct {
	Labels      Labels
	TimestampMs int64
	Value       float64
}

// SamplePoint is a step-aligned (tick timestamp, value) pair in a range result.
type SamplePoint struct {
	TimestampMs int64
	Value       float64
}

// RangeSeries holds the step-aligned points for one matching series.
type RangeSeries struct {
	Labels Labels
	Points []SamplePoint
}

// InstantQuery returns the latest sample at or before tMs for each series
// matching sel. Series with no sample at or before tMs are omitted.
func (e *QueryEngine) InstantQuery(sel Selector, tMs int64) ([]InstantSample, error) {
	return e.InstantQueryContext(context.Background(), sel, tMs)
}

// InstantQueryContext is InstantQuery bound to ctx.
func (e *QueryEngine) InstantQueryContext(ctx context.Context, sel Selector, tMs int64) ([]InstantSample, error) {
	series, err := e.src.Select(ctx, SelectParams{Selector: sel, MinT: tMs, MaxT: tMs, Anchor: true})
	if err != nil {
		return nil, err
	}
	result := make([]InstantSample, 0, len(series))
	for _, sd := range series {
		s, ok := latestAtOrBefore(sd, tMs)
		if !ok {
			continue
		}
		result = append(result, InstantSample{Labels: sd.Labels, TimestampMs: s.TimestampMs, Value: s.Value})
	}
	return result, nil
}

// latestAtOrBefore returns sd's latest sample with timestamp <= t: the last
// in-range sample at or before t, else the anchor, which precedes them all.
func latestAtOrBefore(sd SeriesData, t int64) (Sample, bool) {
	i := sort.Search(len(sd.Samples), func(i int) bool { return sd.Samples[i].TimestampMs > t })
	if i > 0 {
		return sd.Samples[i-1], true
	}
	if sd.Anchor != nil && sd.Anchor.TimestampMs <= t {
		return *sd.Anchor, true
	}
	return Sample{}, false
}

// RangeQuery returns step-aligned points for each series matching sel.
// For each tick t = startMs, startMs+stepMs, ..., endMs the value is the
// latest sample at or before t. The returned TimestampMs for each point is
// the tick t, not the original sample timestamp.
// Series with zero points in the range are omitted.
func (e *QueryEngine) RangeQuery(sel Selector, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	return e.RangeQueryContext(context.Background(), sel, startMs, endMs, stepMs)
}

// RangeQueryContext is RangeQuery bound to ctx.
func (e *QueryEngine) RangeQueryContext(ctx context.Context, sel Selector, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}
	series, err := e.src.Select(ctx, SelectParams{Selector: sel, MinT: startMs, MaxT: endMs, Anchor: true})
	if err != nil {
		return nil, err
	}
	result := make([]RangeSeries, 0, len(series))
	for _, sd := range series {
		var points []SamplePoint
		// cur is the latest sample at or before the current tick. Ticks ascend
		// and Samples are sorted, so next only moves forward: one pass per
		// series instead of a full chunk scan per tick.
		cur := sd.Anchor
		next := 0
		for t := startMs; t <= endMs; t += stepMs {
			for next < len(sd.Samples) && sd.Samples[next].TimestampMs <= t {
				cur = &sd.Samples[next]
				next++
			}
			if cur == nil {
				continue
			}
			points = append(points, SamplePoint{TimestampMs: t, Value: cur.Value})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: sd.Labels, Points: points})
	}
	return result, nil
}

// MetadataFilter narrows metadata queries (LabelNames, LabelValues, Series) by
// optional series selectors and/or an optional time range. The zero value
// applies no filtering: all series across all time.
//
//   - Selectors: OR-union of selectors a series must match (any one). nil/empty
//     means every series.
//   - HasTime: when true, only series with at least one sample in
//     [StartMs, EndMs] are considered. Callers that supply only one bound should
//     widen the other to the min/max representable timestamp.
type MetadataFilter struct {
	Selectors []Selector
	StartMs   int64
	EndMs     int64
	HasTime   bool
}

// isUnfiltered reports whether f applies no narrowing, enabling the index
// fast path that serves names/values straight from the store.
func (f MetadataFilter) isUnfiltered() bool {
	return len(f.Selectors) == 0 && !f.HasTime
}

// matchingSeries returns the deduplicated series satisfying f: the OR-union of
// its selectors (or all series when none are given), optionally restricted to
// those with a sample in [StartMs, EndMs]. A storage error is propagated, never
// swallowed, so a failed read is a failed metadata query rather than a
// successful but silently incomplete one.
func (e *QueryEngine) matchingSeries(ctx context.Context, f MetadataFilter) ([]SeriesData, error) {
	sels := f.Selectors
	if len(sels) == 0 {
		sels = []Selector{{}} // empty selector matches every series
	}
	seen := make(map[SeriesID]struct{})
	var out []SeriesData
	for _, sel := range sels {
		matched, err := e.src.Select(ctx, SelectParams{
			Selector:   sel,
			MinT:       f.StartMs,
			MaxT:       f.EndMs,
			SeriesOnly: true,
			AnyTime:    !f.HasTime,
		})
		if err != nil {
			return nil, err
		}
		for _, sd := range matched {
			id := SeriesID(sd.Labels.Hash())
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, sd)
		}
	}
	return out, nil
}

// LabelNames returns a sorted, deduplicated list of label names. With an
// unfiltered MetadataFilter it is served directly by the source's label index;
// otherwise it is computed from the label sets of the matching series. Always
// returns a non-nil slice on success. A storage error is propagated.
func (e *QueryEngine) LabelNames(f MetadataFilter) ([]string, error) {
	return e.LabelNamesContext(context.Background(), f)
}

// LabelNamesContext is LabelNames bound to ctx.
func (e *QueryEngine) LabelNamesContext(ctx context.Context, f MetadataFilter) ([]string, error) {
	if f.isUnfiltered() {
		names, err := e.src.SelectLabelNames(ctx)
		if err != nil {
			return nil, err
		}
		if names == nil {
			return []string{}, nil
		}
		return names, nil
	}
	series, err := e.matchingSeries(ctx, f)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, sd := range series {
		for name := range sd.Labels.Map() {
			set[name] = struct{}{}
		}
	}
	return sortedStringSet(set), nil
}

// LabelValues returns a sorted, deduplicated list of values for name. With an
// unfiltered MetadataFilter it is served directly by the source's label index;
// otherwise it is computed from the matching series. Always returns a non-nil
// slice on success. A storage error is propagated.
func (e *QueryEngine) LabelValues(name string, f MetadataFilter) ([]string, error) {
	return e.LabelValuesContext(context.Background(), name, f)
}

// LabelValuesContext is LabelValues bound to ctx.
func (e *QueryEngine) LabelValuesContext(ctx context.Context, name string, f MetadataFilter) ([]string, error) {
	if f.isUnfiltered() {
		values, err := e.src.SelectLabelValues(ctx, name)
		if err != nil {
			return nil, err
		}
		if values == nil {
			return []string{}, nil
		}
		return values, nil
	}
	series, err := e.matchingSeries(ctx, f)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, sd := range series {
		if v, ok := sd.Labels.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedStringSet(set), nil
}

// sortedStringSet returns the keys of set sorted ascending, never nil.
func sortedStringSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Series returns the label sets for all series matching the filter (the
// OR-union of its selectors, optionally restricted to a time range). Results
// are deduplicated by series fingerprint and sorted by __name__ then remaining
// label pairs (name then value, lexicographic) for stable UI output. Returns a
// non-nil empty slice when no series match. An empty filter (no selectors)
// returns every series; callers that require at least one selector are
// responsible for enforcing that before calling.
func (e *QueryEngine) Series(f MetadataFilter) ([]Labels, error) {
	return e.SeriesContext(context.Background(), f)
}

// SeriesContext is Series bound to ctx.
func (e *QueryEngine) SeriesContext(ctx context.Context, f MetadataFilter) ([]Labels, error) {
	series, err := e.matchingSeries(ctx, f)
	if err != nil {
		return nil, err
	}
	seen := make(map[SeriesID]Labels)
	for _, sd := range series {
		id := SeriesID(sd.Labels.Hash())
		if _, exists := seen[id]; !exists {
			seen[id] = sd.Labels
		}
	}
	// Cache __name__ per entry to avoid repeated Get calls during sort.
	// Pairs are compared directly (no string encoding) so label values
	// containing any byte sequence cannot produce key collisions.
	type entry struct {
		labels Labels
		name   string
		pairs  []Label
	}
	entries := make([]entry, 0, len(seen))
	for _, labels := range seen {
		name, _ := labels.Get("__name__")
		entries = append(entries, entry{labels: labels, name: name, pairs: sortedPairs(labels)})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].name != entries[j].name {
			return entries[i].name < entries[j].name
		}
		// Compare remaining label pairs in sorted order (sortedPairs returns
		// pairs sorted by name). Advance past __name__ on each side
		// independently, since its position depends on what other labels are
		// present.
		pi, pj := entries[i].pairs, entries[j].pairs
		ai, aj := 0, 0
		for {
			for ai < len(pi) && pi[ai].Name == "__name__" {
				ai++
			}
			for aj < len(pj) && pj[aj].Name == "__name__" {
				aj++
			}
			if ai >= len(pi) && aj >= len(pj) {
				return false // equal
			}
			if ai >= len(pi) {
				return true // i has fewer labels
			}
			if aj >= len(pj) {
				return false // j has fewer labels
			}
			if pi[ai].Name != pj[aj].Name {
				return pi[ai].Name < pj[aj].Name
			}
			if pi[ai].Value != pj[aj].Value {
				return pi[ai].Value < pj[aj].Value
			}
			ai++
			aj++
		}
	})
	result := make([]Labels, len(entries))
	for i, e := range entries {
		result[i] = e.labels
	}
	return result, nil
}

// sortedPairs returns l's label pairs sorted by name. Labels no longer
// exposes its internal pairs slice (it is the shared internal/labels type),
// so callers that need sorted pairs reconstruct them from Map().
func sortedPairs(l Labels) []Label {
	m := l.Map()
	pairs := make([]Label, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, Label{Name: k, Value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}
