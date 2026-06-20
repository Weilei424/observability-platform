package metrics

import (
	"fmt"
	"sort"
)

// queryStore is the read interface QueryEngine depends on.
// *MemoryStore, *BlockStore, and *WALStore all implement it.
type queryStore interface {
	SelectSeries(sel Selector) []MatchedSeries
	QueryInstant(id SeriesID, tMs int64) (Sample, bool, error)
	QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error)
	LabelNames() []string
	LabelValues(name string) []string
}

// QueryEngine executes instant and range queries over a queryStore.
type QueryEngine struct {
	store queryStore
}

// NewQueryEngine returns a QueryEngine backed by store.
func NewQueryEngine(store queryStore) *QueryEngine {
	return &QueryEngine{store: store}
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
	matched := e.store.SelectSeries(sel)
	result := make([]InstantSample, 0, len(matched))
	for _, ms := range matched {
		sample, ok, err := e.store.QueryInstant(ms.Labels.Fingerprint(), tMs)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		result = append(result, InstantSample{
			Labels:      ms.Labels,
			TimestampMs: sample.TimestampMs,
			Value:       sample.Value,
		})
	}
	return result, nil
}

// RangeQuery returns step-aligned points for each series matching sel.
// For each tick t = startMs, startMs+stepMs, ..., endMs the value is the
// latest sample at or before t. The returned TimestampMs for each point is
// the tick t, not the original sample timestamp.
// Series with zero points in the range are omitted.
func (e *QueryEngine) RangeQuery(sel Selector, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}

	matched := e.store.SelectSeries(sel)
	result := make([]RangeSeries, 0, len(matched))

	for _, ms := range matched {
		var points []SamplePoint
		id := ms.Labels.Fingerprint()
		for t := startMs; t <= endMs; t += stepMs {
			sample, ok, err := e.store.QueryInstant(id, t)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			points = append(points, SamplePoint{TimestampMs: t, Value: sample.Value})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: ms.Labels, Points: points})
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
// those active within [StartMs, EndMs]. A storage error encountered while
// testing activity is propagated, never swallowed, so that a corrupt chunk or
// I/O failure surfaces as a failed metadata query rather than a successful but
// silently-incomplete one.
func (e *QueryEngine) matchingSeries(f MetadataFilter) ([]MatchedSeries, error) {
	sels := f.Selectors
	if len(sels) == 0 {
		sels = []Selector{{}} // empty selector matches every series
	}
	seen := make(map[SeriesID]struct{})
	var out []MatchedSeries
	for _, sel := range sels {
		for _, ms := range e.store.SelectSeries(sel) {
			id := ms.Labels.Fingerprint()
			if _, ok := seen[id]; ok {
				continue
			}
			if f.HasTime {
				samples, err := e.store.QueryRange(id, f.StartMs, f.EndMs)
				if err != nil {
					return nil, err
				}
				if len(samples) == 0 {
					continue
				}
			}
			seen[id] = struct{}{}
			out = append(out, ms)
		}
	}
	return out, nil
}

// LabelNames returns a sorted, deduplicated list of label names. With an
// unfiltered MetadataFilter it is served directly by the store's label index;
// otherwise it is computed from the label sets of the matching series. Always
// returns a non-nil slice on success. A storage error from time-range filtering
// is propagated.
func (e *QueryEngine) LabelNames(f MetadataFilter) ([]string, error) {
	if f.isUnfiltered() {
		names := e.store.LabelNames()
		if names == nil {
			return []string{}, nil
		}
		return names, nil
	}
	series, err := e.matchingSeries(f)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, ms := range series {
		for name := range ms.Labels.Map() {
			set[name] = struct{}{}
		}
	}
	return sortedStringSet(set), nil
}

// LabelValues returns a sorted, deduplicated list of values for name. With an
// unfiltered MetadataFilter it is served directly by the store's label index;
// otherwise it is computed from the matching series. Always returns a non-nil
// slice on success. A storage error from time-range filtering is propagated.
func (e *QueryEngine) LabelValues(name string, f MetadataFilter) ([]string, error) {
	if f.isUnfiltered() {
		values := e.store.LabelValues(name)
		if values == nil {
			return []string{}, nil
		}
		return values, nil
	}
	series, err := e.matchingSeries(f)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, ms := range series {
		if v, ok := ms.Labels.Get(name); ok {
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
	series, err := e.matchingSeries(f)
	if err != nil {
		return nil, err
	}
	seen := make(map[SeriesID]Labels)
	for _, ms := range series {
		id := ms.Labels.Fingerprint()
		if _, exists := seen[id]; !exists {
			seen[id] = ms.Labels
		}
	}
	// Cache __name__ per entry to avoid repeated Get calls during sort.
	// Pairs are compared directly (no string encoding) so label values
	// containing any byte sequence cannot produce key collisions.
	type entry struct {
		labels Labels
		name   string
	}
	entries := make([]entry, 0, len(seen))
	for _, labels := range seen {
		name, _ := labels.Get("__name__")
		entries = append(entries, entry{labels: labels, name: name})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].name != entries[j].name {
			return entries[i].name < entries[j].name
		}
		// Compare remaining label pairs in sorted order (Labels.pairs is sorted
		// by name). Advance past __name__ on each side independently, since its
		// position depends on what other labels are present.
		pi, pj := entries[i].labels.pairs, entries[j].labels.pairs
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
