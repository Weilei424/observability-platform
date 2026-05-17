package metrics

import "fmt"

// QueryEngine executes instant and range queries over a MemoryStore.
type QueryEngine struct {
	store *MemoryStore
}

// NewQueryEngine returns a QueryEngine backed by store.
func NewQueryEngine(store *MemoryStore) *QueryEngine {
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
		sample, ok := e.store.QueryInstant(ms.Labels.Fingerprint(), tMs)
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
			sample, ok := e.store.QueryInstant(id, t)
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
