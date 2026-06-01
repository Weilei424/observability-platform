package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func newEngineWithSamples(t *testing.T) (*metrics.QueryEngine, *metrics.MemoryStore) {
	t.Helper()
	store := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(store)
	return engine, store
}

func TestQueryEngine_InstantQuery_ReturnsLatestSample(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 2000, 2.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	got, err := engine.InstantQuery(sel, 1500)
	if err != nil {
		t.Fatalf("InstantQuery: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d samples, want 1", len(got))
	}
	if got[0].TimestampMs != 1000 || got[0].Value != 1.0 {
		t.Errorf("got {%d, %v}, want {1000, 1.0}", got[0].TimestampMs, got[0].Value)
	}
}

func TestQueryEngine_InstantQuery_SkipsSeriesWithNoSampleBeforeT(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 5000, 1.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	got, err := engine.InstantQuery(sel, 1000)
	if err != nil {
		t.Fatalf("InstantQuery: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d samples, want 0 (series has no sample at or before t)", len(got))
	}
}

func TestQueryEngine_InstantQuery_NoMatchingSelector_ReturnsEmpty(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)

	sel := metrics.Selector{MetricName: "nonexistent"}
	got, err := engine.InstantQuery(sel, 2000)
	if err != nil {
		t.Fatalf("InstantQuery: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d samples, want 0", len(got))
	}
}

func TestQueryEngine_RangeQuery_StepAligned(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 3000, 3.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	// step=1000ms, ticks at 1000, 2000, 3000
	got, err := engine.RangeQuery(sel, 1000, 3000, 1000)
	if err != nil {
		t.Fatalf("RangeQuery: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	pts := got[0].Points
	// tick 1000 → sample at 1000 (value 1.0)
	// tick 2000 → latest sample at or before 2000 is at 1000 (value 1.0)
	// tick 3000 → sample at 3000 (value 3.0)
	if len(pts) != 3 {
		t.Fatalf("got %d points, want 3", len(pts))
	}
	// TimestampMs on returned points must be the tick, not the raw sample timestamp
	wantTicks := []int64{1000, 2000, 3000}
	wantVals := []float64{1.0, 1.0, 3.0}
	for i, pt := range pts {
		if pt.TimestampMs != wantTicks[i] {
			t.Errorf("pts[%d].TimestampMs = %d, want %d", i, pt.TimestampMs, wantTicks[i])
		}
		if pt.Value != wantVals[i] {
			t.Errorf("pts[%d].Value = %v, want %v", i, pt.Value, wantVals[i])
		}
	}
}

func TestQueryEngine_RangeQuery_TickWithNoSampleOmitted(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	// Only one sample at 3000; ticks at 1000, 2000, 3000
	_ = store.Append(labels, 3000, 3.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	got, err := engine.RangeQuery(sel, 1000, 3000, 1000)
	if err != nil {
		t.Fatalf("RangeQuery: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	// ticks 1000 and 2000 have no sample at or before them — omitted
	// tick 3000 → sample at 3000
	if len(got[0].Points) != 1 {
		t.Errorf("got %d points, want 1", len(got[0].Points))
	}
	if got[0].Points[0].TimestampMs != 3000 {
		t.Errorf("point timestamp = %d, want 3000", got[0].Points[0].TimestampMs)
	}
}

func TestQueryEngine_RangeQuery_SeriesWithZeroPoints_Omitted(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	// Sample at 9000, query range is [1000, 3000] — no tick will have a sample
	_ = store.Append(labels, 9000, 9.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	got, err := engine.RangeQuery(sel, 1000, 3000, 1000)
	if err != nil {
		t.Fatalf("RangeQuery: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d series, want 0", len(got))
	}
}

func TestQueryEngine_RangeQuery_ZeroStep_ReturnsError(t *testing.T) {
	engine, _ := newEngineWithSamples(t)
	sel := metrics.Selector{MetricName: "cpu_usage"}
	_, err := engine.RangeQuery(sel, 1000, 3000, 0)
	if err == nil {
		t.Error("expected error for step=0, got nil")
	}
}

func TestQueryEngine_RangeQuery_EndBeforeStart_ReturnsError(t *testing.T) {
	engine, _ := newEngineWithSamples(t)
	sel := metrics.Selector{MetricName: "cpu_usage"}
	_, err := engine.RangeQuery(sel, 3000, 1000, 1000)
	if err == nil {
		t.Error("expected error for end < start, got nil")
	}
}

func TestQueryEngine_LabelNames_ReturnsSortedUniqueNames(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	lb := mustNewLabels(t, map[string]string{"__name__": "mem_usage", "region": "us"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	names := engine.LabelNames()
	want := []string{"__name__", "host", "region"}
	if len(names) != len(want) {
		t.Fatalf("LabelNames() = %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("LabelNames()[%d] = %q, want %q", i, names[i], w)
		}
	}
}

func TestQueryEngine_LabelNames_DeduplicatesAcrossSeries(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	lb := mustNewLabels(t, map[string]string{"__name__": "mem_usage", "host": "b"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	names := engine.LabelNames()
	// host appears in both series — must appear only once
	count := 0
	for _, n := range names {
		if n == "host" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("host appears %d times in LabelNames(), want 1", count)
	}
}

func TestQueryEngine_LabelNames_EmptyStore_ReturnsNonNilEmpty(t *testing.T) {
	engine, _ := newEngineWithSamples(t)

	names := engine.LabelNames()
	if names == nil {
		t.Error("LabelNames() returned nil, want non-nil empty slice")
	}
	if len(names) != 0 {
		t.Errorf("LabelNames() = %v, want []", names)
	}
}

func TestQueryEngine_LabelValues_ReturnsSortedUniqueValues(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "b"})
	lb := mustNewLabels(t, map[string]string{"__name__": "mem_usage", "host": "a"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	vals := engine.LabelValues("host")
	want := []string{"a", "b"}
	if len(vals) != len(want) {
		t.Fatalf("LabelValues(\"host\") = %v, want %v", vals, want)
	}
	for i, w := range want {
		if vals[i] != w {
			t.Errorf("LabelValues(\"host\")[%d] = %q, want %q", i, vals[i], w)
		}
	}
}

func TestQueryEngine_LabelValues_MetricNameLabel_ReturnsMetricNames(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	lb := mustNewLabels(t, map[string]string{"__name__": "mem_usage"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	vals := engine.LabelValues("__name__")
	want := []string{"cpu_usage", "mem_usage"}
	if len(vals) != len(want) {
		t.Fatalf("LabelValues(\"__name__\") = %v, want %v", vals, want)
	}
	for i, w := range want {
		if vals[i] != w {
			t.Errorf("LabelValues(\"__name__\")[%d] = %q, want %q", i, vals[i], w)
		}
	}
}

func TestQueryEngine_LabelValues_NonexistentLabel_ReturnsNonNilEmpty(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(la, 1000, 1.0)

	vals := engine.LabelValues("nonexistent")
	if vals == nil {
		t.Error("LabelValues returned nil, want non-nil empty slice")
	}
	if len(vals) != 0 {
		t.Errorf("LabelValues(\"nonexistent\") = %v, want []", vals)
	}
}

func TestQueryEngine_Series_ReturnsMatchingLabelSets(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	lb := mustNewLabels(t, map[string]string{"__name__": "mem_usage", "host": "b"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	result := engine.Series([]metrics.Selector{{MetricName: "cpu_usage"}})
	if len(result) != 1 {
		t.Fatalf("Series() len = %d, want 1", len(result))
	}
	name, ok := result[0].Get("__name__")
	if !ok || name != "cpu_usage" {
		t.Errorf("result[0].__name__ = %q, want cpu_usage", name)
	}
}

func TestQueryEngine_Series_UnionAcrossSelectors_Deduplicated(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	_ = store.Append(la, 1000, 1.0)

	// Two different selectors that both match the same series.
	// Deduplication must be by fingerprint, not by selector identity.
	result := engine.Series([]metrics.Selector{
		{MetricName: "cpu_usage"},
		{MetricName: "cpu_usage", Matchers: []metrics.Matcher{{Name: "host", Value: "a"}}},
	})
	if len(result) != 1 {
		t.Errorf("Series() len = %d, want 1 (deduplicated by fingerprint)", len(result))
	}
}

func TestQueryEngine_Series_SortedByMetricName(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "z_metric"})
	lb := mustNewLabels(t, map[string]string{"__name__": "a_metric"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	result := engine.Series([]metrics.Selector{
		{MetricName: "z_metric"},
		{MetricName: "a_metric"},
	})
	if len(result) != 2 {
		t.Fatalf("Series() len = %d, want 2", len(result))
	}
	firstName, _ := result[0].Get("__name__")
	if firstName != "a_metric" {
		t.Errorf("result[0].__name__ = %q, want a_metric (sorted)", firstName)
	}
}

func TestQueryEngine_Series_NoMatchingSelector_ReturnsNonNilEmpty(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(la, 1000, 1.0)

	result := engine.Series([]metrics.Selector{{MetricName: "nonexistent"}})
	if result == nil {
		t.Error("Series() returned nil, want non-nil empty slice")
	}
	if len(result) != 0 {
		t.Errorf("Series() = %v, want []", result)
	}
}
