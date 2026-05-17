package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func mustNewLabels(t *testing.T, m map[string]string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	return l
}

func TestMemoryStore_AppendAndQueryRange_SingleSample(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	if err := store.Append(labels, 1000, 0.5); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got := store.QueryRange(labels.Fingerprint(), 1000, 1000)
	if len(got) != 1 {
		t.Fatalf("got %d samples, want 1", len(got))
	}
	if got[0].TimestampMs != 1000 || got[0].Value != 0.5 {
		t.Errorf("got %+v, want {TimestampMs:1000 Value:0.5}", got[0])
	}
}

func TestMemoryStore_MultipleSamples_SortedByTimestamp(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	_ = store.Append(labels, 2000, 0.8)
	_ = store.Append(labels, 1000, 0.5)

	got := store.QueryRange(labels.Fingerprint(), 0, 3000)
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2", len(got))
	}
	if got[0].TimestampMs != 1000 || got[1].TimestampMs != 2000 {
		t.Errorf("samples not sorted: got timestamps %d, %d", got[0].TimestampMs, got[1].TimestampMs)
	}
}

func TestMemoryStore_OutOfOrder_InsertedAtCorrectPosition(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "http_requests_total"})

	_ = store.Append(labels, 3000, 3.0)
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 2000, 2.0)

	got := store.QueryRange(labels.Fingerprint(), 0, 5000)
	if len(got) != 3 {
		t.Fatalf("got %d samples, want 3", len(got))
	}
	for i, wantTs := range []int64{1000, 2000, 3000} {
		if got[i].TimestampMs != wantTs {
			t.Errorf("got[%d].TimestampMs = %d, want %d", i, got[i].TimestampMs, wantTs)
		}
	}
}

func TestMemoryStore_DuplicateTimestamp_LastWriteWins(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	_ = store.Append(labels, 1000, 0.5)
	_ = store.Append(labels, 1000, 0.9)

	got := store.QueryRange(labels.Fingerprint(), 1000, 1000)
	if len(got) != 1 {
		t.Fatalf("got %d samples, want 1", len(got))
	}
	if got[0].Value != 0.9 {
		t.Errorf("got value %v, want 0.9", got[0].Value)
	}
}

func TestMemoryStore_DifferentLabelSets_SeparateSeries(t *testing.T) {
	store := metrics.NewMemoryStore()
	a := mustNewLabels(t, map[string]string{"__name__": "req", "service": "api"})
	b := mustNewLabels(t, map[string]string{"__name__": "req", "service": "db"})

	_ = store.Append(a, 1000, 1.0)
	_ = store.Append(b, 1000, 2.0)

	gotA := store.QueryRange(a.Fingerprint(), 0, 2000)
	gotB := store.QueryRange(b.Fingerprint(), 0, 2000)

	if len(gotA) != 1 || gotA[0].Value != 1.0 {
		t.Errorf("series A: got %v", gotA)
	}
	if len(gotB) != 1 || gotB[0].Value != 2.0 {
		t.Errorf("series B: got %v", gotB)
	}
}

func TestMemoryStore_QueryRange_BoundaryBehavior(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "temp"})

	_ = store.Append(labels, 100, 1.0)
	_ = store.Append(labels, 200, 2.0)
	_ = store.Append(labels, 300, 3.0)

	got := store.QueryRange(labels.Fingerprint(), 100, 200)
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2", len(got))
	}
	if got[0].TimestampMs != 100 || got[1].TimestampMs != 200 {
		t.Errorf("unexpected samples: %v", got)
	}

	// Outside range: series exists but no samples in [400, 500]
	outside := store.QueryRange(labels.Fingerprint(), 400, 500)
	if outside == nil {
		t.Error("expected empty slice for known series with no samples in range, got nil")
	}
	if len(outside) != 0 {
		t.Errorf("got %d samples outside range, want 0", len(outside))
	}
}

func TestMemoryStore_QueryRange_UnknownSeries_ReturnsNil(t *testing.T) {
	store := metrics.NewMemoryStore()
	got := store.QueryRange(metrics.SeriesID(999), 0, 9999)
	if got != nil {
		t.Errorf("expected nil for unknown series, got %v", got)
	}
}

func TestMemoryStore_SelectSeries_ByMetricName(t *testing.T) {
	store := metrics.NewMemoryStore()
	a := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	b := mustNewLabels(t, map[string]string{"__name__": "mem_usage", "host": "a"})

	_ = store.Append(a, 1000, 1.0)
	_ = store.Append(b, 1000, 2.0)

	sel := metrics.Selector{MetricName: "cpu_usage"}
	got := store.SelectSeries(sel)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	name, _ := got[0].Labels.Get("__name__")
	if name != "cpu_usage" {
		t.Errorf("Labels __name__ = %q, want %q", name, "cpu_usage")
	}
}

func TestMemoryStore_SelectSeries_ByLabelMatcher(t *testing.T) {
	store := metrics.NewMemoryStore()
	a := mustNewLabels(t, map[string]string{"__name__": "req", "service": "api"})
	b := mustNewLabels(t, map[string]string{"__name__": "req", "service": "db"})

	_ = store.Append(a, 1000, 1.0)
	_ = store.Append(b, 1000, 2.0)

	sel := metrics.Selector{
		MetricName: "req",
		Matchers:   []metrics.Matcher{{Name: "service", Value: "api"}},
	}
	got := store.SelectSeries(sel)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	svc, _ := got[0].Labels.Get("service")
	if svc != "api" {
		t.Errorf("service = %q, want %q", svc, "api")
	}
}

func TestMemoryStore_SelectSeries_NoMatch_ReturnsEmpty(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)

	sel := metrics.Selector{MetricName: "nonexistent"}
	got := store.SelectSeries(sel)
	if len(got) != 0 {
		t.Errorf("got %d series, want 0", len(got))
	}
}

func TestMemoryStore_SelectSeries_EmptyMetricName_MatchesAll(t *testing.T) {
	store := metrics.NewMemoryStore()
	a := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	b := mustNewLabels(t, map[string]string{"__name__": "mem_usage"})
	_ = store.Append(a, 1000, 1.0)
	_ = store.Append(b, 1000, 2.0)

	sel := metrics.Selector{}
	got := store.SelectSeries(sel)
	if len(got) != 2 {
		t.Errorf("got %d series, want 2", len(got))
	}
}

func TestMemoryStore_QueryInstant_ReturnsLatestAtOrBefore(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 2000, 2.0)
	_ = store.Append(labels, 3000, 3.0)

	id := labels.Fingerprint()

	s, ok := store.QueryInstant(id, 2500)
	if !ok {
		t.Fatal("expected sample, got none")
	}
	if s.TimestampMs != 2000 || s.Value != 2.0 {
		t.Errorf("got {%d, %v}, want {2000, 2.0}", s.TimestampMs, s.Value)
	}
}

func TestMemoryStore_QueryInstant_ExactMatch(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)

	s, ok := store.QueryInstant(labels.Fingerprint(), 1000)
	if !ok {
		t.Fatal("expected sample, got none")
	}
	if s.TimestampMs != 1000 || s.Value != 1.0 {
		t.Errorf("got {%d, %v}, want {1000, 1.0}", s.TimestampMs, s.Value)
	}
}

func TestMemoryStore_QueryInstant_BeforeFirstSample_ReturnsFalse(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)

	_, ok := store.QueryInstant(labels.Fingerprint(), 500)
	if ok {
		t.Error("expected no sample before first, got one")
	}
}

func TestMemoryStore_QueryInstant_UnknownSeries_ReturnsFalse(t *testing.T) {
	store := metrics.NewMemoryStore()
	_, ok := store.QueryInstant(metrics.SeriesID(999), 1000)
	if ok {
		t.Error("expected false for unknown series, got true")
	}
}
