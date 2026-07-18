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

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 1000, 1000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
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

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 0, 3000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
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

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 0, 5000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
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

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 1000, 1000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
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

	gotA, err := store.QueryRange(metrics.SeriesID(a.Hash()), 0, 2000)
	if err != nil {
		t.Fatalf("QueryRange A: %v", err)
	}
	gotB, err := store.QueryRange(metrics.SeriesID(b.Hash()), 0, 2000)
	if err != nil {
		t.Fatalf("QueryRange B: %v", err)
	}

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

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 100, 200)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2", len(got))
	}
	if got[0].TimestampMs != 100 || got[1].TimestampMs != 200 {
		t.Errorf("unexpected samples: %v", got)
	}

	// Outside range: series exists but no samples in [400, 500]
	outside, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 400, 500)
	if err != nil {
		t.Fatalf("QueryRange outside: %v", err)
	}
	if outside == nil {
		t.Error("expected empty slice for known series with no samples in range, got nil")
	}
	if len(outside) != 0 {
		t.Errorf("got %d samples outside range, want 0", len(outside))
	}
}

func TestMemoryStore_QueryRange_UnknownSeries_ReturnsNil(t *testing.T) {
	store := metrics.NewMemoryStore()
	got, err := store.QueryRange(metrics.SeriesID(999), 0, 9999)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
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
	got, _ := store.SelectSeries(sel)
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
	got, _ := store.SelectSeries(sel)
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
	got, _ := store.SelectSeries(sel)
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
	got, _ := store.SelectSeries(sel)
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

	id := metrics.SeriesID(labels.Hash())

	s, ok, err := store.QueryInstant(id, 2500)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
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

	s, ok, err := store.QueryInstant(metrics.SeriesID(labels.Hash()), 1000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
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

	_, ok, err := store.QueryInstant(metrics.SeriesID(labels.Hash()), 500)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if ok {
		t.Error("expected no sample before first, got one")
	}
}

func TestMemoryStore_QueryInstant_UnknownSeries_ReturnsFalse(t *testing.T) {
	store := metrics.NewMemoryStore()
	_, ok, err := store.QueryInstant(metrics.SeriesID(999), 1000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if ok {
		t.Error("expected false for unknown series, got true")
	}
}

func TestMemoryStore_ChunkBoundary_TwoChunksCreated(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	// 121 samples → first chunk seals at 120, second chunk gets 1 sample
	for i := 0; i < 121; i++ {
		if err := store.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if n := store.ChunkCount(metrics.SeriesID(labels.Hash())); n != 2 {
		t.Errorf("ChunkCount = %d, want 2", n)
	}
}

func TestMemoryStore_ChunkBoundary_AllSamplesQueryable(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	for i := 0; i < 121; i++ {
		_ = store.Append(labels, int64(i*1000), float64(i))
	}

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 0, 121000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 121 {
		t.Fatalf("QueryRange returned %d samples, want 121", len(got))
	}
	for i, s := range got {
		if s.TimestampMs != int64(i*1000) || s.Value != float64(i) {
			t.Errorf("sample %d: got (%d, %f), want (%d, %f)",
				i, s.TimestampMs, s.Value, int64(i*1000), float64(i))
		}
	}
}

func TestMemoryStore_QueryInstantAcrossChunks(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	// Fill first chunk (120 samples, ts 0..119000)
	for i := 0; i < 120; i++ {
		_ = store.Append(labels, int64(i*1000), float64(i))
	}
	// Second chunk: one sample far in the future
	_ = store.Append(labels, 200000, 999.0)

	// Query from second chunk
	s, ok, err := store.QueryInstant(metrics.SeriesID(labels.Hash()), 200000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if !ok {
		t.Fatal("QueryInstant in second chunk: no sample found")
	}
	if s.TimestampMs != 200000 || s.Value != 999.0 {
		t.Errorf("got (%d, %f), want (200000, 999.0)", s.TimestampMs, s.Value)
	}

	// Query from first chunk
	s, ok, err = store.QueryInstant(metrics.SeriesID(labels.Hash()), 50000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if !ok {
		t.Fatal("QueryInstant in first chunk: no sample found")
	}
	if s.TimestampMs != 50000 || s.Value != 50.0 {
		t.Errorf("got (%d, %f), want (50000, 50.0)", s.TimestampMs, s.Value)
	}
}

func TestMemoryStore_DuplicateTimestamp_AcrossChunkBoundary(t *testing.T) {
	store := metrics.NewMemoryStore()
	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})

	// Fill chunk 0 to 119 samples, then append ts=119000 again as sample 120
	// (seals chunk 0), then append ts=119000 a third time into chunk 1.
	for i := 0; i < 119; i++ {
		_ = store.Append(labels, int64(i*1000), float64(i))
	}
	// Sample 119 seals chunk 0
	_ = store.Append(labels, 119000, 119.0)
	// Duplicate ts=119000 goes into chunk 1 — this value should win
	_ = store.Append(labels, 119000, 999.0)

	if n := store.ChunkCount(metrics.SeriesID(labels.Hash())); n != 2 {
		t.Fatalf("expected 2 chunks, got %d", n)
	}

	// QueryRange: only one sample should appear at ts=119000, with value 999.0 (last-write-wins)
	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 119000, 119000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("QueryRange returned %d samples, want 1", len(got))
	}
	if got[0].Value != 999.0 {
		t.Errorf("value = %f, want 999.0 (last-write-wins)", got[0].Value)
	}

	// QueryInstant: should also return 999.0
	s, ok, err := store.QueryInstant(metrics.SeriesID(labels.Hash()), 119000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if !ok {
		t.Fatal("QueryInstant: no sample found")
	}
	if s.Value != 999.0 {
		t.Errorf("QueryInstant value = %f, want 999.0", s.Value)
	}
}

func mustLabels(t *testing.T, m map[string]string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels(%v): %v", m, err)
	}
	return l
}

func TestMemoryStore_SelectSeries_IndexBacked(t *testing.T) {
	s := metrics.NewMemoryStore()
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "api"}), 1, 1)
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "web"}), 1, 1)
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "cpu", "job": "api"}), 1, 1)

	got, _ := s.SelectSeries(metrics.Selector{MetricName: "http", Matchers: []metrics.Matcher{{Name: "job", Value: "api"}}})
	if len(got) != 1 {
		t.Fatalf("SelectSeries matched %d series, want 1", len(got))
	}
	if name, _ := got[0].Labels.Get("__name__"); name != "http" {
		t.Fatalf("matched __name__ = %q, want http", name)
	}
}

func TestMemoryStore_SelectSeries_EmptySelectorReturnsAll(t *testing.T) {
	s := metrics.NewMemoryStore()
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http"}), 1, 1)
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "cpu"}), 1, 1)
	if got, _ := s.SelectSeries(metrics.Selector{}); len(got) != 2 {
		t.Fatalf("empty selector matched %d, want 2", len(got))
	}
}

func TestMemoryStore_Cardinality(t *testing.T) {
	s := metrics.NewMemoryStore()
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "api"}), 1, 1)
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "web"}), 1, 1)
	series, names, pairs := s.Cardinality()
	if series != 2 || names != 2 || pairs != 3 {
		t.Fatalf("cardinality = (%d,%d,%d), want (2,2,3)", series, names, pairs)
	}
}

func TestMemoryStore_LabelNamesAndValues(t *testing.T) {
	s := metrics.NewMemoryStore()
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "api"}), 1, 1)
	_ = s.Append(mustLabels(t, map[string]string{"__name__": "http", "job": "web"}), 1, 1)
	gotNames := s.LabelNames()
	if len(gotNames) != 2 || gotNames[0] != "__name__" || gotNames[1] != "job" {
		t.Fatalf("LabelNames = %v, want [__name__ job]", gotNames)
	}
	if got := s.LabelValues("job"); len(got) != 2 || got[0] != "api" || got[1] != "web" {
		t.Fatalf("LabelValues(job) = %v, want [api web]", got)
	}
}

func TestMemoryStore_SealedChunkCount(t *testing.T) {
	s := metrics.NewMemoryStore()
	lbls, err := metrics.NewLabels(map[string]string{"__name__": "m"})
	if err != nil {
		t.Fatal(err)
	}
	// 120 samples seal exactly one chunk; the 121st opens an unsealed head chunk.
	for i := 0; i < 121; i++ {
		if err := s.Append(lbls, int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if got := s.SealedChunkCount(); got != 1 {
		t.Fatalf("SealedChunkCount = %d, want 1", got)
	}
}
