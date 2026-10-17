package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

var (
	_ metrics.TenantCounter = (*metrics.MemoryStore)(nil)
	_ metrics.TenantCounter = (*metrics.HeadStore)(nil)
	_ metrics.TenantCounter = (*metrics.BlockStore)(nil)
	_ metrics.TenantCounter = (*metrics.WALStore)(nil)
)

func tenantSeries(t *testing.T, tenantID, name string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(map[string]string{"__name__": name})
	if err != nil {
		t.Fatal(err)
	}
	return labels.WithTenant(l, tenantID)
}

func TestTenantCount_MemoryStoreCountsPerTenant(t *testing.T) {
	s := metrics.NewMemoryStore()
	a, b, d := tenantSeries(t, "team-b", "a"), tenantSeries(t, "team-b", "b"), tenantSeries(t, "anonymous", "d")
	for _, l := range []metrics.Labels{a, a, b, d} {
		if err := s.Append(l, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.TenantSeries("team-b"); got != 2 {
		t.Fatalf("team-b = %d, want 2", got)
	}
	if got := s.TenantSeries("anonymous"); got != 1 {
		t.Fatalf("anonymous = %d, want 1", got)
	}
	if got := s.TenantSeries("nobody"); got != 0 {
		t.Fatalf("nobody = %d, want 0", got)
	}
	counts := s.TenantSeriesCounts()
	if len(counts) != 2 || counts["team-b"] != 2 || counts["anonymous"] != 1 {
		t.Fatalf("counts = %v", counts)
	}
	counts["team-b"] = 99 // a copy: must not leak back
	if got := s.TenantSeries("team-b"); got != 2 {
		t.Fatalf("mutating the returned map changed the store: %d", got)
	}
	if !s.HasSeries(metrics.SeriesID(a.Hash())) || s.HasSeries(metrics.SeriesID(12345)) {
		t.Fatal("HasSeries wrong")
	}
}

func TestTenantCount_RemoveEmptySeriesDecrements(t *testing.T) {
	s := metrics.NewMemoryStore()
	l := tenantSeries(t, "team-b", "gone")
	if err := s.Append(l, 1, 1); err != nil {
		t.Fatal(err)
	}
	id := metrics.SeriesID(l.Hash())
	if s.RemoveEmptySeries(id) { // still has a chunk
		t.Fatal("removed a series that holds data")
	}
	if got := s.TenantSeries("team-b"); got != 1 {
		t.Fatalf("after a refused removal = %d, want 1", got)
	}
}

func TestTenantCount_HeadStoreFlushDropsTheCount(t *testing.T) {
	h, err := metrics.OpenHeadStore(t.TempDir(), &recordingSink{target: newSinkTarget(t)}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l := tenantSeries(t, "team-b", "drained")
	for i := range 120 { // exactly one sealed chunk: nothing stays open
		if err := h.Append(l, int64(i)*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.TenantSeries("team-b"); got != 1 {
		t.Fatalf("before flush = %d, want 1", got)
	}
	if !h.HasSeries(metrics.SeriesID(l.Hash())) {
		t.Fatal("HasSeries false before flush")
	}
	if _, err := h.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	if got := h.TenantSeries("team-b"); got != 0 {
		t.Fatalf("after flush = %d, want 0", got)
	}
	if c := h.TenantSeriesCounts(); len(c) != 0 {
		t.Fatalf("counts after flush = %v, want empty (zero entries omitted)", c)
	}
	if h.HasSeries(metrics.SeriesID(l.Hash())) {
		t.Fatal("HasSeries true after the series was dropped")
	}
}

func TestTenantCount_BlockStoreReportsHeadOnly(t *testing.T) {
	bs := newSinkTarget(t)
	l := tenantSeries(t, "team-b", "x")
	if err := bs.Append(l, 1, 1); err != nil {
		t.Fatal(err)
	}
	if bs.TenantSeries("team-b") != 1 || !bs.HasSeries(metrics.SeriesID(l.Hash())) || bs.TenantSeriesCounts()["team-b"] != 1 {
		t.Fatal("BlockStore pass-through wrong")
	}
}
