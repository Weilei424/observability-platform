package metrics_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

type tenantAppender interface {
	Append(l metrics.Labels, ts int64, v float64) error
}

// fillTwoTenants holds up{job="a"} for the default tenant (value 1) and for
// team-b (value 2), plus a team-b-only series.
func fillTwoTenants(t *testing.T, s tenantAppender) {
	t.Helper()
	add := func(m map[string]string, id string, v float64) {
		l, err := metrics.NewLabels(m)
		if err != nil {
			t.Fatal(err)
		}
		// 120 samples fill a chunk, so a block flush has something sealed.
		for i := range 120 {
			if err := s.Append(labels.WithTenant(l, id), 1000+int64(i), v); err != nil {
				t.Fatal(err)
			}
		}
	}
	add(map[string]string{"__name__": "up", "job": "a"}, tenant.Default, 1)
	add(map[string]string{"__name__": "up", "job": "a"}, "team-b", 2)
	add(map[string]string{"__name__": "only_b", "secret": "x"}, "team-b", 3)
}

func twoTenantStore(t *testing.T) *metrics.MemoryStore {
	t.Helper()
	s := metrics.NewMemoryStore()
	fillTwoTenants(t, s)
	return s
}

// tenantSources builds every local Source the wrapper is deployed over, each
// holding the same two-tenant data. The block store has it flushed into a
// block; the head and WAL stores hold it in the head.
func tenantSources(t *testing.T) map[string]metrics.Source {
	t.Helper()
	out := map[string]metrics.Source{"memory": twoTenantStore(t)}

	flushed, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flushed.Close() })
	fillTwoTenants(t, flushed)
	if ok, err := flushed.FlushBlock(); err != nil || !ok {
		t.Fatalf("FlushBlock: %v, %v", ok, err)
	}
	out["block-flushed"] = flushed

	mixed, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mixed.Close() })
	fillTwoTenants(t, mixed)
	if ok, err := mixed.FlushBlock(); err != nil || !ok {
		t.Fatalf("FlushBlock: %v, %v", ok, err)
	}
	fillTwoTenants(t, mixed) // same samples again, now in the head
	out["block-mixed"] = mixed

	dir := t.TempDir()
	hs, err := metrics.OpenHeadStore(dir, &recordingSink{target: newSinkTarget(t)}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fillTwoTenants(t, hs)
	out["head"] = hs

	dir2 := t.TempDir()
	hs2, err := metrics.OpenHeadStore(dir2, &recordingSink{target: newSinkTarget(t)}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(filepath.Join(dir2, "metrics", "wal"), 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	ws := metrics.NewWALStore(w, hs2, dir2)
	fillTwoTenants(t, ws)
	out["wal"] = ws
	return out
}

var everything = metrics.SelectParams{Selector: metrics.Selector{MetricName: "up"}, MinT: 0, MaxT: 2000}

func TestTenantSourceSeesOnlyItsOwnSeries(t *testing.T) {
	for name, base := range tenantSources(t) {
		t.Run(name, func(t *testing.T) {
			src := metrics.TenantSource(base)
			for _, tc := range []struct {
				id   string
				want float64
			}{{tenant.Default, 1}, {"team-b", 2}} {
				got, err := src.Select(tenant.Inject(context.Background(), tc.id), everything)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != 1 || got[0].Samples[0].Value != tc.want {
					t.Fatalf("%s: got %+v, want one series with value %v", tc.id, got, tc.want)
				}
				if _, ok := got[0].Labels.Get(tenant.Label); ok {
					t.Fatalf("%s: the tenant label leaked into %v", tc.id, got[0].Labels.Map())
				}
			}
		})
	}
}

func TestTenantSourceLabelNamesAndValuesArePerTenant(t *testing.T) {
	for name, base := range tenantSources(t) {
		t.Run(name, func(t *testing.T) {
			src := metrics.TenantSource(base)
			def := tenant.Inject(context.Background(), tenant.Default)
			names, err := src.SelectLabelNames(def)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(names, "secret") || slices.Contains(names, tenant.Label) || !slices.Contains(names, "job") {
				t.Fatalf("default tenant label names = %v", names)
			}
			vals, _ := src.SelectLabelValues(def, "__name__")
			if !slices.Equal(vals, []string{"up"}) {
				t.Fatalf("default tenant __name__ values = %v", vals)
			}
			b := tenant.Inject(context.Background(), "team-b")
			vals, _ = src.SelectLabelValues(b, "__name__")
			if !slices.Equal(vals, []string{"only_b", "up"}) {
				t.Fatalf("team-b __name__ values = %v", vals)
			}
			if v, _ := src.SelectLabelValues(b, tenant.Label); len(v) != 0 {
				t.Fatalf("label values of __tenant__ = %v, want none", v)
			}
		})
	}
}

func TestTenantSourceSelectorNamingTheTenantLabelMatchesNothing(t *testing.T) {
	for name, base := range tenantSources(t) {
		t.Run(name, func(t *testing.T) {
			src := metrics.TenantSource(base)
			p := metrics.SelectParams{Selector: metrics.Selector{Matchers: []metrics.Matcher{{Name: tenant.Label, Value: "team-b"}}}, MinT: 0, MaxT: 2000}
			// team-b too: its own label is internal, so naming it matches nothing
			// even when the value is its own tenant.
			for _, id := range []string{tenant.Default, "other", "team-b"} {
				got, err := src.Select(tenant.Inject(context.Background(), id), p)
				if err != nil || len(got) != 0 {
					t.Fatalf("%s reading {__tenant__=\"team-b\"}: %v, %v", id, got, err)
				}
			}
		})
	}
}

// The label-name and value reads rest on an empty selector with AnyTime
// selecting every series of every source.
func TestEmptySelectorAnyTimeSelectsEverySeries(t *testing.T) {
	for name, base := range tenantSources(t) {
		t.Run(name, func(t *testing.T) {
			got, err := base.Select(context.Background(), metrics.SelectParams{SeriesOnly: true, AnyTime: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 {
				t.Fatalf("got %d series, want 3: %+v", len(got), got)
			}
		})
	}
}
