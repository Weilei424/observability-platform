package metrics_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

func tenantSeriesChunks(t *testing.T, id, name string) metrics.SeriesChunks {
	t.Helper()
	l := labels.WithTenant(makeLabels(t, map[string]string{"__name__": name}), id)
	return metrics.SeriesChunks{ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunk(t)}}
}

func TestBlockStore_IngestSeriesChunks_OneBlockPerTenant(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	batch := []metrics.SeriesChunks{
		tenantSeriesChunks(t, "team-b", "up"),
		tenantSeriesChunks(t, tenant.Default, "up"),
		tenantSeriesChunks(t, "team-b", "other"),
	}
	meta, err := bs.IngestSeriesChunks(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	if meta.NumSeries != 3 {
		t.Fatalf("returned NumSeries = %d, want summed 3", meta.NumSeries)
	}
	check := func(bs *metrics.BlockStore) {
		t.Helper()
		infos := bs.BlockInfos()
		if len(infos) != 2 {
			t.Fatalf("blocks = %d, want 2", len(infos))
		}
		got := map[string]int{}
		for _, in := range infos {
			r, err := block.OpenReader(filepath.Join(dir, "metrics", "blocks", in.ID))
			if err != nil {
				t.Fatal(err)
			}
			m := r.Meta()
			if m.Tenant != in.Tenant {
				t.Fatalf("meta tenant %q != info tenant %q", m.Tenant, in.Tenant)
			}
			for _, se := range r.Series() {
				owner := ""
				for _, lp := range se.Labels {
					if lp.Name == tenant.Label {
						owner = lp.Value
					}
				}
				if owner != m.Tenant {
					t.Fatalf("block of tenant %q holds a series of tenant %q", m.Tenant, owner)
				}
			}
			got[m.Tenant] = len(r.Series())
			_ = r.Close()
		}
		if got[""] != 1 || got["team-b"] != 2 {
			t.Fatalf("series per tenant = %v, want {\"\":1 team-b:2}", got)
		}
		src := metrics.TenantSource(bs)
		for id, want := range map[string]int{tenant.Default: 1, "team-b": 2} {
			res, err := src.Select(tenant.Inject(context.Background(), id), metrics.SelectParams{MinT: 0, MaxT: 1 << 40})
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, sd := range res {
				v, _ := sd.Labels.Get("__name__")
				names = append(names, v)
			}
			sort.Strings(names)
			if len(res) != want {
				t.Fatalf("tenant %q reads %v, want %d series", id, names, want)
			}
		}
	}
	check(bs)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bs2.Close()
	check(bs2)
}
