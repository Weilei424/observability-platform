package metrics_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// rewriteBlockTenant sets meta.json's tenant field of the store's only block,
// as a corruption would.
func rewriteBlockTenant(t *testing.T, dir, to string) {
	t.Helper()
	metas, err := filepath.Glob(filepath.Join(dir, "metrics", "blocks", "*", "meta.json"))
	if err != nil || len(metas) != 1 {
		t.Fatalf("meta.json files = %v (%v), want one", metas, err)
	}
	raw, err := os.ReadFile(metas[0])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if to == "" {
		delete(m, "tenant")
	} else {
		m["tenant"] = to
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metas[0], out, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A block's tenant decides its retention, so a meta.json naming the wrong
// tenant must stop the store from opening rather than let retention expire the
// block on another tenant's window.
func TestBlockStore_RefusesABlockWhoseTenantDisagreesWithItsSeries(t *testing.T) {
	for _, tc := range []struct {
		name, owner, metaTenant string
	}{
		{"team-b block relabelled as the default tenant", "team-b", ""},
		{"default-tenant block relabelled as team-b", tenant.Default, "team-b"},
		{"team-b block relabelled as another tenant", "team-b", "team-c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := metrics.NewBlockStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bs.IngestSeriesChunks(context.Background(), []metrics.SeriesChunks{tenantSeriesChunks(t, tc.owner, "up")}); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			rewriteBlockTenant(t, dir, tc.metaTenant)
			reopened, err := metrics.NewBlockStore(dir)
			if err == nil {
				_ = reopened.Close()
				t.Fatal("store opened a block whose meta.json tenant disagrees with its series")
			}
			if !strings.Contains(err.Error(), "tenant") {
				t.Fatalf("err = %v, want it to name the tenant mismatch", err)
			}
		})
	}
}

// The untouched blocks of both tenants still open.
func TestBlockStore_OpensBlocksWhoseTenantMatchesTheirSeries(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	batch := []metrics.SeriesChunks{tenantSeriesChunks(t, "team-b", "up"), tenantSeriesChunks(t, tenant.Default, "up")}
	if _, err := bs.IngestSeriesChunks(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_ = reopened.Close()
}
