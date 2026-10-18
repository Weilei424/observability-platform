package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

func retentionStore(t *testing.T) *metrics.BlockStore {
	t.Helper()
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	batch := []metrics.SeriesChunks{
		tenantSeriesChunks(t, tenant.Default, "up"),
		tenantSeriesChunks(t, "team-b", "up"),
		tenantSeriesChunks(t, "keep", "up"),
	}
	if _, err := bs.IngestSeriesChunks(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := len(bs.BlockInfos()); got != 3 {
		t.Fatalf("blocks = %d, want 3", got)
	}
	return bs
}

func TestBlockStore_ApplyRetention_PerTenant(t *testing.T) {
	bs := retentionStore(t)
	now := time.UnixMilli(1119).Add(48 * time.Hour)
	policy := tenant.RetentionPolicy{Default: 24 * time.Hour, Overrides: map[string]time.Duration{"team-b": 72 * time.Hour, "keep": 0}}
	n, err := bs.ApplyRetention(now, policy)
	if err != nil || n != 1 {
		t.Fatalf("ApplyRetention = %d, %v; want 1, nil", n, err)
	}
	for _, in := range bs.BlockInfos() {
		if in.Tenant != "team-b" && in.Tenant != "keep" {
			t.Errorf("surviving block of tenant %q", in.Tenant)
		}
	}
	if got := len(bs.BlockInfos()); got != 2 {
		t.Fatalf("blocks = %d, want 2", got)
	}
}

func TestBlockStore_ApplyRetention_ZeroDefaultKeepsAll(t *testing.T) {
	bs := retentionStore(t)
	n, err := bs.ApplyRetention(time.UnixMilli(1119).Add(48*time.Hour), tenant.RetentionPolicy{Default: 0})
	if err != nil || n != 0 {
		t.Fatalf("ApplyRetention = %d, %v; want 0, nil", n, err)
	}
}

func TestBlockStore_ApplyRetention_OverrideExpiresWhenDefaultKeepsForever(t *testing.T) {
	bs := retentionStore(t)
	policy := tenant.RetentionPolicy{Default: 0, Overrides: map[string]time.Duration{"team-b": time.Hour}}
	n, err := bs.ApplyRetention(time.UnixMilli(1119).Add(48*time.Hour), policy)
	if err != nil || n != 1 {
		t.Fatalf("ApplyRetention = %d, %v; want 1, nil", n, err)
	}
}
