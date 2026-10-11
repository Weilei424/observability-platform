package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var testStores = []string{"s1:8080", "s2:8080"}

func TestStoreMetricsIngester(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewStoreMetrics()
	m.RegisterIngester(reg, testStores)
	if v := testutil.ToFloat64(m.Members); v != 2 {
		t.Errorf("obs_store_ring_members = %v, want 2", v)
	}
	if n := testutil.CollectAndCount(reg, "obs_ingester_store_flush_total"); n != len(QuorumOutcomes) {
		t.Errorf("flush series = %d, want %d", n, len(QuorumOutcomes))
	}
	if n := testutil.CollectAndCount(reg, "obs_ingester_store_retries_total"); n != len(RetryOutcomes) {
		t.Errorf("retry series = %d, want %d", n, len(RetryOutcomes))
	}
	if n := testutil.CollectAndCount(reg, "obs_ingester_store_retry_pending_bytes"); n != 1 {
		t.Errorf("pending gauge series = %d, want 1", n)
	}
	for _, name := range []string{"obs_querier_store_reads_total", "obs_compactor_store_passes_total"} {
		if n := testutil.CollectAndCount(reg, name); n != 0 {
			t.Errorf("ingester registers %d %s series, want 0", n, name)
		}
	}
	if v := testutil.ToFloat64(m.FlushQuorum.WithLabelValues("degraded")); v != 0 {
		t.Errorf("degraded = %v, want 0", v)
	}
}

func TestStoreMetricsQuerier(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewStoreMetrics()
	m.RegisterQuerier(reg, testStores)
	if n := testutil.CollectAndCount(reg, "obs_querier_store_reads_total"); n != 2*len(RingOutcomes) {
		t.Errorf("read series = %d, want %d", n, 2*len(RingOutcomes))
	}
	if v := testutil.ToFloat64(m.StoreReads.WithLabelValues("s1:8080", "unavailable")); v != 0 {
		t.Errorf("unavailable = %v, want 0", v)
	}
	if n := testutil.CollectAndCount(reg, "obs_store_ring_members"); n != 1 {
		t.Errorf("members series = %d, want 1", n)
	}
	if n := testutil.CollectAndCount(reg, "obs_ingester_store_flush_total"); n != 0 {
		t.Errorf("querier registers %d flush series, want 0", n)
	}
}

func TestStoreMetricsCompactor(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewStoreMetrics()
	m.RegisterCompactor(reg, testStores)
	if n := testutil.CollectAndCount(reg, "obs_compactor_store_passes_total"); n != 2*len(RingOutcomes) {
		t.Errorf("pass series = %d, want %d", n, 2*len(RingOutcomes))
	}
	if v := testutil.ToFloat64(m.Members); v != 2 {
		t.Errorf("members = %v, want 2", v)
	}
	if n := testutil.CollectAndCount(reg, "obs_querier_store_reads_total"); n != 0 {
		t.Errorf("compactor registers %d read series, want 0", n)
	}
}
