package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRingMetricsRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRingMetrics()
	m.Register(reg, []string{"ingester-0:8080", "ingester-1:8080"}, true)
	if v := testutil.ToFloat64(m.Members); v != 2 {
		t.Errorf("obs_ring_members = %v, want 2", v)
	}
	if n := testutil.CollectAndCount(m.IngesterRequests); n != 6 {
		t.Errorf("pre-initialized series = %d, want 2 members × 3 outcomes", n)
	}
}
