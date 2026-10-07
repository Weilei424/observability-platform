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

func TestRingMetricsQuorumAndReads(t *testing.T) {
	members := []string{"ingester-0:8080", "ingester-1:8080"}

	gw := prometheus.NewRegistry()
	g := NewRingMetrics()
	g.Register(gw, members, true)
	if n := testutil.CollectAndCount(g.WriteQuorum, "obs_gateway_write_quorum_total"); n != 3 {
		t.Errorf("gateway quorum series = %d, want 3", n)
	}
	if n := testutil.CollectAndCount(gw, "obs_querier_ingester_reads_total"); n != 0 {
		t.Errorf("gateway registers %d read series, want 0", n)
	}

	qr := prometheus.NewRegistry()
	q := NewRingMetrics()
	q.Register(qr, members, false)
	if n := testutil.CollectAndCount(qr, "obs_querier_ingester_reads_total"); n != 6 {
		t.Errorf("querier read series = %d, want 2 members × 3 outcomes", n)
	}
	if n := testutil.CollectAndCount(qr, "obs_gateway_write_quorum_total"); n != 0 {
		t.Errorf("querier registers %d quorum series, want 0", n)
	}
	if n := testutil.CollectAndCount(gw, "obs_gateway_write_quorum_total"); n != 3 {
		t.Errorf("gateway registry quorum series = %d, want 3", n)
	}
}
