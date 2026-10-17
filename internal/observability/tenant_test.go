package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func gatherFamily(t *testing.T, reg *prometheus.Registry, name string) []*dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()
		}
	}
	return nil
}

func TestTenantMetricsGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewTenantMetrics()
	series := map[string]int{"anonymous": 3, "team-b": 1, "gone": 0}
	streams := map[string]int{"anonymous": 2}
	m.Register(reg, func() map[string]int { return series }, func() map[string]int { return streams })

	got := gatherFamily(t, reg, "obs_tenant_active_series")
	if len(got) != 2 {
		t.Fatalf("obs_tenant_active_series samples = %d, want 2", len(got))
	}
	want := map[string]float64{"anonymous": 3, "team-b": 1}
	for _, s := range got {
		tenant := s.GetLabel()[0].GetValue()
		if s.GetLabel()[0].GetName() != "tenant" {
			t.Errorf("label name = %q, want tenant", s.GetLabel()[0].GetName())
		}
		if v, ok := want[tenant]; !ok || s.GetGauge().GetValue() != v {
			t.Errorf("series tenant=%q value=%v, want one of %v", tenant, s.GetGauge().GetValue(), want)
		}
	}

	gotStreams := gatherFamily(t, reg, "obs_tenant_active_streams")
	if len(gotStreams) != 1 {
		t.Fatalf("obs_tenant_active_streams samples = %d, want 1", len(gotStreams))
	}
	if tenant := gotStreams[0].GetLabel()[0].GetValue(); tenant != "anonymous" || gotStreams[0].GetGauge().GetValue() != 2 {
		t.Errorf("streams sample tenant=%q value=%v, want anonymous=2", tenant, gotStreams[0].GetGauge().GetValue())
	}
}

func TestTenantMetricsLimitRejections(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewTenantMetrics()
	m.Register(reg, func() map[string]int { return nil }, func() map[string]int { return nil })
	m.LimitRejections.WithLabelValues("team-b", "series").Inc()
	if v := testutil.ToFloat64(m.LimitRejections.WithLabelValues("team-b", "series")); v != 1 {
		t.Errorf("rejections = %v, want 1", v)
	}
	if n := testutil.CollectAndCount(reg, "obs_tenant_limit_rejections_total"); n != 1 {
		t.Errorf("rejection series = %d, want 1", n)
	}
}

func TestTenantMetricsNilStreamsNotRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewTenantMetrics()
	m.Register(reg, func() map[string]int { return map[string]int{"a": 1} }, nil)
	if n := testutil.CollectAndCount(reg, "obs_tenant_active_streams"); n != 0 {
		t.Errorf("streams series = %d, want 0 when streams source is nil", n)
	}
	if n := testutil.CollectAndCount(reg, "obs_tenant_active_series"); n != 1 {
		t.Errorf("series series = %d, want 1", n)
	}
}
