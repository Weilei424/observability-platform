package observability

import "github.com/prometheus/client_golang/prometheus"

// RingOutcomes are the outcome label values of obs_gateway_ingester_requests_total.
var RingOutcomes = []string{"ok", "unavailable", "error"}

// RingMetrics describe the ring a gateway routes over and a querier reads.
type RingMetrics struct {
	Members          prometheus.Gauge
	IngesterRequests *prometheus.CounterVec
}

func NewRingMetrics() *RingMetrics {
	return &RingMetrics{
		Members: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obs_ring_members", Help: "Number of ingesters in the ring this component routes over or reads.",
		}),
		IngesterRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_gateway_ingester_requests_total",
			Help: "Write groups the gateway sent to each ingester, by outcome.",
		}, []string{"ingester", "outcome"}),
	}
}

// Register registers the gauge, set to len(members), and the request counter,
// pre-initialized for every member and outcome so a first failure reads as
// 0 → 1 rather than absent → 1. A querier, which sends no writes, registers
// only the gauge (pass withRequests false).
func (m *RingMetrics) Register(reg *prometheus.Registry, members []string, withRequests bool) {
	m.Members.Set(float64(len(members)))
	reg.MustRegister(m.Members)
	if !withRequests {
		return
	}
	for _, member := range members {
		for _, o := range RingOutcomes {
			m.IngesterRequests.WithLabelValues(member, o)
		}
	}
	reg.MustRegister(m.IngesterRequests)
}
