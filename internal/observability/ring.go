package observability

import "github.com/prometheus/client_golang/prometheus"

// RingOutcomes are the outcome label values of obs_gateway_ingester_requests_total.
var RingOutcomes = []string{"ok", "unavailable", "error"}

// QuorumOutcomes are the outcome label values of obs_gateway_write_quorum_total.
var QuorumOutcomes = []string{"full", "degraded", "failed"}

// RingMetrics describe the ring a gateway routes over and a querier reads.
type RingMetrics struct {
	Members          prometheus.Gauge
	IngesterRequests *prometheus.CounterVec
	WriteQuorum      *prometheus.CounterVec
	IngesterReads    *prometheus.CounterVec
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
		WriteQuorum: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_gateway_write_quorum_total",
			Help: "Routed write batches by quorum outcome: full (every replica acknowledged), degraded (quorum met, a replica failed), failed (quorum not met).",
		}, []string{"outcome"}),
		IngesterReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_querier_ingester_reads_total",
			Help: "Reads the querier sent to each ingester, by outcome.",
		}, []string{"ingester", "outcome"}),
	}
}

// Register registers the gauge, set to len(members), and the counters,
// pre-initialized for every member and outcome so a first failure reads as
// 0 → 1 rather than absent → 1. A gateway (withRequests true) registers the
// request and write-quorum counters; a querier, which sends no writes,
// registers the ingester-read counter instead.
func (m *RingMetrics) Register(reg *prometheus.Registry, members []string, withRequests bool) {
	m.Members.Set(float64(len(members)))
	reg.MustRegister(m.Members)
	if !withRequests {
		for _, member := range members {
			for _, o := range RingOutcomes {
				m.IngesterReads.WithLabelValues(member, o)
			}
		}
		reg.MustRegister(m.IngesterReads)
		return
	}
	for _, member := range members {
		for _, o := range RingOutcomes {
			m.IngesterRequests.WithLabelValues(member, o)
		}
	}
	reg.MustRegister(m.IngesterRequests)
	for _, o := range QuorumOutcomes {
		m.WriteQuorum.WithLabelValues(o)
	}
	reg.MustRegister(m.WriteQuorum)
}
