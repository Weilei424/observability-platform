package observability

import "github.com/prometheus/client_golang/prometheus"

// RetryOutcomes are the outcome label values of obs_ingester_store_retries_total.
var RetryOutcomes = []string{"ok", "failed", "dropped"}

// StoreMetrics describe the store ring: the ingester's flush quorum and
// straggler queue, the querier's store reads, and the compactor's passes.
type StoreMetrics struct {
	FlushQuorum   *prometheus.CounterVec // obs_ingester_store_flush_total{outcome}
	Retries       *prometheus.CounterVec // obs_ingester_store_retries_total{outcome}
	RetryPending  prometheus.Gauge       // obs_ingester_store_retry_pending_bytes
	StoreReads    *prometheus.CounterVec // obs_querier_store_reads_total{store,outcome}
	CompactorPass *prometheus.CounterVec // obs_compactor_store_passes_total{store,outcome}
	Members       prometheus.Gauge       // obs_store_ring_members
}

func NewStoreMetrics() *StoreMetrics {
	return &StoreMetrics{
		FlushQuorum: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_ingester_store_flush_total",
			Help: "Flush batches by store quorum outcome: full, degraded (quorum met, a store failed and its copy was queued), failed (quorum not met; the head keeps the batch).",
		}, []string{"outcome"}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_ingester_store_retries_total",
			Help: "Straggler copies by outcome: ok (delivered), failed (attempt failed, retried later), dropped (over the queue's bounds).",
		}, []string{"outcome"}),
		RetryPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obs_ingester_store_retry_pending_bytes",
			Help: "Bytes of flush bodies waiting in the straggler queue.",
		}),
		StoreReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_querier_store_reads_total",
			Help: "Reads the querier sent to each store, by outcome.",
		}, []string{"store", "outcome"}),
		CompactorPass: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_compactor_store_passes_total",
			Help: "Compaction and retention calls to each store (not whole passes), by outcome.",
		}, []string{"store", "outcome"}),
		Members: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "obs_store_ring_members",
			Help: "Number of stores in the ring this component flushes to, reads, or maintains.",
		}),
	}
}

// RegisterIngester registers the members gauge and the flush, retry, and
// pending-bytes metrics, with every outcome series pre-initialized.
func (m *StoreMetrics) RegisterIngester(reg *prometheus.Registry, stores []string) {
	m.Members.Set(float64(len(stores)))
	reg.MustRegister(m.Members)
	for _, o := range QuorumOutcomes {
		m.FlushQuorum.WithLabelValues(o)
	}
	reg.MustRegister(m.FlushQuorum)
	for _, o := range RetryOutcomes {
		m.Retries.WithLabelValues(o)
	}
	reg.MustRegister(m.Retries)
	reg.MustRegister(m.RetryPending)
}

// RegisterQuerier registers the members gauge and the per-store read counter,
// pre-initialized for every store and outcome.
func (m *StoreMetrics) RegisterQuerier(reg *prometheus.Registry, stores []string) {
	m.Members.Set(float64(len(stores)))
	reg.MustRegister(m.Members)
	preinit(m.StoreReads, stores)
	reg.MustRegister(m.StoreReads)
}

// RegisterCompactor registers the members gauge and the per-store pass
// counter, pre-initialized for every store and outcome.
func (m *StoreMetrics) RegisterCompactor(reg *prometheus.Registry, stores []string) {
	m.Members.Set(float64(len(stores)))
	reg.MustRegister(m.Members)
	preinit(m.CompactorPass, stores)
	reg.MustRegister(m.CompactorPass)
}

func preinit(v *prometheus.CounterVec, stores []string) {
	for _, s := range stores {
		for _, o := range RingOutcomes {
			v.WithLabelValues(s, o)
		}
	}
}
