package observability

import "github.com/prometheus/client_golang/prometheus"

// CardinalitySource provides current label-cardinality counts at scrape time.
type CardinalitySource interface {
	Cardinality() (series, names, pairs int)
}

// StorageStatsSource provides current block storage stats at scrape time.
type StorageStatsSource interface {
	StorageStats() (blocks int, bytes int64)
}

// Metrics holds push-model instruments updated by the compactor.
type Metrics struct {
	CompactionsTotal        prometheus.Counter
	CompactionFailuresTotal prometheus.Counter
	CompactionDuration      prometheus.Histogram
	RetentionDeletedTotal   prometheus.Counter
	FlushesTotal            prometheus.Counter
	FlushFailuresTotal      prometheus.Counter
}

type cardinalityCollector struct {
	src          CardinalitySource
	activeSeries *prometheus.Desc
	labelNames   *prometheus.Desc
	labelPairs   *prometheus.Desc
}

type storageCollector struct {
	src    StorageStatsSource
	blocks *prometheus.Desc
	bytes  *prometheus.Desc
}

// NewRegistry returns a Prometheus registry plus a push-metrics handle. The
// cardinality and storage collectors are pull-model (read from their sources at
// scrape time). storage may be nil (e.g. in tests without block storage), in
// which case the storage gauges are omitted.
func NewRegistry(card CardinalitySource, storage StorageStatsSource) (*prometheus.Registry, *Metrics) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(&cardinalityCollector{
		src:          card,
		activeSeries: prometheus.NewDesc("obs_active_series", "Number of active metric series.", nil, nil),
		labelNames:   prometheus.NewDesc("obs_label_names_total", "Number of distinct label names.", nil, nil),
		labelPairs:   prometheus.NewDesc("obs_label_pairs_total", "Number of distinct label name=value pairs.", nil, nil),
	})
	if storage != nil {
		reg.MustRegister(&storageCollector{
			src:    storage,
			blocks: prometheus.NewDesc("obs_blocks_total", "Number of persisted metric blocks.", nil, nil),
			bytes:  prometheus.NewDesc("obs_blocks_bytes", "Total on-disk size of persisted metric blocks in bytes.", nil, nil),
		})
	}

	m := &Metrics{
		CompactionsTotal:        prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_compactions_total", Help: "Total number of block groups compacted."}),
		CompactionFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_compaction_failures_total", Help: "Total number of failed compaction passes."}),
		CompactionDuration:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "obs_compaction_duration_seconds", Help: "Duration of compaction passes in seconds.", Buckets: prometheus.DefBuckets}),
		RetentionDeletedTotal:   prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_retention_deleted_blocks_total", Help: "Total number of blocks deleted by retention."}),
		FlushesTotal:            prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_flushes_total", Help: "Total number of successful head flushes."}),
		FlushFailuresTotal:      prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_flush_failures_total", Help: "Total number of failed head flushes."}),
	}
	reg.MustRegister(
		m.CompactionsTotal, m.CompactionFailuresTotal, m.CompactionDuration,
		m.RetentionDeletedTotal, m.FlushesTotal, m.FlushFailuresTotal,
	)
	return reg, m
}

func (c *cardinalityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeSeries
	ch <- c.labelNames
	ch <- c.labelPairs
}

func (c *cardinalityCollector) Collect(ch chan<- prometheus.Metric) {
	series, names, pairs := c.src.Cardinality()
	ch <- prometheus.MustNewConstMetric(c.activeSeries, prometheus.GaugeValue, float64(series))
	ch <- prometheus.MustNewConstMetric(c.labelNames, prometheus.GaugeValue, float64(names))
	ch <- prometheus.MustNewConstMetric(c.labelPairs, prometheus.GaugeValue, float64(pairs))
}

func (c *storageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.blocks
	ch <- c.bytes
}

func (c *storageCollector) Collect(ch chan<- prometheus.Metric) {
	blocks, bytes := c.src.StorageStats()
	ch <- prometheus.MustNewConstMetric(c.blocks, prometheus.GaugeValue, float64(blocks))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(bytes))
}
