package observability

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// CardinalitySource provides current label-cardinality counts at scrape time.
type CardinalitySource interface {
	Cardinality() (series, names, pairs int)
}

// StorageStatsSource provides current block storage stats at scrape time.
type StorageStatsSource interface {
	StorageStats() (blocks int, bytes int64)
}

// WALSource is one named write-ahead log to report size and segment count for.
// Stats is a function value rather than an interface so this package does not
// import internal/storage/wal — instrumentation depends on storage, never the
// other way round.
type WALSource struct {
	Name  string
	Stats func() (bytes int64, segments int, err error)
}

// LogStatsSource provides log stream, chunk, and byte counts at scrape time.
type LogStatsSource interface {
	Stats() (streams, chunks int, bytes int64, err error)
}

// InstrumentGroups names push-model instrument groups a component may not own.
type InstrumentGroups uint8

const (
	// FlushGroup is obs_flushes_total and obs_flush_failures_total: whoever
	// flushes the metrics head.
	FlushGroup InstrumentGroups = 1 << iota
	// CompactionGroup is obs_compactions_total, obs_compaction_failures_total,
	// obs_compaction_duration_seconds, and obs_retention_deleted_blocks_total.
	CompactionGroup
	// IngestGroup is the accepted and rejected sample and log-line counters.
	IngestGroup
	// LogFlushGroup is obs_log_flushes_total and obs_log_flush_failures_total.
	LogFlushGroup

	// AllGroups is every push-model group: what the store, querier, and gateway omit.
	AllGroups = FlushGroup | CompactionGroup | IngestGroup | LogFlushGroup
)

// RegistryOptions collects the telemetry sources a registry reads from. Every
// field is optional: a component registers only the sources it owns, and a
// metric whose source is absent is not registered at all rather than
// reporting zero.
type RegistryOptions struct {
	Cardinality CardinalitySource
	Storage     StorageStatsSource
	WALs        []WALSource
	Logs        LogStatsSource

	// Omit lists the push-model groups this component does not own. Their
	// handles in Instruments still work but are never registered, so a
	// component exports no counter for work it cannot do. The zero value omits
	// nothing: all-in-one's shape.
	Omit InstrumentGroups

	// Logger receives the collectors' read-failure and recovery lines. It
	// must be component-free, for the same reason as api.Deps.Logger: each
	// collector stamps its own component ("wal" or "logs", matching its
	// obs_collector_errors_total label), and a pre-stamped logger would put
	// two component keys on every one of those lines. Nil falls back to
	// slog.Default(), so a failure is never silently discarded.
	Logger *slog.Logger
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

// Instruments are the push-model handles the server hands to the components that
// update them. Pull-model collectors are not here: they read from their sources
// at scrape time and nobody holds a handle to them.
type Instruments struct {
	Maintenance *Metrics
	HTTP        *HTTPMetrics
	Ingest      *IngestMetrics
	LogFlush    *LogFlushMetrics
}

// LogFlushMetrics count log-store flushes. In the ingester a failed flush no
// longer fails the push that triggered it, so without these a store outage
// would show up only in the logs.
type LogFlushMetrics struct {
	Flushes  prometheus.Counter
	Failures prometheus.Counter
}

// NewLogFlushMetrics builds the instruments without registering them.
func NewLogFlushMetrics() *LogFlushMetrics {
	return &LogFlushMetrics{
		Flushes:  prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_log_flushes_total", Help: "Total number of successful log-store flushes."}),
		Failures: prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_log_flush_failures_total", Help: "Total number of failed log-store flushes."}),
	}
}

// Observe counts one flush attempt. It is the logs head's flush hook.
func (m *LogFlushMetrics) Observe(err error) {
	if err != nil {
		m.Failures.Inc()
		return
	}
	m.Flushes.Inc()
}

// NewRegistry returns a Prometheus registry plus the push-model instrument
// handles. Cardinality, storage, WAL, and log metrics are pull-model: they read
// from their sources when Prometheus scrapes.
func NewRegistry(opts RegistryOptions) (*prometheus.Registry, *Instruments) {
	reg := prometheus.NewRegistry()

	collectorErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "obs_collector_errors_total",
		Help: "Total scrape-time collector failures by collector.",
	}, []string{"collector"})
	// Preinitialize both known collectors so a single failure reads as
	// absent(0) -> 1 rather than absent -> 1: the latter is invisible to
	// rate(), which needs two points in its window. See CollectorNames.
	for _, collector := range CollectorNames {
		collectorErrors.WithLabelValues(collector)
	}
	reg.MustRegister(collectorErrors)

	if opts.Cardinality != nil {
		reg.MustRegister(&cardinalityCollector{
			src:          opts.Cardinality,
			activeSeries: prometheus.NewDesc("obs_active_series", "Number of active metric series.", nil, nil),
			labelNames:   prometheus.NewDesc("obs_label_names_total", "Number of distinct label names.", nil, nil),
			labelPairs:   prometheus.NewDesc("obs_label_pairs_total", "Number of distinct label name=value pairs.", nil, nil),
		})
	}

	if opts.Storage != nil {
		reg.MustRegister(&storageCollector{
			src:    opts.Storage,
			blocks: prometheus.NewDesc("obs_blocks_total", "Number of persisted metric blocks.", nil, nil),
			bytes:  prometheus.NewDesc("obs_blocks_bytes", "Total on-disk size of persisted metric blocks in bytes.", nil, nil),
		})
	}

	if len(opts.WALs) > 0 {
		reg.MustRegister(newWALCollector(opts.WALs, collectorErrors, opts.Logger))
	}

	if opts.Logs != nil {
		reg.MustRegister(newLogsCollector(opts.Logs, collectorErrors, opts.Logger))
	}

	m := &Metrics{
		CompactionsTotal:        prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_compactions_total", Help: "Total number of block groups compacted."}),
		CompactionFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_compaction_failures_total", Help: "Total number of failed compaction passes."}),
		CompactionDuration:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "obs_compaction_duration_seconds", Help: "Duration of compaction passes in seconds.", Buckets: prometheus.DefBuckets}),
		RetentionDeletedTotal:   prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_retention_deleted_blocks_total", Help: "Total number of blocks deleted by retention."}),
		FlushesTotal:            prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_flushes_total", Help: "Total number of successful head flushes."}),
		FlushFailuresTotal:      prometheus.NewCounter(prometheus.CounterOpts{Name: "obs_flush_failures_total", Help: "Total number of failed head flushes."}),
	}
	if opts.Omit&CompactionGroup == 0 {
		reg.MustRegister(m.CompactionsTotal, m.CompactionFailuresTotal, m.CompactionDuration, m.RetentionDeletedTotal)
	}
	if opts.Omit&FlushGroup == 0 {
		reg.MustRegister(m.FlushesTotal, m.FlushFailuresTotal)
	}

	httpMetrics := NewHTTPMetrics()
	reg.MustRegister(httpMetrics.collectors()...)

	ingestMetrics := NewIngestMetrics()
	if opts.Omit&IngestGroup == 0 {
		reg.MustRegister(ingestMetrics.collectors()...)
	}

	logFlush := NewLogFlushMetrics()
	if opts.Omit&LogFlushGroup == 0 {
		reg.MustRegister(logFlush.Flushes, logFlush.Failures)
	}

	return reg, &Instruments{Maintenance: m, HTTP: httpMetrics, Ingest: ingestMetrics, LogFlush: logFlush}
}
