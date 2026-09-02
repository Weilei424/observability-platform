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

// RegistryOptions collects the telemetry sources a registry reads from. Every
// field except Cardinality is optional: tests and the healthcheck probe build a
// registry with no storage, no WAL, and no log store, and the corresponding
// metrics are then not registered at all rather than reporting zero.
type RegistryOptions struct {
	Cardinality CardinalitySource
	Storage     StorageStatsSource
	WALs        []WALSource
	Logs        LogStatsSource
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
	reg.MustRegister(collectorErrors)

	reg.MustRegister(&cardinalityCollector{
		src:          opts.Cardinality,
		activeSeries: prometheus.NewDesc("obs_active_series", "Number of active metric series.", nil, nil),
		labelNames:   prometheus.NewDesc("obs_label_names_total", "Number of distinct label names.", nil, nil),
		labelPairs:   prometheus.NewDesc("obs_label_pairs_total", "Number of distinct label name=value pairs.", nil, nil),
	})

	if opts.Storage != nil {
		reg.MustRegister(&storageCollector{
			src:    opts.Storage,
			blocks: prometheus.NewDesc("obs_blocks_total", "Number of persisted metric blocks.", nil, nil),
			bytes:  prometheus.NewDesc("obs_blocks_bytes", "Total on-disk size of persisted metric blocks in bytes.", nil, nil),
		})
	}

	if len(opts.WALs) > 0 {
		reg.MustRegister(&walCollector{
			sources:  opts.WALs,
			errors:   collectorErrors,
			bytes:    prometheus.NewDesc("obs_wal_bytes", "Total size in bytes of the WAL segment files.", []string{"wal"}, nil),
			segments: prometheus.NewDesc("obs_wal_segments", "Number of WAL segment files.", []string{"wal"}, nil),
		})
	}

	if opts.Logs != nil {
		reg.MustRegister(&logsCollector{
			src:     opts.Logs,
			errors:  collectorErrors,
			streams: prometheus.NewDesc("obs_log_streams_total", "Number of distinct log streams.", nil, nil),
			chunks:  prometheus.NewDesc("obs_log_chunks_total", "Number of persisted log chunk files.", nil, nil),
			bytes:   prometheus.NewDesc("obs_log_chunk_bytes", "Total on-disk size of persisted log chunk files in bytes.", nil, nil),
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

	return reg, &Instruments{Maintenance: m}
}
