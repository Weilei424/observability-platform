package observability

import (
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector label values for obs_collector_errors_total. CollectorNames is
// the closed set NewRegistry preinitializes to zero; walCollector and
// logsCollector below count against these same constants so a third
// collector added later cannot be counted here without also being
// preinitialized there.
const (
	CollectorWAL  = "wal"
	CollectorLogs = "logs"
)

// CollectorNames is the closed set of "collector" label values.
var CollectorNames = []string{CollectorWAL, CollectorLogs}

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

// walCollector reports size and segment count for each named WAL at scrape time.
//
// On a read failure it emits NOTHING for that WAL and counts the error instead.
// Emitting zero would draw a WAL that had shrunk away, which is indistinguishable
// on a dashboard from real data loss; a gap is not. prometheus.NewInvalidMetric
// is also wrong here — promhttp is configured with HTTPErrorOnError, so a Gather
// error 500s the entire scrape and blanks every other panel.
type walCollector struct {
	sources  []WALSource
	errors   *prometheus.CounterVec
	bytes    *prometheus.Desc
	segments *prometheus.Desc
	log      *slog.Logger
	// failing[i] tracks whether sources[i] failed on its last read, so a
	// failure is logged once when it starts rather than on every scrape.
	failing []atomic.Bool
}

func (c *walCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bytes
	ch <- c.segments
}

func (c *walCollector) Collect(ch chan<- prometheus.Metric) {
	for i, src := range c.sources {
		bytes, segments, err := src.Stats()
		reportReadState(c.log, &c.failing[i], src.Name, err)
		if err != nil {
			c.errors.WithLabelValues(CollectorWAL).Inc()
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(bytes), src.Name)
		ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(segments), src.Name)
	}
}

// logsCollector reports log stream, chunk, and byte counts at scrape time. It
// follows walCollector's error policy: a gap and a counted error, never a zero.
type logsCollector struct {
	src     LogStatsSource
	errors  *prometheus.CounterVec
	streams *prometheus.Desc
	chunks  *prometheus.Desc
	bytes   *prometheus.Desc
	log     *slog.Logger
	failing atomic.Bool
}

func (c *logsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.streams
	ch <- c.chunks
	ch <- c.bytes
}

func (c *logsCollector) Collect(ch chan<- prometheus.Metric) {
	streams, chunks, bytes, err := c.src.Stats()
	reportReadState(c.log, &c.failing, CollectorLogs, err)
	if err != nil {
		c.errors.WithLabelValues(CollectorLogs).Inc()
		return
	}
	ch <- prometheus.MustNewConstMetric(c.streams, prometheus.GaugeValue, float64(streams))
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(chunks))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(bytes))
}

// newWALCollector builds a walCollector with its invariants established: one
// failure-state slot per source, and a non-nil logger carrying the collector's
// component. Build through this, never a struct literal — Collect indexes
// failing by source and logs through log, so a literal missing either panics on
// the first failed read. log must be component-free (see RegistryOptions.Logger);
// nil falls back to slog.Default() so a failure is never silently discarded.
func newWALCollector(sources []WALSource, errors *prometheus.CounterVec, log *slog.Logger) *walCollector {
	if log == nil {
		log = slog.Default()
	}
	return &walCollector{
		sources:  sources,
		errors:   errors,
		log:      Component(log, CollectorWAL),
		failing:  make([]atomic.Bool, len(sources)),
		bytes:    prometheus.NewDesc("obs_wal_bytes", "Total size in bytes of the WAL segment files.", []string{"wal"}, nil),
		segments: prometheus.NewDesc("obs_wal_segments", "Number of WAL segment files.", []string{"wal"}, nil),
	}
}

// newLogsCollector is newWALCollector's counterpart. Its single failure flag is
// usable at its zero value, but log is not, so the same rule applies.
func newLogsCollector(src LogStatsSource, errors *prometheus.CounterVec, log *slog.Logger) *logsCollector {
	if log == nil {
		log = slog.Default()
	}
	return &logsCollector{
		src:     src,
		errors:  errors,
		log:     Component(log, CollectorLogs),
		streams: prometheus.NewDesc("obs_log_streams_total", "Number of distinct log streams.", nil, nil),
		chunks:  prometheus.NewDesc("obs_log_chunks_total", "Number of persisted log chunk files.", nil, nil),
		bytes:   prometheus.NewDesc("obs_log_chunk_bytes", "Total on-disk size of persisted log chunk files in bytes.", nil, nil),
	}
}

// reportReadState logs a collector source's read failures on state TRANSITIONS
// only: once when the source starts failing, carrying the error, and once when
// it recovers. obs_collector_errors_total already counts every failed scrape;
// logging each one would repeat the same line every scrape interval for as long
// as a directory stays unreadable, burying the one line that says why.
//
// Swap makes the transition atomic, so concurrent scrapes of the same source
// produce exactly one line per transition rather than one per scrape in flight.
//
// source names what failed. It is what distinguishes the metrics WAL from the
// logs WAL, which share collector="wal" on the counter.
func reportReadState(log *slog.Logger, failing *atomic.Bool, source string, err error) {
	if err != nil {
		if !failing.Swap(true) {
			log.Error("collector read failed; its gauges will show a gap until it recovers",
				"source", source, "error", err.Error())
		}
		return
	}
	if failing.Swap(false) {
		log.Info("collector read recovered", "source", source)
	}
}
