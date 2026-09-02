package observability

import "github.com/prometheus/client_golang/prometheus"

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
}

func (c *walCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bytes
	ch <- c.segments
}

func (c *walCollector) Collect(ch chan<- prometheus.Metric) {
	for _, src := range c.sources {
		bytes, segments, err := src.Stats()
		if err != nil {
			c.errors.WithLabelValues("wal").Inc()
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
}

func (c *logsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.streams
	ch <- c.chunks
	ch <- c.bytes
}

func (c *logsCollector) Collect(ch chan<- prometheus.Metric) {
	streams, chunks, bytes, err := c.src.Stats()
	if err != nil {
		c.errors.WithLabelValues("logs").Inc()
		return
	}
	ch <- prometheus.MustNewConstMetric(c.streams, prometheus.GaugeValue, float64(streams))
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(chunks))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(bytes))
}
