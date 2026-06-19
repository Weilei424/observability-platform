package observability

import "github.com/prometheus/client_golang/prometheus"

// CardinalitySource provides current label-cardinality counts at scrape time.
type CardinalitySource interface {
	Cardinality() (series, names, pairs int)
}

type cardinalityCollector struct {
	src          CardinalitySource
	activeSeries *prometheus.Desc
	labelNames   *prometheus.Desc
	labelPairs   *prometheus.Desc
}

// NewRegistry returns a Prometheus registry with a collector that reports label
// cardinality (active series, label names, label pairs) pulled from src at
// scrape time.
func NewRegistry(src CardinalitySource) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(&cardinalityCollector{
		src:          src,
		activeSeries: prometheus.NewDesc("obs_active_series", "Number of active metric series.", nil, nil),
		labelNames:   prometheus.NewDesc("obs_label_names_total", "Number of distinct label names.", nil, nil),
		labelPairs:   prometheus.NewDesc("obs_label_pairs_total", "Number of distinct label name=value pairs.", nil, nil),
	})
	return reg
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
