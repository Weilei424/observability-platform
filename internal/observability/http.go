package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// UnmatchedRoute is the route label for a request that matched no route. A 404
// has no route pattern, and recording the raw URL path instead would let anyone
// grow the registry by requesting nonexistent paths.
const UnmatchedRoute = "<unmatched>"

// HTTPMetrics are the request-edge instruments: one counter and one histogram,
// both labelled by the chi ROUTE PATTERN rather than the resolved path.
type HTTPMetrics struct {
	Requests *prometheus.CounterVec
	Duration *prometheus.HistogramVec
}

// NewHTTPMetrics builds the instruments without registering them. NewRegistry
// registers the set it owns; tests use this directly.
func NewHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_http_requests_total",
			Help: "Total HTTP requests by route pattern, method, and response status.",
		}, []string{"route", "method", "status"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "obs_http_request_duration_seconds",
			Help: "HTTP request duration in seconds by route pattern and method.",
			// Not DefBuckets: it starts at 5ms, which cannot separate a 1ms
			// metadata lookup from a 4ms one, and both are normal here.
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"route", "method"}),
	}
}

func (m *HTTPMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.Requests, m.Duration}
}

// Observe records one completed request. route MUST be a bounded value — a chi
// route pattern or UnmatchedRoute — never a raw URL path.
func (m *HTTPMetrics) Observe(route, method string, status int, d time.Duration) {
	if route == "" {
		route = UnmatchedRoute
	}
	m.Requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.Duration.WithLabelValues(route, method).Observe(d.Seconds())
}

// IngestMetrics count accepted and rejected data at the ingest edge. The reason
// label is a closed set produced by the API layer's classifier; raw validation
// fields are client-influenced and never become label values.
type IngestMetrics struct {
	SamplesIngested  prometheus.Counter
	SamplesRejected  *prometheus.CounterVec
	LogLinesIngested prometheus.Counter
	LogLinesRejected *prometheus.CounterVec
}

// NewIngestMetrics builds the instruments without registering them.
func NewIngestMetrics() *IngestMetrics {
	return &IngestMetrics{
		SamplesIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "obs_samples_ingested_total",
			Help: "Total metric samples accepted and appended.",
		}),
		SamplesRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_samples_rejected_total",
			Help: "Total metric samples rejected, by reason.",
		}, []string{"reason"}),
		LogLinesIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "obs_log_lines_ingested_total",
			Help: "Total log lines accepted and appended.",
		}),
		LogLinesRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_log_lines_rejected_total",
			Help: "Total log lines rejected, by reason.",
		}, []string{"reason"}),
	}
}

func (m *IngestMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.SamplesIngested, m.SamplesRejected, m.LogLinesIngested, m.LogLinesRejected}
}
