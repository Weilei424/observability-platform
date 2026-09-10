package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// UnmatchedRoute is the route label for a request that matched no route. A 404
// has no route pattern, and recording the raw URL path instead would let anyone
// grow the registry by requesting nonexistent paths.
const UnmatchedRoute = "<unmatched>"

// UnknownMethod is the method label for a request whose HTTP method is not one
// of the nine standard methods in standardHTTPMethods below. net/http accepts
// any RFC 7230 token as a method, and chi only classifies one as invalid from
// inside ServeHTTP — after this middleware would already have recorded it — so
// an unrecognized method must collapse to a sentinel here, the same way an
// unmatched route collapses to UnmatchedRoute, or a client sending arbitrary
// method strings could grow the registry without bound.
const UnknownMethod = "<unknown>"

// standardHTTPMethods is the allowlist a method is measured against before it
// can become a label value: the nine methods RFC 7231/7238 define.
var standardHTTPMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodPost:    true,
	http.MethodPut:     true,
	http.MethodPatch:   true,
	http.MethodDelete:  true,
	http.MethodConnect: true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

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
// route pattern or UnmatchedRoute — never a raw URL path. method is sanitized
// against standardHTTPMethods here (not at each call site) so every caller is
// covered: an unbounded, client-supplied method string must never reach
// WithLabelValues.
func (m *HTTPMetrics) Observe(route, method string, status int, d time.Duration) {
	if route == "" {
		route = UnmatchedRoute
	}
	if !standardHTTPMethods[method] {
		method = UnknownMethod
	}
	m.Requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.Duration.WithLabelValues(route, method).Observe(d.Seconds())
}

// Reason label values for obs_samples_rejected_total and
// obs_log_lines_rejected_total. These are the single source of truth for both
// the API layer's field-to-reason classifiers (internal/api/reject_reason.go)
// and the preinitialization below: a reason must be added here before it can
// be produced or counted anywhere, so it cannot be introduced in one place
// only and silently missing from the other.
const (
	ReasonName      = "name"
	ReasonTimestamp = "timestamp"
	ReasonValue     = "value"
	ReasonValues    = "values"
	ReasonLine      = "line"
	ReasonLabels    = "labels"
	ReasonOther     = "other"
	ReasonAppend    = "append"
	ReasonBatch     = "batch"
)

// SampleRejectReasons is the closed set of reason values obs_samples_rejected_total
// can take. Order is display order, not significant otherwise.
var SampleRejectReasons = []string{
	ReasonName, ReasonTimestamp, ReasonValue, ReasonLabels, ReasonOther, ReasonAppend, ReasonBatch,
}

// LogLineRejectReasons is the closed set of reason values obs_log_lines_rejected_total
// can take.
var LogLineRejectReasons = []string{
	ReasonValues, ReasonTimestamp, ReasonLine, ReasonLabels, ReasonOther, ReasonAppend, ReasonBatch,
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

// NewIngestMetrics builds the instruments without registering them. Every
// reason in SampleRejectReasons and LogLineRejectReasons is given a
// zero-valued child up front: without this, a CounterVec child does not exist
// until its first Inc, so Prometheus sees the series go absent -> 1 rather
// than 0 -> 1, and rate() needs two points in its window to render anything.
// A single rejection can then be invisible on a dashboard whose entire job is
// to show rejections. Preinitializing is safe here because the label sets are
// closed and small — unlike obs_http_requests_total's route/method/status,
// which is not preinitialized for exactly that reason.
func NewIngestMetrics() *IngestMetrics {
	m := &IngestMetrics{
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
	for _, reason := range SampleRejectReasons {
		m.SamplesRejected.WithLabelValues(reason)
	}
	for _, reason := range LogLineRejectReasons {
		m.LogLinesRejected.WithLabelValues(reason)
	}
	return m
}

func (m *IngestMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.SamplesIngested, m.SamplesRejected, m.LogLinesIngested, m.LogLinesRejected}
}
