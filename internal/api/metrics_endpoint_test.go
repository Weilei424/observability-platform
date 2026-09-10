package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpoint_ExposesCardinality(t *testing.T) {
	srv := newTestServer(t, t.TempDir())

	// Ingest two series.
	postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "http_requests_total", "labels": map[string]string{"job": "api"}, "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})
	postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "http_requests_total", "labels": map[string]string{"job": "web"}, "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "obs_active_series 2") {
		t.Fatalf("/metrics body missing obs_active_series 2:\n%s", body)
	}
}

// Pins the metric names the self-observability dashboard queries. A rename here
// silently empties a panel, and nothing else in the Go suite would notice.
func TestMetricsEndpointExposesTheDashboardMetricNames(t *testing.T) {
	srv := newTestServer(t, t.TempDir())

	// One ingest and one read so both the HTTP and the ingest instruments have a
	// series to expose — a counter with no observations is not in the output at all.
	postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "demo", "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"obs_http_requests_total",
		"obs_http_request_duration_seconds",
		"obs_samples_ingested_total",
		"obs_wal_bytes",
		"obs_wal_segments",
		"obs_active_series",
		// obs_samples_rejected_total and obs_log_lines_rejected_total are
		// *prometheus.CounterVec, and obs_collector_errors_total is too; this
		// fixture never observes any of the three. They still appear:
		// NewIngestMetrics and NewRegistry preinitialize a zero-valued child
		// for every reason in observability.SampleRejectReasons /
		// LogLineRejectReasons, and for each of observability.CollectorNames,
		// specifically so a labelled failure counter reads absent(0) -> 1 on
		// its first observation instead of absent -> 1, which rate() cannot
		// render at all. Before that preinitialization a CounterVec with no
		// observed child emitted nothing on a healthy scrape, which is why
		// these three were once excluded from this list. (They were joined
		// in that exclusion, inaccurately, by obs_log_lines_ingested_total: a
		// plain, unlabelled Counter always reports its zero value once
		// registered, preinitialization or not, so it belonged in this list
		// all along.)
		"obs_samples_rejected_total",
		"obs_log_lines_ingested_total",
		"obs_log_lines_rejected_total",
		"obs_collector_errors_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics does not expose %s", name)
		}
	}
}
