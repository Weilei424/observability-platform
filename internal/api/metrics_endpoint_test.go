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
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics does not expose %s", name)
		}
	}
	// obs_collector_errors_total is deliberately not in the list above for the
	// same reason obs_samples_rejected_total, obs_log_lines_ingested_total, and
	// obs_log_lines_rejected_total are not: it is a *prometheus.CounterVec, and a
	// labelled counter with no observed child emits nothing at all — not even a
	// zero. Per collectors.go's walCollector/logsCollector (and the tests in
	// internal/observability/collectors_test.go, e.g.
	// TestScrapeSucceedsWhenACollectorFails), the "collector" label is only ever
	// populated by an actual scrape-time failure; a healthy WAL source, as this
	// fixture uses, never touches it. That is intentional: synthesizing a zero
	// here would be indistinguishable from "scraped and healthy" on a dashboard,
	// which is the exact failure mode walCollector's own gap-not-zero contract
	// (see its doc comment) is designed to avoid for the gauges it guards.
}
