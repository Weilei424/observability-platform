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
