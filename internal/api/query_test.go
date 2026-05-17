package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func newQueryTestServer(t *testing.T) (*api.Server, *metrics.MemoryStore) {
	t.Helper()
	store := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(store)
	cfg := &config.Config{
		HTTPAddr: ":8080",
		DataDir:  t.TempDir(),
		LogLevel: "info",
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return api.New(cfg, log, store, engine), store
}

func getQuery(t *testing.T, srv *api.Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func decodePromResponse(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return body
}

func TestQuery_InstantQuery_ReturnsLatestSample(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "host": "a"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 2000, 2.0)

	// time=1.5 (1500ms) → latest sample at or before 1500ms is the one at 1000ms
	rr := getQuery(t, srv, "/api/v1/query?query=cpu_usage&time=1.5")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}

	data := body["data"].(map[string]any)
	if data["resultType"] != "vector" {
		t.Errorf("resultType = %v, want vector", data["resultType"])
	}
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	sample := result[0].(map[string]any)
	metricMap := sample["metric"].(map[string]any)
	if metricMap["__name__"] != "cpu_usage" {
		t.Errorf("__name__ = %v, want cpu_usage", metricMap["__name__"])
	}
	value := sample["value"].([]any)
	if value[1] != "1" {
		t.Errorf("value = %v, want \"1\"", value[1])
	}
}

func TestQuery_InstantQuery_UnknownMetric_ReturnsEmptyResult(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query?query=nonexistent&time=1000")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 0 {
		t.Errorf("result len = %d, want 0", len(result))
	}
}

func TestQuery_InstantQuery_MissingQueryParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "error" {
		t.Errorf("status = %v, want error", body["status"])
	}
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_InstantQuery_InvalidSelector_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/query?" + url.Values{"query": {`metric{service!="api"}`}, "time": {"1000"}}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_RangeQuery_ReturnsStepAlignedPoints(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	// Samples at 1000ms and 3000ms
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 3000, 3.0)

	// start=1, end=3, step=1 → ticks at 1000ms, 2000ms, 3000ms
	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=1&end=3&step=1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}

	data := body["data"].(map[string]any)
	if data["resultType"] != "matrix" {
		t.Errorf("resultType = %v, want matrix", data["resultType"])
	}
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}

	series := result[0].(map[string]any)
	values := series["values"].([]any)
	// 3 ticks: 1→value 1.0, 2→value 1.0 (lookback), 3→value 3.0
	if len(values) != 3 {
		t.Fatalf("values len = %d, want 3", len(values))
	}

	wantTimestamps := []float64{1.0, 2.0, 3.0}
	wantValues := []string{"1", "1", "3"}
	for i, v := range values {
		pair := v.([]any)
		if pair[0].(float64) != wantTimestamps[i] {
			t.Errorf("values[%d] timestamp = %v, want %v", i, pair[0], wantTimestamps[i])
		}
		if pair[1] != wantValues[i] {
			t.Errorf("values[%d] value = %v, want %v", i, pair[1], wantValues[i])
		}
	}
}

func TestQuery_RangeQuery_MissingQueryParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?start=1&end=3&step=1")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_RangeQuery_MissingStartParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&end=3&step=1")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestQuery_RangeQuery_MissingEndParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=1&step=1")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestQuery_RangeQuery_MissingStepParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=1&end=3")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestQuery_RangeQuery_InvalidSelector_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/query_range?" + url.Values{"query": {"metric{bad"}, "start": {"1"}, "end": {"3"}, "step": {"1"}}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}
