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

func TestQuery_RangeQuery_InvalidStep_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=1&end=3&step=0")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_RangeQuery_EndBeforeStart_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=5&end=1&step=1")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_RateInstantQuery_EmptyStore_ReturnsEmptyVector(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/query?" + url.Values{
		"query": {"rate(http_requests_total[5m])"},
		"time":  {"1000"},
	}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 0 {
		t.Errorf("result len = %d, want 0 (no data)", len(result))
	}
}

func TestQuery_IngestThenInstantQuery_ReturnsIngestedValue(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	// Ingest via HTTP POST
	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "http_requests_total",
				"labels":       map[string]string{"service": "api"},
				"timestamp_ms": int64(1000),
				"value":        float64(42),
			},
		},
	})
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest status = %d, want 204; body: %s", rr.Code, rr.Body.String())
	}

	// Query via HTTP GET
	u := "/api/v1/query?" + url.Values{
		"query": {`http_requests_total{service="api"}`},
		"time":  {"1"},
	}.Encode()
	rr = getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("query status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	sample := result[0].(map[string]any)
	value := sample["value"].([]any)
	if value[1] != "42" {
		t.Errorf("value = %v, want \"42\"", value[1])
	}
}

func TestQuery_NonFiniteTimeParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	cases := []struct {
		name string
		url  string
	}{
		{"instant NaN", "/api/v1/query?query=cpu_usage&time=NaN"},
		{"instant +Inf", "/api/v1/query?query=cpu_usage&time=%2BInf"},
		{"instant -Inf", "/api/v1/query?query=cpu_usage&time=-Inf"},
		{"range start NaN", "/api/v1/query_range?query=cpu_usage&start=NaN&end=3&step=1"},
		{"range end +Inf", "/api/v1/query_range?query=cpu_usage&start=1&end=%2BInf&step=1"},
		{"range step +Inf", "/api/v1/query_range?query=cpu_usage&start=1&end=3&step=%2BInf"},
	}

	for _, tc := range cases {
		rr := getQuery(t, srv, tc.url)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body: %s", tc.name, rr.Code, rr.Body.String())
			continue
		}
		body := decodePromResponse(t, rr)
		if body["errorType"] != "bad_data" {
			t.Errorf("%s: errorType = %v, want bad_data", tc.name, body["errorType"])
		}
	}
}

func TestQuery_RFC3339TimeParam_AcceptedOnInstantQuery(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	// Sample at Unix 1000ms = 1970-01-01T00:00:01Z
	_ = store.Append(labels, 1000, 7.0)

	// time as RFC3339
	u := "/api/v1/query?" + url.Values{"query": {"cpu_usage"}, "time": {"1970-01-01T00:00:01Z"}}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	sample := result[0].(map[string]any)
	value := sample["value"].([]any)
	if value[1] != "7" {
		t.Errorf("value = %v, want \"7\"", value[1])
	}
}

func TestQuery_RFC3339StartEnd_AcceptedOnRangeQuery(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 5.0)

	u := "/api/v1/query_range?" + url.Values{
		"query": {"cpu_usage"},
		"start": {"1970-01-01T00:00:01Z"},
		"end":   {"1970-01-01T00:00:01Z"},
		"step":  {"1"},
	}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
}

func TestQuery_DurationStep_OutOfOrderUnits_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	cases := []string{
		"1m1h",   // m before h — out of order
		"1h1h",   // repeated unit
		"1s1s",   // repeated unit
		"1ms1s",  // ms before s — out of order
		"1m1m1s", // repeated m
	}
	for _, step := range cases {
		u := "/api/v1/query_range?" + url.Values{
			"query": {"cpu_usage"},
			"start": {"1"},
			"end":   {"60"},
			"step":  {step},
		}.Encode()
		rr := getQuery(t, srv, u)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("step=%q: status = %d, want 400; body: %s", step, rr.Code, rr.Body.String())
			continue
		}
		body := decodePromResponse(t, rr)
		if body["errorType"] != "bad_data" {
			t.Errorf("step=%q: errorType = %v, want bad_data", step, body["errorType"])
		}
	}
}

func TestQuery_DurationStep_AcceptedOnRangeQuery(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 16000, 2.0)

	// step=15s should be parsed as 15000ms
	u := "/api/v1/query_range?" + url.Values{
		"query": {"cpu_usage"},
		"start": {"1"},
		"end":   {"16"},
		"step":  {"15s"},
	}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	// step=15s over [1s,16s] → ticks at 1s and 16s
	values := result[0].(map[string]any)["values"].([]any)
	if len(values) != 2 {
		t.Fatalf("values len = %d, want 2 (ticks at 1s and 16s)", len(values))
	}
}

func TestQuery_IngestThenRangeQuery_ReturnsIngestedValues(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	// Ingest two samples at t=1s and t=3s
	for _, m := range []struct {
		ts  int64
		val float64
	}{{1000, 10}, {3000, 30}} {
		rr := postIngest(t, srv, map[string]any{
			"metrics": []any{
				map[string]any{
					"name":         "cpu_usage",
					"timestamp_ms": m.ts,
					"value":        m.val,
				},
			},
		})
		if rr.Code != http.StatusNoContent {
			t.Fatalf("ingest status = %d; body: %s", rr.Code, rr.Body.String())
		}
	}

	// Range query: start=1, end=3, step=1 → ticks at 1000ms, 2000ms, 3000ms
	rr := getQuery(t, srv, "/api/v1/query_range?query=cpu_usage&start=1&end=3&step=1")
	if rr.Code != http.StatusOK {
		t.Fatalf("query status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	values := result[0].(map[string]any)["values"].([]any)
	if len(values) != 3 {
		t.Fatalf("values len = %d, want 3", len(values))
	}
	// tick 2000ms (t=2s) carries forward value 10 from the sample at 1000ms
	pair := values[1].([]any)
	if pair[1] != "10" {
		t.Errorf("values[1] = %v, want \"10\"", pair[1])
	}
}

func TestQuery_RateInstantQuery_ReturnsCorrectRate(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "requests_total"})
	// value 0 at t=0, value 60 at t=60s → rate = 1.0/sec
	_ = store.Append(labels, 0, 0.0)
	_ = store.Append(labels, 60000, 60.0)

	u := "/api/v1/query?" + url.Values{
		"query": {"rate(requests_total[60s])"},
		"time":  {"60"},
	}.Encode()
	rr := getQuery(t, srv, u)
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
	value := result[0].(map[string]any)["value"].([]any)
	if value[1] != "1" {
		t.Errorf("rate = %v, want \"1\"", value[1])
	}
}

func TestQuery_RateRangeQuery_ReturnsMatrixWithRatePerTick(t *testing.T) {
	srv, store := newQueryTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "requests_total"})
	_ = store.Append(labels, 0, 0.0)
	_ = store.Append(labels, 30000, 30.0)
	_ = store.Append(labels, 60000, 60.0)
	_ = store.Append(labels, 90000, 90.0)

	// rate(requests_total[60s]) over [60s, 90s] step 30s
	// tick 60s: [0s,60s] → 60/60=1.0; tick 90s: [30s,90s] → 60/60=1.0
	u := "/api/v1/query_range?" + url.Values{
		"query": {"rate(requests_total[60s])"},
		"start": {"60"},
		"end":   {"90"},
		"step":  {"30"},
	}.Encode()
	rr := getQuery(t, srv, u)
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
	values := result[0].(map[string]any)["values"].([]any)
	if len(values) != 2 {
		t.Fatalf("values len = %d, want 2 (ticks at 60s and 90s)", len(values))
	}
	for i, v := range values {
		pair := v.([]any)
		if pair[1] != "1" {
			t.Errorf("values[%d] rate = %v, want \"1\"", i, pair[1])
		}
	}
}

func TestQuery_SumByRangeQuery_ReturnsGroupedMatrix(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "requests_total", "service": "api"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "requests_total", "service": "db"})
	_ = store.Append(la, 1000, 10.0)
	_ = store.Append(lb, 1000, 5.0)
	_ = store.Append(la, 2000, 20.0)
	_ = store.Append(lb, 2000, 8.0)

	u := "/api/v1/query_range?" + url.Values{
		"query": {"sum by (service)(requests_total)"},
		"start": {"1"},
		"end":   {"2"},
		"step":  {"1"},
	}.Encode()
	rr := getQuery(t, srv, u)
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
	if len(result) != 2 {
		t.Fatalf("result len = %d, want 2 (api and db groups)", len(result))
	}
	// Each group's metric map should contain service but not __name__
	for _, r := range result {
		metric := r.(map[string]any)["metric"].(map[string]any)
		if _, hasName := metric["__name__"]; hasName {
			t.Errorf("output metric should not contain __name__, got %v", metric)
		}
		if _, hasSvc := metric["service"]; !hasSvc {
			t.Errorf("output metric should contain service label, got %v", metric)
		}
	}
}

func TestQuery_UnknownFunction_Returns400WithBadData(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/query?" + url.Values{
		"query": {"avg(cpu_usage)"},
		"time":  {"1000"},
	}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestQuery_ScalarArithmetic_ReturnsScalarResultType(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/query?" + url.Values{
		"query": {"1+1"},
		"time":  {"1000"},
	}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	data, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("data field missing or wrong type: %v", body)
	}
	if data["resultType"] != "scalar" {
		t.Errorf("resultType = %v, want scalar", data["resultType"])
	}
	result, ok := data["result"].([2]any)
	if !ok {
		// JSON unmarshals [ts, "2"] as []any
		raw, ok2 := data["result"].([]any)
		if !ok2 || len(raw) != 2 {
			t.Fatalf("result = %v, want [timestamp, value]", data["result"])
		}
		if raw[1] != "2" {
			t.Errorf("scalar value = %v, want \"2\"", raw[1])
		}
		return
	}
	_ = result
}
