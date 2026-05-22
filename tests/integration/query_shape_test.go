package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstantQueryResponseShape(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srv, w := newWALServer(t, dataDir, walDir)
	defer w.Close()

	// Ingest one sample at t=5s (5000ms)
	body, _ := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "shape_test_metric",
				"labels":       map[string]string{"env": "test"},
				"timestamp_ms": int64(5000),
				"value":        float64(99),
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: got %d; body: %s", rr.Code, rr.Body.String())
	}

	// Instant query at time=5 (seconds)
	req = httptest.NewRequest(http.MethodGet, "/api/v1/query?query=shape_test_metric&time=5", nil)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query: got %d; body: %s", rr.Code, rr.Body.String())
	}

	var envelope struct {
		Status   string          `json:"status"`
		Warnings json.RawMessage `json:"warnings"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
	}

	if envelope.Status != "success" {
		t.Errorf("status = %q, want success", envelope.Status)
	}
	// warnings must be present as a JSON array
	if len(envelope.Warnings) == 0 || envelope.Warnings[0] != '[' {
		t.Errorf("warnings = %s, want JSON array", envelope.Warnings)
	}
	if envelope.Data.ResultType != "vector" {
		t.Errorf("resultType = %q, want vector", envelope.Data.ResultType)
	}
	if len(envelope.Data.Result) != 1 {
		t.Fatalf("result len = %d, want 1", len(envelope.Data.Result))
	}
	if _, ok := envelope.Data.Result[0].Metric["__name__"]; !ok {
		t.Error("metric map missing __name__ key")
	}

	pair := envelope.Data.Result[0].Value
	if len(pair) != 2 {
		t.Fatalf("value len = %d, want 2", len(pair))
	}
	// value[0]: timestamp as float seconds (not milliseconds)
	var ts float64
	if err := json.Unmarshal(pair[0], &ts); err != nil {
		t.Errorf("value[0] is not a float64 timestamp: %s — %v", pair[0], err)
	}
	if ts != 5.0 {
		t.Errorf("value[0] timestamp = %v, want 5.0 (seconds, not ms)", ts)
	}
	// value[1]: sample value as a quoted string
	var val string
	if err := json.Unmarshal(pair[1], &val); err != nil {
		t.Errorf("value[1] is not a string: %s — %v", pair[1], err)
	}
	if val != "99" {
		t.Errorf("value[1] = %q, want \"99\"", val)
	}
}

func TestRangeQueryResponseShape(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srv, w := newWALServer(t, dataDir, walDir)
	defer w.Close()

	// Ingest two samples at t=1s and t=2s
	body, _ := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{"name": "shape_range_metric", "timestamp_ms": int64(1000), "value": float64(10)},
			map[string]any{"name": "shape_range_metric", "timestamp_ms": int64(2000), "value": float64(20)},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: got %d; body: %s", rr.Code, rr.Body.String())
	}

	// Range query: start=1s, end=2s, step=1s → ticks at 1s and 2s
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/query_range?query=shape_range_metric&start=1&end=2&step=1", nil)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query_range: got %d; body: %s", rr.Code, rr.Body.String())
	}

	var envelope struct {
		Status   string          `json:"status"`
		Warnings json.RawMessage `json:"warnings"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string   `json:"metric"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
	}

	if envelope.Status != "success" {
		t.Errorf("status = %q, want success", envelope.Status)
	}
	if len(envelope.Warnings) == 0 || envelope.Warnings[0] != '[' {
		t.Errorf("warnings = %s, want JSON array", envelope.Warnings)
	}
	if envelope.Data.ResultType != "matrix" {
		t.Errorf("resultType = %q, want matrix", envelope.Data.ResultType)
	}
	if len(envelope.Data.Result) != 1 {
		t.Fatalf("result len = %d, want 1", len(envelope.Data.Result))
	}

	values := envelope.Data.Result[0].Values
	if len(values) != 2 {
		t.Fatalf("values len = %d, want 2 (ticks at 1s and 2s)", len(values))
	}

	// First value pair: [1.0, "10"]
	pair := values[0]
	if len(pair) != 2 {
		t.Fatalf("values[0] len = %d, want 2", len(pair))
	}
	var ts float64
	if err := json.Unmarshal(pair[0], &ts); err != nil {
		t.Errorf("values[0][0] is not a float64 timestamp: %s — %v", pair[0], err)
	}
	if ts != 1.0 {
		t.Errorf("values[0][0] = %v, want 1.0 (seconds, not ms)", ts)
	}
	var val string
	if err := json.Unmarshal(pair[1], &val); err != nil {
		t.Errorf("values[0][1] is not a string: %s — %v", pair[1], err)
	}
	if val != "10" {
		t.Errorf("values[0][1] = %q, want \"10\"", val)
	}

	// Second value pair: [2.0, "20"]
	pair = values[1]
	if len(pair) != 2 {
		t.Fatalf("values[1] len = %d, want 2", len(pair))
	}
	if err := json.Unmarshal(pair[0], &ts); err != nil {
		t.Errorf("values[1][0] is not a float64 timestamp: %s — %v", pair[0], err)
	}
	if ts != 2.0 {
		t.Errorf("values[1][0] = %v, want 2.0 (seconds, not ms)", ts)
	}
	if err := json.Unmarshal(pair[1], &val); err != nil {
		t.Errorf("values[1][1] is not a string: %s — %v", pair[1], err)
	}
	if val != "20" {
		t.Errorf("values[1][1] = %q, want \"20\"", val)
	}
}

func TestGrafanaStylePOSTInstantQuery(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srv, w := newWALServer(t, dataDir, walDir)
	defer w.Close()

	// Ingest one sample at t=1s
	body, _ := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "grafana_post_instant_metric",
				"timestamp_ms": int64(1000),
				"value":        float64(77),
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: got %d; body: %s", rr.Code, rr.Body.String())
	}

	// POST /api/v1/query with URL-encoded form body (Grafana datasource style)
	form := url.Values{
		"query": {"grafana_post_instant_metric"},
		"time":  {"1"},
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/query",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST query: got %d; body: %s", rr.Code, rr.Body.String())
	}

	var envelope struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
	}
	if envelope.Status != "success" {
		t.Errorf("status = %q, want success", envelope.Status)
	}
	if envelope.Data.ResultType != "vector" {
		t.Errorf("resultType = %q, want vector", envelope.Data.ResultType)
	}
	if len(envelope.Data.Result) == 0 {
		t.Fatal("result is empty, want at least one series")
	}
}

func TestGrafanaStylePOSTQuery(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srv, w := newWALServer(t, dataDir, walDir)
	defer w.Close()

	// Ingest one sample
	body, _ := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "grafana_post_metric",
				"timestamp_ms": int64(1000),
				"value":        float64(42),
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: got %d; body: %s", rr.Code, rr.Body.String())
	}

	// POST query_range with URL-encoded form body (Grafana datasource style)
	form := url.Values{
		"query": {"grafana_post_metric"},
		"start": {"1"},
		"end":   {"1"},
		"step":  {"1"},
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/query_range",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST query_range: got %d; body: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
	}
	if resp.Status != "success" {
		t.Errorf("status = %q, want success", resp.Status)
	}
	if resp.Data.ResultType != "matrix" {
		t.Errorf("resultType = %q, want matrix", resp.Data.ResultType)
	}

	var fullResp struct {
		Data struct {
			Result []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &fullResp); err != nil {
		t.Fatalf("unmarshal full: %v", err)
	}
	if len(fullResp.Data.Result) == 0 {
		t.Fatal("result is empty, want at least one series")
	}
	if len(fullResp.Data.Result[0].Values) == 0 {
		t.Fatal("values is empty, want at least one data point")
	}
}
