package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// newTestServer creates a WALStore-backed api.Server against walDir, replaying
// any existing WAL segments before opening a fresh write segment.
// Caller must close the returned *wal.WAL when done.
func newTestServer(t *testing.T, dataDir, walDir string) (*api.Server, *wal.WAL) {
	t.Helper()

	blockStore, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	checkpoint := metrics.ReadCheckpoint(dataDir)
	if err := wal.ReplayFrom(walDir, checkpoint, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		labels, err := metrics.NewLabels(lm)
		if err != nil {
			t.Errorf("replay NewLabels: %v", err)
			return
		}
		if err := blockStore.Append(labels, tsMs, value); err != nil {
			t.Errorf("replay Append: %v", err)
		}
	}); err != nil {
		t.Fatalf("wal.ReplayFrom: %v", err)
	}

	w, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	cfg := &config.Config{HTTPAddr: ":0", DataDir: dataDir, LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := metrics.NewWALStore(w, blockStore, dataDir)
	engine := metrics.NewQueryEngine(blockStore)
	reg := observability.NewRegistry(blockStore)
	return api.New(cfg, log, store, engine, reg), w
}

// ingestSamples POSTs a batch to /api/v1/ingest/metrics and fails the test on
// anything other than 204.
func ingestSamples(t *testing.T, srv http.Handler, entries []map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"metrics": entries})
	if err != nil {
		t.Fatalf("marshal ingest body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: got %d, want 204; body: %s", rr.Code, rr.Body.String())
	}
}

// queryInstantCount issues GET /api/v1/query and returns the number of series
// in the result vector.
func queryInstantCount(t *testing.T, srv http.Handler, selector string, tsSec float64) int {
	t.Helper()
	url := fmt.Sprintf("/api/v1/query?query=%s&time=%.3f", selector, tsSec)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query %s: got %d, want 200; body: %s", selector, rr.Code, rr.Body.String())
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string           `json:"resultType"`
			Result     []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal query response: %v; body: %s", err, rr.Body.String())
	}
	if resp.Status != "success" {
		t.Fatalf("query status = %q, want \"success\"; body: %s", resp.Status, rr.Body.String())
	}
	if resp.Data.ResultType != "vector" {
		t.Fatalf("query %s: resultType = %q, want \"vector\"", selector, resp.Data.ResultType)
	}
	return len(resp.Data.Result)
}

// TestMetricsEndToEnd verifies the complete ingest → instant query → WAL
// restart → instant query path using the demo metric names.
func TestMetricsEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")

	srv1, w1 := newTestServer(t, dataDir, walDir)

	const tsMs = int64(1_000_000) // 1000 s in milliseconds
	const tsSec = float64(1000)   // same instant as float seconds for query

	ingestSamples(t, srv1, []map[string]any{
		{
			"name":         "http_requests_total",
			"labels":       map[string]string{"service": "api", "method": "GET", "status": "200"},
			"timestamp_ms": tsMs,
			"value":        float64(1),
		},
		{
			"name":         "http_requests_total",
			"labels":       map[string]string{"service": "api", "method": "POST", "status": "201"},
			"timestamp_ms": tsMs,
			"value":        float64(2),
		},
		{
			"name":         "http_request_duration_seconds",
			"labels":       map[string]string{"service": "api"},
			"timestamp_ms": tsMs,
			"value":        float64(0.012),
		},
	})

	if n := queryInstantCount(t, srv1, "http_requests_total", tsSec); n != 2 {
		t.Errorf("before restart: http_requests_total series count = %d, want 2", n)
	}
	if n := queryInstantCount(t, srv1, "http_request_duration_seconds", tsSec); n != 1 {
		t.Errorf("before restart: http_request_duration_seconds series count = %d, want 1", n)
	}

	// Simulate process stop: close the WAL writer.
	if err := w1.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	// Simulate restart: new server replays from the same WAL dir.
	srv2, w2 := newTestServer(t, dataDir, walDir)
	defer w2.Close()

	if n := queryInstantCount(t, srv2, "http_requests_total", tsSec); n != 2 {
		t.Errorf("after restart: http_requests_total series count = %d, want 2 (WAL replay failed?)", n)
	}
	if n := queryInstantCount(t, srv2, "http_request_duration_seconds", tsSec); n != 1 {
		t.Errorf("after restart: http_request_duration_seconds series count = %d, want 1 (WAL replay failed?)", n)
	}
}

// TestMetricsRangeQuery verifies that range queries respect time boundaries.
// Ingests 6 samples at 10 s intervals starting at t=1000 s; checks that a full
// window returns all 6 points and a narrow window returns only 2.
func TestMetricsRangeQuery(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")

	srv, w := newTestServer(t, dataDir, walDir)
	defer w.Close()

	// 6 samples: t=1000s, 1010s, 1020s, 1030s, 1040s, 1050s
	const t0Ms = int64(1_000_000) // 1000 s in ms
	const stepMs = int64(10_000)  // 10 s in ms
	entries := make([]map[string]any, 6)
	for i := range entries {
		entries[i] = map[string]any{
			"name":         "http_requests_total",
			"labels":       map[string]string{"service": "api", "method": "GET", "status": "200"},
			"timestamp_ms": t0Ms + int64(i)*stepMs,
			"value":        float64(i + 1),
		}
	}
	ingestSamples(t, srv, entries)

	// Full window: start=1000s end=1050s step=10s → ticks 1000,1010,...,1050 → 6 points
	if n := queryRangePoints(t, srv, 1000, 1050, 10); n != 6 {
		t.Errorf("full window: got %d points, want 6", n)
	}

	// Narrow window: start=1020s end=1030s step=10s → ticks 1020,1030 → 2 points
	if n := queryRangePoints(t, srv, 1020, 1030, 10); n != 2 {
		t.Errorf("narrow window: got %d points, want 2", n)
	}
}

// queryRangePoints issues GET /api/v1/query_range for http_requests_total and
// returns the number of data points in the first result series.
func queryRangePoints(t *testing.T, srv http.Handler, startSec, endSec, stepSec float64) int {
	t.Helper()
	url := fmt.Sprintf(
		"/api/v1/query_range?query=http_requests_total&start=%.3f&end=%.3f&step=%.3f",
		startSec, endSec, stepSec,
	)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query_range: got %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
	}
	if resp.Status != "success" {
		t.Fatalf("query_range status = %q, want \"success\"; body: %s", resp.Status, rr.Body.String())
	}
	if len(resp.Data.Result) == 0 {
		return 0
	}
	return len(resp.Data.Result[0].Values)
}
