package integration_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

func newWALServer(t *testing.T, dataDir, walDir string) (*api.Server, *wal.WAL) {
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
	blockStore.MemStore().SetHeadFence(checkpoint + 1)

	w, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	cfg := &config.Config{HTTPAddr: ":0", DataDir: dataDir, LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := metrics.NewWALStore(w, blockStore, dataDir)
	engine := metrics.NewQueryEngine(blockStore)
	reg, _ := observability.NewRegistry(blockStore, nil)
	return api.New(cfg, log, store, engine, reg, logs.NewMemoryStore()), w
}

func TestIngestRestartQuery(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}

	// --- First server: ingest three samples ---
	srv1, w1 := newWALServer(t, dataDir, walDir)

	ingestBody, _ := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{"name": "restart_counter", "labels": map[string]string{"env": "test"}, "timestamp_ms": int64(1000), "value": float64(1)},
			map[string]any{"name": "restart_counter", "labels": map[string]string{"env": "test"}, "timestamp_ms": int64(2000), "value": float64(2)},
			map[string]any{"name": "restart_counter", "labels": map[string]string{"env": "test"}, "timestamp_ms": int64(3000), "value": float64(3)},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(ingestBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv1.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest status = %d, want 204; body: %s", rr.Code, rr.Body.String())
	}

	// Simulate shutdown: close WAL.
	if err := w1.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	// --- Second server: replay WAL, then query ---
	srv2, w2 := newWALServer(t, dataDir, walDir)
	defer w2.Close()

	// query_range for all three samples: start=1, end=3, step=1 (seconds)
	req2 := httptest.NewRequest(http.MethodGet,
		`/api/v1/query_range?query=restart_counter%7Benv%3D%22test%22%7D&start=1&end=3&step=1`, nil)
	rr2 := httptest.NewRecorder()
	srv2.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("query_range status = %d, want 200; body: %s", rr2.Code, rr2.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr2.Body.String())
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want %q; body: %s", resp.Status, "success", rr2.Body.String())
	}
	if len(resp.Data.Result) == 0 {
		t.Fatalf("no series in query result after restart; body: %s", rr2.Body.String())
	}
	if len(resp.Data.Result[0].Values) != 3 {
		t.Errorf("got %d values, want 3 (one per step tick); body: %s",
			len(resp.Data.Result[0].Values), rr2.Body.String())
	}
}
