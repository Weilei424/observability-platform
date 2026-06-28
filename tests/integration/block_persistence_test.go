package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// TestBlockPersistence_IngestFlushRestartQuery is the Phase 3.2 DoD test.
// It verifies that flushed samples survive a simulated restart (new store from
// same dataDir) and are returned by a range query.
func TestBlockPersistence_IngestFlushRestartQuery(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}

	// --- Phase A: ingest 120 samples via HTTP, close WAL ---
	srv1, w1 := newWALServer(t, dataDir, walDir)

	ingestPayload := buildIngestPayload(t, "block_counter", map[string]string{"env": "test"}, 120)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(ingestPayload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv1.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close w1: %v", err)
	}

	// --- Phase B: replay WAL into a fresh store, flush to block ---
	ws := buildWALStore(t, dataDir, walDir)
	if _, err := ws.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// --- Phase C: simulate restart — fresh store loads block, query returns all samples ---
	srv2, w2 := newWALServer(t, dataDir, walDir)
	defer w2.Close()

	// Query all 120 samples: timestamps 0..119 seconds (step=1s).
	req2 := httptest.NewRequest(http.MethodGet,
		`/api/v1/query_range?query=block_counter%7Benv%3D%22test%22%7D&start=0&end=119&step=1`, nil)
	rr2 := httptest.NewRecorder()
	srv2.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("query_range status = %d; body: %s", rr2.Code, rr2.Body.String())
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body: %s", err, rr2.Body.String())
	}
	if resp.Status != "success" {
		t.Fatalf("status = %q, want success; body: %s", resp.Status, rr2.Body.String())
	}
	if len(resp.Data.Result) == 0 {
		t.Fatalf("no series in result after restart; body: %s", rr2.Body.String())
	}
	if got := len(resp.Data.Result[0].Values); got != 120 {
		t.Errorf("got %d values after restart, want 120; body: %s", got, rr2.Body.String())
	}
}

// buildIngestPayload creates a JSON ingest payload with n samples for the given metric.
// Timestamps are i*1000 ms (i = 0..n-1); values are float64(i).
func buildIngestPayload(t *testing.T, name string, lbls map[string]string, n int) []byte {
	t.Helper()
	samples := make([]any, n)
	for i := 0; i < n; i++ {
		samples[i] = map[string]any{
			"name":         name,
			"labels":       lbls,
			"timestamp_ms": int64(i * 1000),
			"value":        float64(i),
		}
	}
	data, err := json.Marshal(map[string]any{"metrics": samples})
	if err != nil {
		t.Fatalf("marshal ingest payload: %v", err)
	}
	return data
}

func buildWALStore(t *testing.T, dataDir, walDir string) *metrics.WALStore {
	t.Helper()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	checkpoint := metrics.ReadCheckpoint(dataDir)
	replayInto(t, walDir, checkpoint, bs)
	bs.MemStore().SetHeadFence(checkpoint + 1)
	w, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return metrics.NewWALStore(w, bs, dataDir)
}

func replayInto(t *testing.T, walDir string, afterSegment int, bs *metrics.BlockStore) {
	t.Helper()
	if err := wal.ReplayFrom(walDir, afterSegment, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		lbs, err := metrics.NewLabels(lm)
		if err != nil {
			t.Errorf("replay NewLabels: %v", err)
			return
		}
		if err := bs.Append(lbs, tsMs, value); err != nil {
			t.Errorf("replay Append: %v", err)
		}
	}); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
}
