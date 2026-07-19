package integration_test

import (
	"bytes"
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
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

// newLogServer builds a server whose logs ingester is a WALStore over logsWALDir,
// replaying any existing segments into a fresh MemoryStore first. It returns the
// server and the underlying log store for assertions.
func newLogServer(t *testing.T, dataDir, logsWALDir string) (*api.Server, *logs.MemoryStore, *logwal.LogWAL) {
	t.Helper()

	logStore := logs.NewMemoryStore()
	if err := logwal.Replay(logsWALDir, func(pairs []logwal.LabelPair, tsNs int64, line string) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		sl, err := logs.NewStreamLabels(lm)
		if err != nil {
			t.Errorf("replay NewStreamLabels: %v", err)
			return
		}
		if err := logStore.Append(sl, tsNs, line); err != nil {
			t.Errorf("replay Append: %v", err)
		}
	}); err != nil {
		t.Fatalf("logwal.Replay: %v", err)
	}

	lw, err := logwal.Open(logsWALDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("logwal.Open: %v", err)
	}

	cfg := &config.Config{HTTPAddr: ":0", DataDir: dataDir, LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	mstore := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(mstore)
	reg, _ := observability.NewRegistry(mstore, nil)
	logIngester := logs.NewWALStore(lw, logStore)
	return api.New(cfg, log, mstore, engine, reg, logIngester), logStore, lw
}

const pushBody = `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","first"],["1700000000000000001","second"]]}]}`

func TestLokiPush_BufferedAfterPush(t *testing.T) {
	dataDir := t.TempDir()
	logsWALDir := filepath.Join(dataDir, "logs", "wal")

	srv, store, lw := newLogServer(t, dataDir, logsWALDir)
	defer lw.Close()

	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader([]byte(pushBody)))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("push status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}

	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	if got := len(store.StreamEntries(logs.StreamIDOf(sl))); got != 2 {
		t.Errorf("buffered entries = %d, want 2", got)
	}
}

func TestLokiPush_SurvivesRestartViaReplay(t *testing.T) {
	dataDir := t.TempDir()
	logsWALDir := filepath.Join(dataDir, "logs", "wal")

	// --- First server: push two lines, then close the WAL (simulating shutdown). ---
	srv1, _, lw1 := newLogServer(t, dataDir, logsWALDir)
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader([]byte(pushBody)))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv1.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("push status = %d, want 204", rr.Code)
	}
	if err := lw1.Close(); err != nil {
		t.Fatalf("close lw1: %v", err)
	}

	// --- Second server: fresh MemoryStore rebuilt from WAL replay. ---
	_, store2, lw2 := newLogServer(t, dataDir, logsWALDir)
	defer lw2.Close()

	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	entries := store2.StreamEntries(logs.StreamIDOf(sl))
	if len(entries) != 2 {
		t.Fatalf("after restart: entries = %d, want 2", len(entries))
	}
	if entries[0].Line != "first" || entries[1].Line != "second" {
		t.Errorf("after restart entries = %+v, want first/second", entries)
	}
	if entries[0].TimestampNs != 1700000000000000000 {
		t.Errorf("after restart entries[0].TimestampNs = %d, want 1700000000000000000", entries[0].TimestampNs)
	}
}
