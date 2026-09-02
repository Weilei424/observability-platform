package api_test

import (
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
)

func newTestServer(t *testing.T, dataDir string) *api.Server {
	t.Helper()
	cfg := &config.Config{
		HTTPAddr: ":8080",
		DataDir:  dataDir,
		LogLevel: "info",
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(store)
	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Cardinality: store,
		WALs: []observability.WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 128, 1, nil }},
		},
	})
	return api.New(api.Deps{
		Config:      cfg,
		Logger:      log,
		Ingester:    store,
		Engine:      engine,
		Registry:    reg,
		LogIngester: logs.NewMemoryStore(),
		HTTP:        inst.HTTP,
		Ingest:      inst.Ingest,
	})
}

func TestHealthz_Returns200(t *testing.T) {
	srv := newTestServer(t, t.TempDir())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body.status = %q, want %q", body["status"], "ok")
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
}

func TestReadyz_WritableDir_Returns200(t *testing.T) {
	srv := newTestServer(t, t.TempDir())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestReadyz_UncreatableDir_Returns503(t *testing.T) {
	// DataDir does not exist — os.CreateTemp will fail with "no such file or directory"
	nonexistent := filepath.Join(t.TempDir(), "nonexistent-subdir")
	srv := newTestServer(t, nonexistent)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "unavailable" {
		t.Errorf("body.status = %q, want %q", body["status"], "unavailable")
	}
	if body["reason"] == "" {
		t.Error("body.reason should not be empty")
	}
}

// The server must work when Deps carries neither instruments nor a registry: that
// is how most tests construct it, and a nil dereference would surface as an
// unrelated handler test failing rather than as a constructor problem.
//
// package api_test cannot read Server.http, so this asserts the behaviour that
// depends on it: a request goes through the metrics middleware and comes back 200.
func TestNewWithoutInstrumentsStillServesRequests(t *testing.T) {
	store := metrics.NewMemoryStore()
	srv := api.New(api.Deps{
		Config:   &config.Config{HTTPAddr: ":8080", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Ingester: store,
		Engine:   metrics.NewQueryEngine(store),
	})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200 with no instruments and no registry configured", rec.Code)
	}
}
