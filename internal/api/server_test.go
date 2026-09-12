package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-chi/chi/v5"
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

// The router is the only authority on what the server serves. Router() exists so
// tests and tooling can enumerate it with chi.Walk instead of re-deriving it by
// grepping router.go — a text search reports /metrics as unconditional (it is
// registered only when Deps.Registry is non-nil) and goes blind the moment a
// route moves to a sub-router.
func TestRouterEnumeratesEveryRegisteredRoute(t *testing.T) {
	srv := newTestServer(t, t.TempDir())

	got := map[string]bool{}
	err := chi.Walk(srv.Router(), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		got[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}

	for _, want := range []string{
		"GET /healthz",
		"GET /readyz",
		"POST /api/v1/ingest/metrics",
		"GET /api/v1/query",
		"POST /api/v1/query",
		"GET /api/v1/query_range",
		"GET /api/v1/labels",
		"GET /api/v1/label/{name}/values",
		"GET /api/v1/series",
		"POST /loki/api/v1/push",
		"GET /loki/api/v1/query_range",
		"GET /loki/api/v1/label/{name}/values",
	} {
		if !got[want] {
			t.Errorf("Router() does not serve %q; walked %d routes: %v", want, len(got), slices.Sorted(maps.Keys(got)))
		}
	}
	if len(got) == 0 {
		t.Fatal("chi.Walk found no routes at all; the walk callback or the router changed shape")
	}
}

// /metrics is registered only when a Registry is supplied. A Deps without one is
// normal in tests, and registering promhttp over a nil registry panics at request
// time — so the route must genuinely be absent, not present and broken.
func TestRouterServesMetricsOnlyWithARegistry(t *testing.T) {
	store := metrics.NewMemoryStore()
	srv := api.New(api.Deps{
		Config:      &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Ingester:    store,
		Engine:      metrics.NewQueryEngine(store),
		LogIngester: logs.NewMemoryStore(),
	})

	sawMetrics := false
	if err := chi.Walk(srv.Router(), func(_, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route == "/metrics" {
			sawMetrics = true
		}
		return nil
	}); err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}
	if sawMetrics {
		t.Error("/metrics is registered without a Registry; promhttp over a nil registry panics at request time")
	}

	withReg := newTestServer(t, t.TempDir())
	sawMetrics = false
	if err := chi.Walk(withReg.Router(), func(_, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route == "/metrics" {
			sawMetrics = true
		}
		return nil
	}); err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}
	if !sawMetrics {
		t.Error("/metrics is not registered even with a Registry")
	}
}
