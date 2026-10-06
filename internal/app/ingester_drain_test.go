package app_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
)

// hangingStore is a store that accepts requests and never answers them while
// the test runs. It never reads the body either, so the server does not notice
// a client giving up; the test's end releases it.
func hangingStore(t *testing.T) string {
	t.Helper()
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stop) }) // runs first
	return srv.URL
}

// Once a drain starts, the ingester takes no new writes on any route, even if
// the drain itself failed: a write accepted after the flush could be left
// only in this ingester's WAL behind a drain's 200.
func TestIngesterRefusesWritesOnceADrainStarts(t *testing.T) {
	a, err := app.Build(withPeers(testConfig(t, config.TargetIngester)), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(a.Close)
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusNoContent {
		t.Fatalf("push before the drain = %d, want 204", rec.Code)
	}
	if rec := do(a.Handler, http.MethodPost, "/internal/v1/drain", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("drain with the store down = %d, want 503", rec.Code)
	}
	for _, w := range []struct{ path, body string }{
		{"/api/v1/ingest/metrics", oneSample},
		{"/loki/api/v1/push", oneLine},
		{"/internal/v1/metrics/push", `{"series":[{"labels":{"__name__":"late"},"samples":[[2000,"1"]]}]}`},
		{"/internal/v1/logs/push", `{"streams":[{"labels":{"service":"late"},"entries":[[2000,"x"]]}]}`},
	} {
		if rec := do(a.Handler, http.MethodPost, w.path, w.body); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("POST %s after a drain started = %d, want 503; body %s", w.path, rec.Code, rec.Body)
		}
	}
}

// The shutdown drain takes what is left of the process's shutdown budget: a
// store that never answers cannot hold the stop past it.
func TestIngesterShutdownDrainHonorsTheShutdownBudget(t *testing.T) {
	cfg := testConfig(t, config.TargetIngester)
	cfg.StoreURL = hangingStore(t)
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusNoContent {
		t.Fatalf("metrics push = %d, want 204", rec.Code)
	}
	if rec := do(a.Handler, http.MethodPost, "/loki/api/v1/push", oneLine); rec.Code != http.StatusNoContent {
		t.Fatalf("logs push = %d, want 204", rec.Code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	a.CloseContext(ctx)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("CloseContext took %v with a 300ms budget", d)
	}
}
