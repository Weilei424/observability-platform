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

// A background replica push to a hung ingester outlives the write it belongs
// to; the gateway's close waits for it only as long as the shutdown budget
// allows, not for the push's own timeout.
func TestGatewayCloseDoesNotOutliveItsContext(t *testing.T) {
	acks := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(hung.Close)
	t.Cleanup(func() { close(release) }) // runs first: frees the hung handler so Close returns

	cfg := testConfig(t, config.TargetGateway)
	cfg.IngesterURLs = []string{acks(), acks(), hung.URL}
	cfg.QuerierURL = "http://127.0.0.1:1"
	cfg.ReplicationFactor = 3
	cfg.IngesterTimeout = 10 * time.Second
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	body := `{"metrics":[{"name":"m","labels":{"job":"x"},"timestamp_ms":1,"value":2}]}`
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != http.StatusNoContent {
		t.Fatalf("write: %d %s, want 204 at quorum", rec.Code, rec.Body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	a.CloseContext(ctx)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("CloseContext took %v with a 100ms context; the hung push held it", d)
	}
}

// A write that reaches the router after the gateway started closing — a slow
// handler can outlive the HTTP server's shutdown — is refused with 503, not
// pushed by a router that is no longer waiting for its pushes.
func TestGatewayRefusesWritesOnceClosing(t *testing.T) {
	acks := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	cfg := testConfig(t, config.TargetGateway)
	cfg.IngesterURLs = []string{acks(), acks(), acks()}
	cfg.QuerierURL = "http://127.0.0.1:1"
	cfg.ReplicationFactor = 3
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	a.CloseContext(context.Background())
	body := `{"metrics":[{"name":"m","labels":{"job":"x"},"timestamp_ms":1,"value":2}]}`
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("write after close: %d %s, want 503", rec.Code, rec.Body)
	}
}
