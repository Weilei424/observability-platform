package app_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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

// gatewayWith builds a gateway over the given ingester URLs at RF 3, logging
// JSON at debug into buf.
func gatewayWith(t *testing.T, buf *bytes.Buffer, timeout time.Duration, urls ...string) *app.App {
	t.Helper()
	cfg := testConfig(t, config.TargetGateway)
	cfg.IngesterURLs = urls
	cfg.QuerierURL = "http://127.0.0.1:1"
	cfg.ReplicationFactor = 3
	cfg.IngesterTimeout = timeout
	a, err := app.Build(cfg, slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return a
}

func answering(t *testing.T, code int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
	t.Cleanup(srv.Close)
	return srv.URL
}

func hanging(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv.URL
}

// A degraded batch -- quorum met, one replica failed -- logs at debug, with
// the failing ingester and the request's ID (spec section 5.4).
func TestGatewayLogsADegradedBatchAtDebug(t *testing.T) {
	var buf bytes.Buffer
	down := answering(t, http.StatusServiceUnavailable)
	a := gatewayWith(t, &buf, 2*time.Second, answering(t, http.StatusNoContent), answering(t, http.StatusNoContent), down)
	body := `{"metrics":[{"name":"m","labels":{"job":"x"},"timestamp_ms":1,"value":2}]}`
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != http.StatusNoContent {
		t.Fatalf("write = %d %s, want 204 at quorum", rec.Code, rec.Body)
	}
	a.CloseContext(context.Background()) // waits for the batch's report
	logged := buf.String()
	for _, want := range []string{`"level":"DEBUG"`, `"msg":"write quorum met with failed replicas"`, strings.TrimPrefix(down, "http://"), `"request_id"`} {
		if !strings.Contains(logged, want) {
			t.Errorf("degraded batch log lacks %s:\n%s", want, logged)
		}
	}
}

// A batch that fails after its client left is logged by the gateway, since
// no handler saw its QuorumError.
func TestGatewayLogsAFailureAfterTheClientLeft(t *testing.T) {
	var buf bytes.Buffer
	a := gatewayWith(t, &buf, 300*time.Millisecond,
		answering(t, http.StatusNoContent), hanging(t), hanging(t))
	body := `{"metrics":[{"name":"m","labels":{"job":"x"},"timestamp_ms":1,"value":2}]}`
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", strings.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()
	a.Handler.ServeHTTP(rec, req) // the client leaves before the two hung replicas time out
	a.CloseContext(context.Background())
	logged := buf.String()
	for _, want := range []string{`"level":"WARN"`, `"msg":"write quorum not met after the client left"`, `"ingesters"`} {
		if !strings.Contains(logged, want) {
			t.Errorf("orphaned failure log lacks %s:\n%s", want, logged)
		}
	}
}
