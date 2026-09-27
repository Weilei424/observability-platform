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

// unreachable is a peer URL nothing listens on: port 1 is refused at once.
const unreachable = "http://127.0.0.1:1"

func withPeers(cfg *config.Config) *config.Config {
	switch cfg.Target {
	case config.TargetGateway:
		cfg.IngesterURL, cfg.QuerierURL = unreachable, unreachable
	case config.TargetIngester, config.TargetCompactor:
		cfg.StoreURL = unreachable
	case config.TargetQuerier:
		cfg.IngesterURL, cfg.StoreURL = unreachable, unreachable
	}
	return cfg
}

func do(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
	return rec
}

const oneSample = `{"metrics":[{"name":"alone","labels":{},"timestamp_ms":1000,"value":1}]}`
const oneLine = `{"streams":[{"stream":{"service":"alone"},"values":[["1000000000","hello"]]}]}`

// Every target starts with every peer down: no startup ordering, and readiness
// reports only the process's own state.
func TestEveryTargetStartsAloneAndIsReady(t *testing.T) {
	for _, target := range config.Targets {
		t.Run(string(target), func(t *testing.T) {
			a, err := app.Build(withPeers(testConfig(t, target)), slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatalf("Build(%s) with peers down: %v", target, err)
			}
			for _, path := range []string{"/healthz", "/readyz"} {
				if rec := do(a.Handler, http.MethodGet, path, ""); rec.Code != http.StatusOK {
					t.Errorf("%s = %d, want 200 with every peer down", path, rec.Code)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			a.Run(ctx) // every loop starts and stops cleanly with its peers down
			a.Close()
		})
	}
}

// What each target does with its peers down: writes to the ingester need no
// peer; everything that needs one answers 503.
func TestTargetsWithPeersDown(t *testing.T) {
	build := func(target config.Target) http.Handler {
		a, err := app.Build(withPeers(testConfig(t, target)), slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(a.Close)
		return a.Handler
	}

	ing := build(config.TargetIngester)
	if rec := do(ing, http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusNoContent {
		t.Errorf("ingester ingest with the store down = %d, want 204: the WAL has it", rec.Code)
	}
	if rec := do(ing, http.MethodPost, "/loki/api/v1/push", oneLine); rec.Code != http.StatusNoContent {
		t.Errorf("ingester push with the store down = %d, want 204", rec.Code)
	}
	if rec := do(ing, http.MethodGet, "/api/v1/query?query=alone", ""); rec.Code != http.StatusNotFound {
		t.Errorf("ingester serves queries (%d); only the querier does", rec.Code)
	}

	q := build(config.TargetQuerier)
	if rec := do(q, http.MethodGet, "/api/v1/query?query=alone&time=1", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("querier with peers down = %d, want 503", rec.Code)
	}

	gw := build(config.TargetGateway)
	if rec := do(gw, http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("gateway with the ingester down = %d, want 503", rec.Code)
	}

	st := build(config.TargetStore)
	if rec := do(st, http.MethodPost, "/internal/v1/metrics/select", `{"matchers":[],"min_ms":0,"max_ms":1}`); rec.Code != http.StatusOK {
		t.Errorf("store select = %d, want 200", rec.Code)
	}
	if rec := do(st, http.MethodGet, "/api/v1/query?query=x", ""); rec.Code != http.StatusNotFound {
		t.Errorf("store serves public queries (%d)", rec.Code)
	}
}

// invalidPeerURL fails net/url.Parse: a URL cannot open with a bare ":", so
// this is rpc.NewClient's (and url.Parse's, in the gateway) one failure mode.
const invalidPeerURL = "://bad"

// TestBuildLogsOnceWhenAPeerURLFailsToParse covers every builder that turns a
// peer URL setting into a client at startup. main no longer logs a Build
// failure itself (see app.go's Build doc comment and cmd/server/main.go), so
// each of these builders must log its own failure, exactly once, before
// returning -- otherwise a malformed peer URL would exit 1 with nothing in
// the log. Config is built directly here, bypassing config.Load's own peer-
// URL validation (validatePeerURL), so the malformed string actually reaches
// the builder instead of being rejected first.
func TestBuildLogsOnceWhenAPeerURLFailsToParse(t *testing.T) {
	for _, tc := range []struct {
		target config.Target
		set    func(cfg *config.Config)
	}{
		{config.TargetIngester, func(cfg *config.Config) { cfg.StoreURL = invalidPeerURL }},
		{config.TargetCompactor, func(cfg *config.Config) { cfg.StoreURL = invalidPeerURL }},
		{config.TargetQuerier, func(cfg *config.Config) { cfg.IngesterURL = invalidPeerURL }},
		{config.TargetGateway, func(cfg *config.Config) { cfg.IngesterURL = invalidPeerURL }},
	} {
		t.Run(string(tc.target), func(t *testing.T) {
			var logs bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&logs, nil))
			cfg := testConfig(t, tc.target)
			tc.set(cfg)

			_, err := app.Build(cfg, log)
			if err == nil {
				t.Fatalf("Build(%s): want an error for a peer URL that fails to parse, got nil", tc.target)
			}
			if n := countErrorLines(logs.String()); n != 1 {
				t.Errorf("Build(%s) logged %d ERROR lines, want exactly 1:\n%s", tc.target, n, logs.String())
			}
		})
	}
}
