package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/config"
)

// TestBuildServerKeepsLoggerComponentFree guards the actual wiring boundary
// finding 1 slipped through: internal/api/middleware.TestHandlerComponentSurvivesAsTheOnlyComponentKey
// proves the middleware itself never adds "component", but the real bug was
// what main.go handed in as api.Deps.Logger (a logger already stamped with
// component=api, via observability.Component(log, "api")). That test still
// passes no matter what main.go does, because it builds its own plain logger
// and calls the middleware directly.
//
// buildServer is the exact function main() calls to construct api.Deps, so
// this test drives a real request through that same production wiring and
// checks the seam the bug actually broke: a handler's own
// observability.Component(observability.FromContext(ctx), "<subsystem>") call
// must be the ONLY "component" key on its log line. If buildServer is ever
// changed to pass a pre-stamped logger as api.Deps.Logger again, this fails --
// see the doc comment on api.Deps.Logger in internal/api/server.go for why.
func TestBuildServerKeepsLoggerComponentFree(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := &config.Config{
		HTTPAddr:                ":0",
		DataDir:                 t.TempDir(),
		LogLevel:                "info",
		WALSegmentMaxBytes:      1 << 20,
		WALSyncEveryN:           1,
		LogsFlushThresholdBytes: 1 << 20,
	}

	sc, err := buildServer(cfg, log)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(func() {
		_ = sc.LogStore.Close()
		_ = sc.BlockStore.Close()
	})

	// Force the next metrics append to fail so handleIngestMetrics takes its
	// error-logging branch deterministically: internal/api/ingest.go logs via
	// observability.Component(observability.FromContext(r.Context()),
	// "metrics_ingest") only when s.ingester.Append returns an error. Closing
	// the WAL segment out from under it produces a plain "file already
	// closed" write error -- no corrupted fixtures needed.
	if err := sc.WAL.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reqBody, err := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "test_metric",
				"labels":       map[string]string{},
				"timestamp_ms": int64(1000),
				"value":        float64(1),
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	sc.Server.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d (body: %s)", rr.Code, http.StatusInternalServerError, rr.Body.String())
	}

	var handlerLine string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, `"msg":"ingester append failed"`) {
			handlerLine = line
			break
		}
	}
	if handlerLine == "" {
		t.Fatalf("no \"ingester append failed\" log line found in output:\n%s", buf.String())
	}

	// Checked on the raw JSON text, before any decode collapses duplicate keys
	// into a single map entry: decoding first would hide the exact failure
	// mode this test exists to catch.
	if n := strings.Count(handlerLine, `"component"`); n != 1 {
		t.Errorf("handler line carries %d component keys, want exactly 1: %s", n, handlerLine)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(handlerLine), &decoded); err != nil {
		t.Fatalf("handler line is not JSON: %q: %v", handlerLine, err)
	}
	if decoded["component"] != "metrics_ingest" {
		t.Errorf("component = %v, want %q (the handler's own subsystem, not \"api\")", decoded["component"], "metrics_ingest")
	}
}

// TestBuildServerStartupLogsCarryComponent guards F4: buildServer's own
// startup/replay log lines (WAL checkpoint, logs store readiness, ...) must
// carry a "component" name, same as request-scoped logging does. Before the
// fix these lines went through the plain, component-free logger handed to
// buildServer (which must stay component-free because it also becomes
// api.Deps.Logger) and so had no component at all.
func TestBuildServerStartupLogsCarryComponent(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := &config.Config{
		HTTPAddr:                ":0",
		DataDir:                 t.TempDir(),
		LogLevel:                "info",
		WALSegmentMaxBytes:      1 << 20,
		WALSyncEveryN:           1,
		LogsFlushThresholdBytes: 1 << 20,
	}

	sc, err := buildServer(cfg, log)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(func() {
		_ = sc.LogStore.Close()
		_ = sc.BlockStore.Close()
	})

	wantComponent := map[string]string{
		"WAL checkpoint":   "wal",
		"logs store ready": "logs",
	}
	found := make(map[string]bool, len(wantComponent))

	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("startup log line is not JSON: %q: %v", line, err)
		}
		msg, _ := decoded["msg"].(string)
		wantComp, ok := wantComponent[msg]
		if !ok {
			continue
		}
		found[msg] = true
		if got, _ := decoded["component"]; got != wantComp {
			t.Errorf("line %q: component = %v, want %q", msg, got, wantComp)
		}
	}
	for msg := range wantComponent {
		if !found[msg] {
			t.Errorf("expected a %q log line, found none in:\n%s", msg, buf.String())
		}
	}
}
