package main

import (
	"bytes"
	"encoding/json"
	"io"
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
		if got := decoded["component"]; got != wantComp {
			t.Errorf("line %q: component = %v, want %q", msg, got, wantComp)
		}
	}
	for msg := range wantComponent {
		if !found[msg] {
			t.Errorf("expected a %q log line, found none in:\n%s", msg, buf.String())
		}
	}
}

// TestBuildServerExposesEveryPlottedStoreGauge scrapes /metrics from the server
// buildServer actually wires, and requires every gauge the self-observability
// dashboard plots from a store buildServer wires in: the block store (block
// count and bytes, and active series via its cardinality), both WALs, and the
// log store. The API fixture registers a single fake WAL and the Compose smoke
// test accepts any obs_wal_bytes series, so before this, removing any of those
// sources from buildServer blanked a panel with every test still green.
//
// The list was derived from the dashboard's own obs_* references, keeping the
// gauges and dropping the handler- and compactor-driven counters and
// histograms, which are covered elsewhere. An earlier version of this test
// claimed "every storage gauge" and omitted both block gauges, so production
// Storage: nil passed it; if the dashboard gains a store-backed gauge, add it
// here.
//
// Presence is the assertion, not just registration: a collector that fails to
// read its directory omits its gauges (a gap, by design), so a missing series
// here means either the source is not wired in or its read failed. On a fresh
// data directory buildServer creates all three directories, so neither should
// happen.
func TestBuildServerExposesEveryPlottedStoreGauge(t *testing.T) {
	cfg := &config.Config{
		HTTPAddr:                ":0",
		DataDir:                 t.TempDir(),
		LogLevel:                "info",
		WALSegmentMaxBytes:      1 << 20,
		WALSyncEveryN:           1,
		LogsFlushThresholdBytes: 1 << 20,
	}
	sc, err := buildServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(func() {
		_ = sc.LogStore.Close()
		_ = sc.BlockStore.Close()
	})

	rec := httptest.NewRecorder()
	sc.Server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, series := range []string{
		`obs_blocks_total`,
		`obs_blocks_bytes`,
		`obs_active_series`,
		`obs_wal_bytes{wal="metrics"}`,
		`obs_wal_bytes{wal="logs"}`,
		`obs_wal_segments{wal="metrics"}`,
		`obs_wal_segments{wal="logs"}`,
		`obs_log_streams_total`,
		`obs_log_chunks_total`,
		`obs_log_chunk_bytes`,
	} {
		if !seriesPresent(body, series) {
			t.Errorf("production /metrics has no %s sample; the dashboard panel plotting it would be empty", series)
		}
	}
}

// seriesPresent reports whether the exposition text has a sample line for
// series, which is either a bare metric name or name{labels} written exactly as
// the text format prints it. Comment lines are skipped, so a # HELP or # TYPE
// line for the name does not count as a sample.
func seriesPresent(body, series string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, series+" ") {
			return true
		}
	}
	return false
}
