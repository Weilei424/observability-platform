package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
)

func testConfig(t *testing.T, target config.Target) *config.Config {
	t.Helper()
	return &config.Config{
		Target:                  target,
		HTTPAddr:                ":0",
		DataDir:                 t.TempDir(),
		LogLevel:                "info",
		WALSegmentMaxBytes:      1 << 20,
		WALSyncEveryN:           1,
		LogsFlushThresholdBytes: 1 << 20,
		MaintenanceInterval:     time.Hour,
		FlushInterval:           time.Hour,
		CompactionBaseRange:     2 * time.Hour,
		CompactionMultiplier:    4,
		CompactionLevels:        3,
	}
}

func TestAllInOneAppServesRunsAndClosesCleanly(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	a, err := app.Build(testConfig(t, config.TargetAllInOne), log)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if a.Target != config.TargetAllInOne {
		t.Errorf("Target = %q", a.Target)
	}

	rec := httptest.NewRecorder()
	a.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", rec.Code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
	a.Close()
	if strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Fatalf("a clean shutdown logged an error:\n%s", logs.String())
	}
}

// countErrorLines counts JSON log lines carrying level=ERROR, so a test can
// assert a failure was logged exactly once rather than merely "at least
// once" -- the distinction a doubled log line (the specific cause, then a
// second, generic one) would otherwise pass unnoticed.
func countErrorLines(s string) int {
	var n int
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if strings.Contains(line, `"level":"ERROR"`) {
			n++
		}
	}
	return n
}

// TestBuildUnavailableTargetLogsOnceAndLeavesDataDirEmpty covers a target
// Build cannot construct yet (every non-all-in-one target, until a later
// task fills them in). Build must fail without touching storage -- nothing
// in cfg.DataDir, since it never reaches BuildAllInOne's data-dir/WAL/block-
// store bring-up -- and must log the failure exactly once, from the site
// that actually knows the cause (Build's own default branch), rather than
// leaving it to a caller that would otherwise add a second, generic line on
// top.
func TestBuildUnavailableTargetLogsOnceAndLeavesDataDirEmpty(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := testConfig(t, config.TargetGateway)

	_, err := app.Build(cfg, log)
	if err == nil {
		t.Fatal("Build: want an error for a target that is not available yet, got nil")
	}
	if !strings.Contains(err.Error(), string(config.TargetGateway)) {
		t.Errorf("error = %q, want it to name the target %q", err.Error(), config.TargetGateway)
	}

	entries, err := os.ReadDir(cfg.DataDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", cfg.DataDir, err)
	}
	if len(entries) != 0 {
		t.Errorf("data dir has %d entries, want 0 (Build must not touch storage for a target it cannot build yet): %v", len(entries), entries)
	}

	if n := countErrorLines(logs.String()); n != 1 {
		t.Errorf("logged %d ERROR lines, want exactly 1:\n%s", n, logs.String())
	}
}

// TestAllInOneAppFlushesOnShutdownAndClosersRunOnClose drives the full
// lifecycle through the same handles BuildAllInOne hands the ingester and
// cmd/server's tests: ingest real samples through Handler, run the
// maintenance loop to a cancellation, and confirm both halves of shutdown
// actually happened -- a block reached disk (the loop was registered and ran
// its final flush) and the WAL is genuinely closed (the closers ran, in
// order, on Close).
func TestAllInOneAppFlushesOnShutdownAndClosersRunOnClose(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := testConfig(t, config.TargetAllInOne)

	built, err := app.BuildAllInOne(cfg, log)
	if err != nil {
		t.Fatalf("BuildAllInOne: %v", err)
	}
	a := built.App(log)

	// A chunk seals at a 2-hour sample-timestamp span or 120 samples,
	// whichever comes first (internal/storage/chunk: maxSpanMs/maxSamples).
	// FlushBlock is a documented no-op with no sealed chunk, so two samples
	// exactly that span apart -- sealing the chunk on the second append --
	// are what give the maintenance loop's final flush below something to
	// write; one sample alone would leave metrics/blocks empty no matter
	// whether the loop ran at all.
	reqBody, err := json.Marshal(map[string]any{
		"metrics": []any{
			map[string]any{"name": "test_metric", "labels": map[string]string{}, "timestamp_ms": int64(0), "value": float64(1)},
			map[string]any{"name": "test_metric", "labels": map[string]string{}, "timestamp_ms": int64(7_200_000), "value": float64(2)},
		},
	})
	if err != nil {
		t.Fatalf("marshal ingest body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("ingest status = %d, want %d (body: %s)", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	blockDir := filepath.Join(cfg.DataDir, "metrics", "blocks")
	blockEntries, err := os.ReadDir(blockDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", blockDir, err)
	}
	if len(blockEntries) == 0 {
		t.Fatal("no block was flushed to metrics/blocks: the maintenance loop's final flush did not run")
	}

	a.Close()
	if strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Fatalf("a clean shutdown logged an error:\n%s", logs.String())
	}

	// The closers ran, in order: WAL, then logs store, then block store (see
	// AllInOne.App). wal.Open would happily reopen the directory regardless
	// of whether the prior handle was closed -- it just creates the next
	// segment index, with no exclusive lock of its own -- so it would pass
	// whether or not Close had run and prove nothing. Sync on the already-
	// closed handle is the precise check: cmd/server's own
	// TestBuildServerKeepsLoggerComponentFree relies on the same
	// already-closed-file error to force a deterministic append failure, so
	// this reuses a technique this codebase already trusts elsewhere.
	if err := built.WAL.Sync(); err == nil {
		t.Error("WAL.Sync succeeded after Close: the wal closer did not run (or the WAL was left open)")
	}
}

// TestAppRunWithNoLoopsBlocksUntilCancelled covers a target with nothing to
// run in the background (a future gateway or querier): Run's doc comment
// promises it blocks until ctx is done regardless of how many loops are
// registered, and with zero loops the wg alone (Add is never called) would
// otherwise let Run return immediately. App's zero value has no loops and no
// closers, and its exported fields need no keyed literal to reach that
// state, so this needs nothing from BuildAllInOne.
func TestAppRunWithNoLoopsBlocksUntilCancelled(t *testing.T) {
	var a app.App

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()

	select {
	case <-done:
		t.Fatal("Run returned before its context was cancelled")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}
