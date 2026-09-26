package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
