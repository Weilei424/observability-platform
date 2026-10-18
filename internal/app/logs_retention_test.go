package app_test

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

func oldStream(t *testing.T) (logs.StreamLabels, logs.StreamData) {
	t.Helper()
	l, err := logs.NewStreamLabels(map[string]string{"service": "old"})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Now().Add(-2 * time.Hour).UnixNano()
	return l, logs.StreamData{Labels: l, Entries: []logs.LogEntry{{TimestampNs: ts, Line: "old"}}}
}

// All-in-one's maintenance pass deletes its own expired log chunks.
func TestAllInOneCompactorAppliesLogsRetention(t *testing.T) {
	cfg := testConfig(t, config.TargetAllInOne)
	cfg.Retention = time.Hour
	built, err := app.BuildAllInOne(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a := built.App(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(a.Close)

	l, sd := oldStream(t)
	if err := built.LogStore.Append(l, sd.Entries[0].TimestampNs, sd.Entries[0].Line); err != nil {
		t.Fatal(err)
	}
	if err := built.LogStore.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := built.LogStore.StreamEntries(context.Background(), logs.StreamIDOf(l), math.MinInt64, math.MaxInt64); len(got) != 1 {
		t.Fatalf("before retention: %d entries, want 1", len(got))
	}
	built.Compactor.RunOnce(context.Background())
	if got, err := built.LogStore.StreamEntries(context.Background(), logs.StreamIDOf(l), math.MinInt64, math.MaxInt64); err != nil || len(got) != 0 {
		t.Fatalf("after retention: %v, %v; want nothing", got, err)
	}
	body := do(a.Handler, "GET", "/metrics", "").Body.String()
	if !strings.Contains(body, "obs_logs_retention_deleted_chunks_total 1") {
		t.Fatalf("/metrics lacks obs_logs_retention_deleted_chunks_total 1")
	}
}

// The compactor target drives every store's logs retention over RPC.
func TestCompactorTargetAppliesLogsRetentionOnTheStore(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	cs, err := logs.OpenChunkStore(filepath.Join(dir, "logs", "chunks"), filepath.Join(dir, "logs", "index"))
	if err != nil {
		t.Fatal(err)
	}
	l, sd := oldStream(t)
	if err := cs.IngestStreams(context.Background(), []logs.StreamData{sd}); err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { rpc.MountStore(r, bs, cs, &rpc.FlushBarrier{}) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	cfg := testConfig(t, config.TargetCompactor)
	cfg.StoreURL, cfg.StoreURLs = srv.URL, []string{srv.URL}
	cfg.Retention = time.Hour
	cfg.MaintenanceInterval = 20 * time.Millisecond
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := cs.StreamLabelSet(logs.StreamIDOf(l)); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the compactor target never deleted the store's expired log chunk")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
