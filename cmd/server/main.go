package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(1)
	}
	// Route package-level slog calls (e.g. WAL replay recovery warnings) through
	// the structured JSON application logger instead of the stdlib text default.
	slog.SetDefault(log)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		log.Error("failed to create data directory", slog.String("data_dir", cfg.DataDir), slog.String("error", err.Error()))
		os.Exit(1)
	}

	walDir := filepath.Join(cfg.DataDir, "metrics", "wal")

	blockStore, err := metrics.NewBlockStore(cfg.DataDir)
	if err != nil {
		log.Error("failed to open block store", slog.String("error", err.Error()))
		os.Exit(1)
	}

	checkpoint := metrics.ReadCheckpoint(cfg.DataDir)
	log.Info("WAL checkpoint", slog.Int("after_segment", checkpoint))

	var replayCount int
	if err := wal.ReplayFrom(walDir, checkpoint, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		labels, err := metrics.NewLabels(lm)
		if err != nil {
			log.Warn("WAL replay: skipping record with invalid labels", slog.String("error", err.Error()))
			return
		}
		if err := blockStore.Append(labels, tsMs, value); err != nil {
			log.Warn("WAL replay: failed to append sample", slog.String("error", err.Error()))
			return
		}
		replayCount++
	}); err != nil {
		log.Error("WAL replay failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("WAL replay complete", slog.Int("samples_restored", replayCount))

	blockStore.MemStore().SetHeadFence(checkpoint + 1)

	w, err := wal.Open(walDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN)
	if err != nil {
		log.Error("failed to open WAL", slog.String("wal_dir", walDir), slog.String("error", err.Error()))
		os.Exit(1)
	}

	logsWALDir := filepath.Join(cfg.DataDir, "logs", "wal")

	logStore := logs.NewMemoryStore()
	var logReplayCount int
	if err := logwal.Replay(logsWALDir, func(pairs []logwal.LabelPair, tsNs int64, line string) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		sl, err := logs.NewStreamLabels(lm)
		if err != nil {
			log.Warn("logs WAL replay: skipping record with invalid labels", slog.String("error", err.Error()))
			return
		}
		if err := logStore.Append(sl, tsNs, line); err != nil {
			log.Warn("logs WAL replay: failed to append entry", slog.String("error", err.Error()))
			return
		}
		logReplayCount++
	}); err != nil {
		log.Error("logs WAL replay failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("logs WAL replay complete", slog.Int("entries_restored", logReplayCount))

	lw, err := logwal.Open(logsWALDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN)
	if err != nil {
		log.Error("failed to open logs WAL", slog.String("wal_dir", logsWALDir), slog.String("error", err.Error()))
		os.Exit(1)
	}
	logIngester := logs.NewWALStore(lw, logStore)

	store := metrics.NewWALStore(w, blockStore, cfg.DataDir)
	engine := metrics.NewQueryEngine(blockStore)
	reg, mx := observability.NewRegistry(blockStore, blockStore)
	srv := api.New(cfg, log, store, engine, reg, logIngester)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	comp := compactor.New(store, blockStore, store, time.Now, compactor.Config{
		MaintenanceInterval: cfg.MaintenanceInterval,
		FlushInterval:       cfg.FlushInterval,
		FlushSealedChunks:   cfg.FlushSealedChunks,
		FlushWALBytes:       cfg.FlushWALBytes,
		Ranges:              compactor.Ranges(cfg.CompactionBaseRange.Milliseconds(), int64(cfg.CompactionMultiplier), cfg.CompactionLevels),
		Retention:           cfg.Retention,
	}, mx, log)

	compDone := make(chan struct{})
	go func() {
		comp.Run(ctx)
		close(compDone)
	}()

	// Bind synchronously so a bind failure (e.g. address already in use) is fatal
	// immediately, rather than surfacing later during graceful shutdown. With
	// port 0 the OS assigns a free ephemeral port; ln.Addr() reports the actual
	// bound address, which we optionally publish to OBS_ADDR_FILE so a supervising
	// process (e.g. the k6 bench runner) can discover it race-free.
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		log.Error("failed to bind HTTP address", slog.String("addr", cfg.HTTPAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	boundAddr := ln.Addr().String()
	if addrFile := os.Getenv("OBS_ADDR_FILE"); addrFile != "" {
		if err := os.WriteFile(addrFile, []byte(boundAddr+"\n"), 0o644); err != nil {
			log.Error("failed to write OBS_ADDR_FILE", slog.String("path", addrFile), slog.String("error", err.Error()))
			_ = ln.Close()
			os.Exit(1)
		}
	}

	httpSrv := &http.Server{Handler: srv}
	go func() {
		log.Info("starting server", slog.String("addr", boundAddr), slog.String("data_dir", cfg.DataDir))
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", slog.String("error", err.Error()))
			stop() // unblock shutdown below
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", slog.String("error", err.Error()))
	}

	<-compDone // compactor performs its final flush on ctx cancellation

	if err := w.Close(); err != nil {
		log.Error("wal close error", slog.String("error", err.Error()))
	}
	if err := lw.Close(); err != nil {
		log.Error("logs wal close error", slog.String("error", err.Error()))
	}
	if err := blockStore.Close(); err != nil {
		log.Error("block store close error", slog.String("error", err.Error()))
	}
	log.Info("shutdown complete")
}
