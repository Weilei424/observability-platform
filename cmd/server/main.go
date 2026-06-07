package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
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

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Error("failed to create data directory",
			slog.String("data_dir", cfg.DataDir),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	walDir := filepath.Join(cfg.DataDir, "metrics", "wal")

	// 1. Load persisted blocks and prepare temp directory.
	blockStore, err := metrics.NewBlockStore(cfg.DataDir)
	if err != nil {
		log.Error("failed to open block store", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 2. Read checkpoint — skip WAL segments already covered by blocks.
	checkpoint := metrics.ReadCheckpoint(cfg.DataDir)
	log.Info("WAL checkpoint", slog.Int("after_segment", checkpoint))

	// 3. Replay WAL segments after the checkpoint into the block store.
	// Replay BEFORE opening the WAL for writes (wal.Open creates a new segment
	// at maxIdx+1; replaying after would make the previous last segment non-final).
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

	// 4. Open WAL for new writes.
	w, err := wal.Open(walDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN)
	if err != nil {
		log.Error("failed to open WAL", slog.String("wal_dir", walDir), slog.String("error", err.Error()))
		os.Exit(1)
	}

	store := metrics.NewWALStore(w, blockStore, cfg.DataDir)
	engine := metrics.NewQueryEngine(blockStore)
	srv := api.New(cfg, log, store, engine)

	log.Info("starting server",
		slog.String("addr", cfg.HTTPAddr),
		slog.String("data_dir", cfg.DataDir),
	)

	if err := http.ListenAndServe(cfg.HTTPAddr, srv); err != nil {
		log.Error("server stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
