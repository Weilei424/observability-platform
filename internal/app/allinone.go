package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// AllInOne is the single-process assembly: every component in one process
// with in-process calls, the on-disk layout and single maintenance loop it has
// always had. Its fields are the handles cmd/server's tests drive.
type AllInOne struct {
	Server      *api.Server
	Store       *metrics.WALStore
	BlockStore  *metrics.BlockStore
	WAL         *wal.WAL
	LogStore    *logs.Store
	Maintenance *observability.Metrics
	Compactor   *compactor.Compactor
}

// BuildAllInOne creates the data directory, opens the block store, replays the
// metrics WAL from the last checkpoint, opens the WAL for new writes, opens the
// logs store, then constructs the query engines, registry, api.Deps, and the
// maintenance loop. Every failure is logged here under its own component
// before it is returned.
//
// log becomes api.Deps.Logger unchanged and must stay component-free (see the
// doc comment on that field), so every line logged here goes through a
// derived, component-stamped logger instead: main for generic startup, and the
// wal and logs names those subsystems already use for their runtime lines.
func BuildAllInOne(cfg *config.Config, log *slog.Logger) (*AllInOne, error) {
	mainLog := observability.Component(log, "main")
	walLog := observability.Component(log, "wal")
	logsLog := observability.Component(log, "logs")

	// Durably create the data directory so its own directory entry survives a
	// power loss on first startup — a plain MkdirAll leaves the entry only in the
	// OS cache, and the WAL helpers below stop walking at the (now-existing) data
	// dir and never fsync its parent.
	if err := fsutil.MkdirAllSync(cfg.DataDir); err != nil {
		mainLog.Error("failed to create data directory", slog.String("data_dir", cfg.DataDir), slog.String("error", err.Error()))
		return nil, err
	}

	walDir := filepath.Join(cfg.DataDir, "metrics", "wal")

	blockStore, err := metrics.NewBlockStore(cfg.DataDir)
	if err != nil {
		mainLog.Error("failed to open block store", slog.String("error", err.Error()))
		return nil, err
	}

	checkpoint := metrics.ReadCheckpoint(cfg.DataDir)
	walLog.Info("WAL checkpoint", slog.Int("after_segment", checkpoint))
	restored, err := replayMetricsWAL(walLog, walDir, checkpoint, blockStore.Append)
	if err != nil {
		walLog.Error("WAL replay failed", slog.String("error", err.Error()))
		return nil, err
	}
	walLog.Info("WAL replay complete", slog.Int("samples_restored", restored))
	blockStore.MemStore().SetHeadFence(checkpoint + 1)

	w, err := wal.Open(walDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN)
	if err != nil {
		walLog.Error("failed to open WAL", slog.String("wal_dir", walDir), slog.String("error", err.Error()))
		return nil, err
	}

	logsDir := filepath.Join(cfg.DataDir, "logs")
	logsWALDir := filepath.Join(logsDir, "wal")
	logStore, err := logs.NewStore(
		logsWALDir,
		filepath.Join(logsDir, "chunks"),
		filepath.Join(logsDir, "index"),
		cfg.WALSegmentMaxBytes,
		cfg.WALSyncEveryN,
		cfg.LogsFlushThresholdBytes,
	)
	if err != nil {
		logsLog.Error("failed to open logs store", slog.String("logs_dir", logsDir), slog.String("error", err.Error()))
		return nil, err
	}
	logsLog.Info("logs store ready", slog.String("logs_dir", logsDir))

	store := metrics.NewWALStore(w, blockStore, cfg.DataDir)
	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Cardinality: blockStore,
		Storage:     blockStore,
		WALs: []observability.WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return wal.DirStats(walDir) }},
			{Name: "logs", Stats: func() (int64, int, error) { return wal.DirStats(logsWALDir) }},
		},
		Logs: logStore,
		// The plain logger, never a component-stamped one: each collector adds
		// its own component (see RegistryOptions.Logger).
		Logger: log,
	})
	srv := api.New(api.Deps{
		Config:      cfg,
		Logger:      log,
		Ingester:    store,
		Engine:      metrics.NewQueryEngine(blockStore),
		Registry:    reg,
		LogIngester: logStore,
		LogQuery:    logs.NewQueryEngine(logStore),
		HTTP:        inst.HTTP,
		Ingest:      inst.Ingest,
	})
	comp := compactor.New(store, blockStore, store, time.Now, maintenanceConfig(cfg),
		inst.Maintenance, observability.Component(log, "compactor"))

	return &AllInOne{
		Server:      srv,
		Store:       store,
		BlockStore:  blockStore,
		WAL:         w,
		LogStore:    logStore,
		Maintenance: inst.Maintenance,
		Compactor:   comp,
	}, nil
}

// App wraps the assembly in the generic process shape: its one maintenance
// loop, and its closers in the shutdown order main has always used — the
// metrics WAL, then the logs store, then the block store.
func (a *AllInOne) App(log *slog.Logger) *App {
	return &App{
		Target:  config.TargetAllInOne,
		Handler: a.Server,
		loops:   []func(context.Context){a.Compactor.Run},
		closers: []closer{
			{component: "wal", msg: "wal close error", close: a.WAL.Close},
			// Close joins the flush error and the WAL-close error, so this line can
			// carry either or both. Both are durability failures worth naming as
			// such: a failed flush leaves the head only in the WAL, and a failed WAL
			// close means its tail was never fsynced.
			{component: "logs", msg: "logs store close error: buffered logs may not have reached disk", close: a.LogStore.Close},
			{component: "main", msg: "block store close error", close: a.BlockStore.Close},
		},
		log: log,
	}
}
