package app

import (
	"log/slog"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
)

// buildStore assembles the store: blocks and log chunks, the internal reads
// over them, flush-in, the block maintenance the compactor drives, and the
// handoff that copies its data to the remaining stores before it is removed.
// It serves no public route and calls a peer only during a handoff.
func buildStore(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	logsLog := observability.Component(log, "logs")
	if err := fsutil.MkdirAllSync(cfg.DataDir); err != nil {
		mainLog.Error("failed to create data directory", slog.String("data_dir", cfg.DataDir), slog.String("error", err.Error()))
		return nil, err
	}
	blocks, err := metrics.NewBlockStore(cfg.DataDir)
	if err != nil {
		mainLog.Error("failed to open block store", slog.String("error", err.Error()))
		return nil, err
	}
	logsDir := filepath.Join(cfg.DataDir, "logs")
	chunks, err := logs.OpenChunkStore(filepath.Join(logsDir, "chunks"), filepath.Join(logsDir, "index"))
	if err != nil {
		logsLog.Error("failed to open log chunk store", slog.String("logs_dir", logsDir), slog.String("error", err.Error()))
		_ = blocks.Close()
		return nil, err
	}
	logsLog.Info("log chunk store ready", slog.String("logs_dir", logsDir))

	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Storage: blocks,
		Logs:    chunks,
		Omit:    observability.AllGroups,
		Logger:  log,
	})
	// Closed by a handoff; flush-ins answer 503 from then until restart.
	barrier := &rpc.FlushBarrier{}
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesNone,
		Registry: reg,
		HTTP:     inst.HTTP,
		Internal: func(r chi.Router) {
			// Refuse a request meant for another store (rpc.MemberHeader).
			if cfg.StoreSelfURL != "" {
				r.Use(rpc.RequireMember(cfg.StoreSelfURL))
			}
			rpc.MountReads(r, blocks, logs.AsSource(chunks))
			rpc.MountStore(r, blocks, chunks, barrier)
			rpc.MountHandoff(r, blocks, chunks, barrier, cfg.StoreSelfURL, metrics.DefaultFlushTimeout)
		},
	})
	return &App{
		Target:  config.TargetStore,
		Handler: srv,
		closers: []closer{{component: "main", msg: "block store close error", close: blocks.Close}},
		log:     log,
	}, nil
}
