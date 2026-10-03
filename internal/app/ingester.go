package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// buildIngester assembles the ingester: the metrics WAL and head, the logs WAL
// and head, the two write routes, the internal head reads, and a flush-only
// maintenance loop. Both heads flush to the store at cfg.StoreURL; nothing at
// startup waits for it.
func buildIngester(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	walLog := observability.Component(log, "wal")
	logsLog := observability.Component(log, "logs")

	if err := fsutil.MkdirAllSync(cfg.DataDir); err != nil {
		mainLog.Error("failed to create data directory", slog.String("data_dir", cfg.DataDir), slog.String("error", err.Error()))
		return nil, err
	}
	store, err := rpc.NewClient("store", cfg.StoreURL)
	if err != nil {
		mainLog.Error("failed to build store client", slog.String("error", err.Error()))
		return nil, err
	}

	head, err := metrics.OpenHeadStore(cfg.DataDir, rpc.NewBlockSink(store), metrics.HeadStoreOptions{})
	if err != nil {
		mainLog.Error("failed to open metrics head", slog.String("error", err.Error()))
		return nil, err
	}
	walDir := filepath.Join(cfg.DataDir, "metrics", "wal")
	checkpoint := metrics.ReadCheckpoint(cfg.DataDir)
	walLog.Info("WAL checkpoint", slog.Int("after_segment", checkpoint))
	restored, err := replayMetricsWAL(walLog, walDir, checkpoint, head.AppendGen)
	if err != nil {
		walLog.Error("WAL replay failed", slog.String("error", err.Error()))
		return nil, err
	}
	walLog.Info("WAL replay complete", slog.Int("samples_restored", restored))
	head.SetHeadFence(checkpoint + 1)
	w, err := wal.Open(walDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN)
	if err != nil {
		walLog.Error("failed to open WAL", slog.String("wal_dir", walDir), slog.String("error", err.Error()))
		return nil, err
	}
	writes := metrics.NewWALStore(w, head, cfg.DataDir)

	logsWALDir := filepath.Join(cfg.DataDir, "logs", "wal")
	logHead, err := logs.OpenHead(logsWALDir, cfg.WALSegmentMaxBytes, cfg.WALSyncEveryN, cfg.LogsFlushThresholdBytes,
		rpc.NewChunkSink(store), logs.HeadOptions{
			TolerateFlushErrors: true,
			FlushTimeout:        logs.DefaultLogFlushTimeout,
			BatchBytes:          logs.DefaultLogFlushBatchBytes,
		})
	if err != nil {
		logsLog.Error("failed to open logs head", slog.String("wal_dir", logsWALDir), slog.String("error", err.Error()))
		_ = w.Close()
		return nil, err
	}
	logsLog.Info("logs head ready", slog.String("wal_dir", logsWALDir))

	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Cardinality: head,
		WALs: []observability.WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return wal.DirStats(walDir) }},
			{Name: "logs", Stats: func() (int64, int, error) { return wal.DirStats(logsWALDir) }},
		},
		Omit:   observability.CompactionGroup,
		Logger: log,
	})

	// The ingester's logs head tolerates flush errors so a push never fails just
	// because the store is unreachable -- the entry is already durable in the
	// WAL. That makes this hook the only place a failed flush is ever reported;
	// a successful flush needs no line. lastFlushErr records exactly the error
	// this hook last logged, so the logs closer below (which fires on the final
	// flush Close() performs on its way out) can tell "the same failure the hook
	// just reported" apart from a genuine WAL-close error and never log the
	// former a second time.
	flushLog := observability.Component(log, "flush")
	var lastFlushErr error
	logHead.SetFlushHook(func(err error) {
		inst.LogFlush.Observe(err)
		lastFlushErr = err
		if err != nil {
			flushLog.Error("logs flush failed", slog.String("error", err.Error()))
		}
	})
	srv := api.New(api.Deps{
		Config:      cfg,
		Logger:      log,
		Routes:      api.RoutesWrite,
		Ingester:    writes,
		LogIngester: logHead,
		Registry:    reg,
		HTTP:        inst.HTTP,
		Ingest:      inst.Ingest,
		Internal:    func(r chi.Router) { rpc.MountReads(r, head, logs.AsSource(logHead)) },
	})
	flush := compactor.New(writes, nil, writes, time.Now, maintenanceConfig(cfg),
		inst.Maintenance, flushLog)

	return &App{
		Target:  config.TargetIngester,
		Handler: srv,
		loops:   []func(ctx context.Context){flush.Run},
		closers: []closer{
			{component: "wal", msg: "wal close error", close: w.Close},
			// Close's final flush runs the hook above synchronously before Close
			// returns, so if the only error Close reports is the one the hook just
			// logged (no additional WAL-close failure joined onto it), swallow it
			// here rather than logging the same failure a second time under a
			// different message. errors.Join's Error() concatenates each non-nil
			// error's own message with "\n", so a join of exactly one error reads
			// back identical to that error's own message; a second, distinct
			// WAL-close error appended changes the text and is still logged.
			{component: "logs", msg: "logs head close error: buffered logs may not have reached the store", close: func() error {
				err := logHead.Close()
				if err != nil && lastFlushErr != nil && err.Error() == lastFlushErr.Error() {
					return nil
				}
				return err
			}},
		},
		log: log,
	}, nil
}
