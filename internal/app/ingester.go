package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/drain"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// ingesterDrainTimeout bounds one drain of the whole head, on the drain route
// and at shutdown. At shutdown it is also inside the process's shutdown budget
// (cmd/server), which keeps a stop within the 60s grace period Compose and the
// Helm chart give an ingester.
const ingesterDrainTimeout = 40 * time.Second

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
	// a successful flush needs no line. The shutdown drain below relies on it
	// too: it leaves a failed logs flush to this hook rather than log it twice.
	flushLog := observability.Component(log, "flush")
	logHead.SetFlushHook(func(err error) {
		inst.LogFlush.Observe(err)
		if err != nil {
			flushLog.Error("logs flush failed", slog.String("error", err.Error()))
		}
	})
	// gate is the write barrier: every write, public or internal, passes it,
	// and a drain closes it for good before flushing, so nothing can land in a
	// head the drain already emptied. A 200 from the drain route therefore
	// covers every write this ingester ever acknowledged.
	gate := drain.NewGate()
	// drainHeads closes the gate, waiting for admitted writes to finish, then
	// flushes the whole head — metrics, open chunks included, then logs — and
	// succeeds only if both heads are empty afterwards. Everything runs within
	// ctx. The drain route runs it before an ingester is removed; shutdown
	// runs it last.
	drainHeads := func(ctx context.Context) error {
		if err := gate.Close(ctx); err != nil {
			return fmt.Errorf("waiting for in-flight writes: %w", err)
		}
		if err := writes.Drain(ctx); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		if err := logHead.FlushContext(ctx); err != nil {
			return fmt.Errorf("logs: %w", err)
		}
		if !logHead.Empty() {
			return errors.New("logs: head not empty after the drain")
		}
		return nil
	}
	srv := api.New(api.Deps{
		Config:      cfg,
		Logger:      log,
		Routes:      api.RoutesWrite,
		Ingester:    gate.Metrics(writes),
		LogIngester: gate.Logs(logHead),
		Registry:    reg,
		HTTP:        inst.HTTP,
		Ingest:      inst.Ingest,
		Internal: func(r chi.Router) {
			rpc.MountDrain(r, drainHeads, ingesterDrainTimeout)
			rpc.MountReads(r, head, logs.AsSource(logHead))
			rpc.MountWrites(r, gate.Metrics(writes), gate.Logs(logHead), inst.Ingest)
		},
	})
	mcfg := maintenanceConfig(cfg)
	mcfg.SkipFinalFlush = true // the drain below flushes the whole head instead
	flush := compactor.New(writes, nil, writes, time.Now, mcfg,
		inst.Maintenance, flushLog)

	return &App{
		Target:  config.TargetIngester,
		Handler: srv,
		loops:   []func(ctx context.Context){flush.Run},
		closers: []closer{
			// One bounded drain before the WALs close, in place of the loop's
			// final flush: an ingester removed from the ring must not leave its
			// open chunks or buffered lines in a WAL no reader will see. It
			// shares the process's shutdown budget (ctx) and is capped at
			// ingesterDrainTimeout. Both heads are attempted. A failed logs
			// flush is already logged once by the flush hook above, so this
			// reports only what the hook cannot: the gate or a metrics failure,
			// or lines left in the head. On failure the data is still in the
			// WAL and replays on the next start.
			{component: "flush", msg: "drain failed: unflushed data stays in this ingester's WAL until it restarts", closeCtx: func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, ingesterDrainTimeout)
				defer cancel()
				if err := gate.Close(ctx); err != nil {
					return fmt.Errorf("waiting for in-flight writes: %w", err)
				}
				metricsErr := writes.Drain(ctx)
				logsErr := logHead.FlushContext(ctx)
				if metricsErr != nil {
					return fmt.Errorf("metrics: %w", metricsErr)
				}
				if logsErr == nil && !logHead.Empty() {
					return errors.New("logs: head not empty after the drain")
				}
				return nil
			}},
			{component: "wal", msg: "wal close error", close: w.Close},
			// The drain above made the one bounded attempt to flush the logs
			// head; closing without another flush keeps a store outage from
			// being retried (and reported) a second time past the deadline.
			// Whatever it could not flush stays in the logs WAL.
			{component: "logs", msg: "logs WAL close error", close: logHead.CloseWithoutFlush},
		},
		log: log,
	}, nil
}
