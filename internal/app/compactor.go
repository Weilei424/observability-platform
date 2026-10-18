package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// buildCompactor assembles the compactor: the compaction and retention loop,
// planning locally and having every store in cfg.StoreURLs execute. It owns no
// data and serves only health and metrics.
func buildCompactor(cfg *config.Config, log *slog.Logger) (*App, error) {
	labels := labelsOf(cfg.StoreURLs)
	managers := make([]compactor.BlockManager, 0, len(cfg.StoreURLs))
	logManagers := make([]compactor.LogManager, 0, len(cfg.StoreURLs))
	for i, u := range cfg.StoreURLs {
		store, err := rpc.NewClient("store "+labels[i], u)
		if err != nil {
			observability.Component(log, "main").Error("failed to build store client", slog.String("error", err.Error()))
			return nil, err
		}
		managers = append(managers, rpc.NewBlockManager(store))
		logManagers = append(logManagers, rpc.NewLogsRetentionClient(store))
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Omit:   observability.FlushGroup | observability.IngestGroup | observability.LogFlushGroup,
		Logger: log,
	})
	sm := observability.NewStoreMetrics()
	sm.RegisterCompactor(reg, labels)
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesNone,
		Registry: reg,
		HTTP:     inst.HTTP,
		Ready:    alwaysReady,
	})
	clog := observability.Component(log, "compactor")
	observe := func(s, o string) { sm.CompactorPass.WithLabelValues(s, o).Inc() }
	blocks := compactor.NewMultiBlockManager(labels, managers, rpc.OutcomeOf, observe, clog)
	logsRetention := compactor.NewMultiLogManager(labels, logManagers, rpc.OutcomeOf, observe, clog)
	loop := compactor.New(nil, blocks, nil, time.Now, maintenanceConfig(cfg),
		inst.Maintenance, clog, compactor.WithLogs(logsRetention))
	return &App{
		Target:  config.TargetCompactor,
		Handler: srv,
		loops:   []func(context.Context){loop.Run},
		log:     log,
	}, nil
}
