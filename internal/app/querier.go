package app

import (
	"log/slog"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// buildQuerier assembles the querier: the nine read routes over every ingester,
// then the store -- every ingester read to completion before the store, which
// keeps a flush invisible.
func buildQuerier(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	r, err := ring.New(cfg.IngesterURLs)
	if err != nil {
		mainLog.Error("failed to build the ingester ring", slog.String("error", err.Error()))
		return nil, err
	}
	var metricHeads []metrics.Source
	var logHeads []logs.Source
	for _, m := range r.Members() {
		c, err := rpc.NewClient("ingester "+rpc.MemberLabel(m), m)
		if err != nil {
			mainLog.Error("failed to build ingester client", slog.String("error", err.Error()))
			return nil, err
		}
		metricHeads = append(metricHeads, rpc.NewMetricsSource(c))
		logHeads = append(logHeads, rpc.NewLogsSource(c))
	}
	store, err := rpc.NewClient("store", cfg.StoreURL)
	if err != nil {
		mainLog.Error("failed to build store client", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Omit: observability.AllGroups, Logger: log})
	observability.NewRingMetrics().Register(reg, r.Members(), false)
	logRing(mainLog, r, max(cfg.ReplicationFactor, 1))
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(metrics.Merge(metrics.MergeHeads(metricHeads...), rpc.NewMetricsSource(store))),
		LogQuery: logs.NewQueryEngineFromSource(logs.Merge(logs.MergeHeads(logHeads...), rpc.NewLogsSource(store))),
		Registry: reg,
		HTTP:     inst.HTTP,
		Ready:    alwaysReady,
	})
	return &App{Target: config.TargetQuerier, Handler: srv, log: log}, nil
}
