package app

import (
	"log/slog"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// buildQuerier assembles the querier: the nine read routes over the ingester
// and store merged -- ingester first, which is what keeps a flush invisible.
func buildQuerier(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	ingester, err := rpc.NewClient("ingester", cfg.IngesterURL)
	if err != nil {
		mainLog.Error("failed to build ingester client", slog.String("error", err.Error()))
		return nil, err
	}
	store, err := rpc.NewClient("store", cfg.StoreURL)
	if err != nil {
		mainLog.Error("failed to build store client", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Logger: log})
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(metrics.Merge(rpc.NewMetricsSource(ingester), rpc.NewMetricsSource(store))),
		LogQuery: logs.NewQueryEngineFromSource(logs.Merge(rpc.NewLogsSource(ingester), rpc.NewLogsSource(store))),
		Registry: reg,
		HTTP:     inst.HTTP,
		Ready:    alwaysReady,
	})
	return &App{Target: config.TargetQuerier, Handler: srv, log: log}, nil
}
