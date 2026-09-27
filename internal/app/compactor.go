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
// planning locally and having the store at cfg.StoreURL execute. It owns no
// data and serves only health and metrics.
func buildCompactor(cfg *config.Config, log *slog.Logger) (*App, error) {
	store, err := rpc.NewClient("store", cfg.StoreURL)
	if err != nil {
		observability.Component(log, "main").Error("failed to build store client", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Logger: log})
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesNone,
		Registry: reg,
		HTTP:     inst.HTTP,
		Ready:    alwaysReady,
	})
	loop := compactor.New(nil, rpc.NewBlockManager(store), nil, time.Now, maintenanceConfig(cfg),
		inst.Maintenance, observability.Component(log, "compactor"))
	return &App{
		Target:  config.TargetCompactor,
		Handler: srv,
		loops:   []func(context.Context){loop.Run},
		log:     log,
	}, nil
}
