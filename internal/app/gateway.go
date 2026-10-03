package app

import (
	"fmt"
	"log/slog"
	"net/url"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// buildGateway assembles the gateway: all-in-one's public route table,
// validated and ring-routed writes, proxied reads.
func buildGateway(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	members, err := ring.New(cfg.IngesterURLs)
	if err != nil {
		err = fmt.Errorf("app: ingester ring: %w", err)
		mainLog.Error("failed to build ingester ring", slog.String("error", err.Error()))
		return nil, err
	}
	router, err := rpc.NewRouter(members, nil)
	if err != nil {
		err = fmt.Errorf("app: write router: %w", err)
		mainLog.Error("failed to build write router", slog.String("error", err.Error()))
		return nil, err
	}
	querier, err := url.Parse(cfg.QuerierURL)
	if err != nil {
		err = fmt.Errorf("app: querier URL: %w", err)
		mainLog.Error("failed to parse querier URL", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Omit: observability.AllGroups, Logger: log})
	srv := api.New(api.Deps{
		Config:    cfg,
		Logger:    log,
		Registry:  reg,
		HTTP:      inst.HTTP,
		Ready:     alwaysReady,
		Upstreams: &api.Upstreams{Querier: querier},
		Writes:    router,
	})
	return &App{Target: config.TargetGateway, Handler: srv, log: log}, nil
}
