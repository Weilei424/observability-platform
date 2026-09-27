package app

import (
	"fmt"
	"log/slog"
	"net/url"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// buildGateway assembles the gateway: all-in-one's public route table,
// proxied -- writes to the ingester, reads to the querier.
func buildGateway(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	ingester, err := url.Parse(cfg.IngesterURL)
	if err != nil {
		err = fmt.Errorf("app: ingester URL: %w", err)
		mainLog.Error("failed to parse ingester URL", slog.String("error", err.Error()))
		return nil, err
	}
	querier, err := url.Parse(cfg.QuerierURL)
	if err != nil {
		err = fmt.Errorf("app: querier URL: %w", err)
		mainLog.Error("failed to parse querier URL", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Logger: log})
	srv := api.New(api.Deps{
		Config:    cfg,
		Logger:    log,
		Registry:  reg,
		HTTP:      inst.HTTP,
		Ready:     alwaysReady,
		Upstreams: &api.Upstreams{Ingester: ingester, Querier: querier},
	})
	return &App{Target: config.TargetGateway, Handler: srv, log: log}, nil
}
