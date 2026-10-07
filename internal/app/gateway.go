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
	// The gateway validates writes, so it owns the ingest counters' rejections;
	// the ingesters count what lands.
	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Omit: observability.AllGroups &^ observability.IngestGroup, Logger: log,
	})
	rm := observability.NewRingMetrics()
	labels := make([]string, 0, len(members.Members()))
	for _, m := range members.Members() {
		labels = append(labels, rpc.MemberLabel(m))
	}
	rm.Register(reg, labels, true)
	router, err := rpc.NewRouter(members, rpc.RouterOptions{ReplicationFactor: 1, ObserveMember: func(member, outcome string) {
		rm.IngesterRequests.WithLabelValues(member, outcome).Inc()
	}})
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
	logRing(mainLog, members)
	srv := api.New(api.Deps{
		Config:    cfg,
		Logger:    log,
		Registry:  reg,
		HTTP:      inst.HTTP,
		Ingest:    inst.Ingest,
		Ready:     alwaysReady,
		Upstreams: &api.Upstreams{Querier: querier},
		Writes:    router,
	})
	return &App{Target: config.TargetGateway, Handler: srv, log: log}, nil
}

// logRing logs the ring's size and member-set hash. The gateway and querier
// log the same hash exactly when they route and read over the same set.
func logRing(mainLog *slog.Logger, r *ring.Ring) {
	mainLog.Info("ring ready", slog.Int("members", len(r.Members())), slog.String("ring", r.Hash()))
}
