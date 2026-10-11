package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// buildQuerier assembles the querier: the nine read routes over every
// ingester read in parallel, then every store in parallel; all ingester reads
// finish before any store read starts, which keeps a flush invisible. Up to
// W_s-1 unavailable stores are skipped, as up to W-1 ingesters are.
func buildQuerier(cfg *config.Config, log *slog.Logger) (*App, error) {
	mainLog := observability.Component(log, "main")
	r, err := ring.New(cfg.IngesterURLs)
	if err != nil {
		mainLog.Error("failed to build the ingester ring", slog.String("error", err.Error()))
		return nil, err
	}
	rf := max(cfg.ReplicationFactor, 1)
	timeout := cfg.IngesterTimeout
	if timeout <= 0 {
		timeout = config.DefaultIngesterTimeout
	}
	members := r.Members()
	labels := make([]string, len(members))
	for i, m := range members {
		labels[i] = rpc.MemberLabel(m)
	}
	var metricHeads []metrics.Source
	var logHeads []logs.Source
	for i, m := range members {
		c, err := rpc.NewClient("ingester "+labels[i], m, rpc.WithRequestTimeout(timeout))
		if err != nil {
			mainLog.Error("failed to build ingester client", slog.String("error", err.Error()))
			return nil, err
		}
		metricHeads = append(metricHeads, rpc.NewMetricsSource(c))
		logHeads = append(logHeads, rpc.NewLogsSource(c))
	}
	storeRing, err := ring.New(cfg.StoreURLs)
	if err != nil {
		mainLog.Error("failed to build the store ring", slog.String("error", err.Error()))
		return nil, err
	}
	storeMembers := storeRing.Members()
	storeLabels := labelsOf(storeMembers)
	var metricStores []metrics.Source
	var logStores []logs.Source
	for i, m := range storeMembers {
		c, err := rpc.NewClient("store "+storeLabels[i], m, rpc.WithRequestTimeout(timeout))
		if err != nil {
			mainLog.Error("failed to build store client", slog.String("error", err.Error()))
			return nil, err
		}
		metricStores = append(metricStores, rpc.NewMetricsSource(c))
		logStores = append(logStores, rpc.NewLogsSource(c))
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Omit: observability.AllGroups, Logger: log})
	rm := observability.NewRingMetrics()
	rm.Register(reg, labels, false)
	sm := observability.NewStoreMetrics()
	sm.RegisterQuerier(reg, storeLabels)
	logRing(mainLog, r, rf)
	skippable := func(err error) bool { return errors.Is(err, rpc.ErrUnavailable) }
	// A read the caller cancelled says nothing about the ingester, so it is
	// not counted, as the gateway's ObserveMember skips a cancelled push.
	observe := func(i int, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		rm.IngesterReads.WithLabelValues(labels[i], rpc.OutcomeOf(err)).Inc()
	}
	// The head merge records a skip on the request's note; the request logs
	// it once, with its ID, only if its response succeeded
	// (replication_note.go).
	onSkip := func(ctx context.Context, skipped []int) {
		names := make([]string, len(skipped))
		for j, i := range skipped {
			names[j] = labels[i]
		}
		recordSkip(ctx, names)
	}
	tolerate := config.Quorum(rf) - 1
	storeTolerate := config.Quorum(max(cfg.StoreReplicationFactor, 1)) - 1
	storeObserve := func(i int, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		sm.StoreReads.WithLabelValues(storeLabels[i], rpc.OutcomeOf(err)).Inc()
	}
	onStoreSkip := func(ctx context.Context, skipped []int) {
		names := make([]string, len(skipped))
		for j, i := range skipped {
			names[j] = "store " + storeLabels[i]
		}
		recordSkip(ctx, names)
	}
	ingesterOpts := metrics.HeadsOptions{Tolerate: tolerate, Skippable: skippable, Observe: observe, OnSkip: onSkip}
	storeOpts := metrics.HeadsOptions{Tolerate: storeTolerate, Skippable: skippable, Observe: storeObserve, OnSkip: onStoreSkip}
	logIngesterOpts := logs.HeadsOptions{Tolerate: tolerate, Skippable: skippable, Observe: observe, OnSkip: onSkip}
	logStoreOpts := logs.HeadsOptions{Tolerate: storeTolerate, Skippable: skippable, Observe: storeObserve, OnSkip: onStoreSkip}
	srv := api.New(api.Deps{
		Config: cfg,
		Logger: log,
		Routes: api.RoutesRead,
		Engine: metrics.NewQueryEngineFromSource(metrics.Merge(
			metrics.MergeHeadsWith(ingesterOpts, metricHeads...),
			metrics.MergeHeadsWith(storeOpts, metricStores...))),
		LogQuery: logs.NewQueryEngineFromSource(logs.Merge(
			logs.MergeHeadsWith(logIngesterOpts, logHeads...),
			logs.MergeHeadsWith(logStoreOpts, logStores...))),
		Middleware: []func(http.Handler) http.Handler{replicationNotes},
		Registry:   reg,
		HTTP:       inst.HTTP,
		Ready:      alwaysReady,
	})
	return &App{Target: config.TargetQuerier, Handler: srv, log: log}, nil
}
