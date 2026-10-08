package app

import (
	"context"
	"errors"
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
	store, err := rpc.NewClient("store", cfg.StoreURL)
	if err != nil {
		mainLog.Error("failed to build store client", slog.String("error", err.Error()))
		return nil, err
	}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{Omit: observability.AllGroups, Logger: log})
	rm := observability.NewRingMetrics()
	rm.Register(reg, labels, false)
	logRing(mainLog, r, rf)
	queryLog := observability.Component(log, "querier")
	skippable := func(err error) bool { return errors.Is(err, rpc.ErrUnavailable) }
	// A read the caller cancelled says nothing about the ingester, so it is
	// not counted, as the gateway's ObserveMember skips a cancelled push.
	observe := func(i int, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		rm.IngesterReads.WithLabelValues(labels[i], rpc.OutcomeOf(err)).Inc()
	}
	onSkip := func(skipped []int) {
		names := make([]string, len(skipped))
		for j, i := range skipped {
			names[j] = labels[i]
		}
		queryLog.Warn("read answered by replication", slog.Any("skipped", names))
	}
	tolerate := config.Quorum(rf) - 1
	srv := api.New(api.Deps{
		Config:   cfg,
		Logger:   log,
		Routes:   api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(metrics.Merge(metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: tolerate, Skippable: skippable, Observe: observe, OnSkip: onSkip}, metricHeads...), rpc.NewMetricsSource(store))),
		LogQuery: logs.NewQueryEngineFromSource(logs.Merge(logs.MergeHeadsWith(logs.HeadsOptions{Tolerate: tolerate, Skippable: skippable, Observe: observe, OnSkip: onSkip}, logHeads...), rpc.NewLogsSource(store))),
		Registry: reg,
		HTTP:     inst.HTTP,
		Ready:    alwaysReady,
	})
	return &App{Target: config.TargetQuerier, Handler: srv, log: log}, nil
}
