// Package app assembles one process from the component OBS_TARGET names: the
// HTTP handler it serves, the background loops it runs, and the resources it
// closes on shutdown.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// App is one assembled process.
type App struct {
	Target  config.Target
	Handler http.Handler

	loops   []func(ctx context.Context)
	closers []closer
	log     *slog.Logger
}

// closer is one resource to release at shutdown, and the line its failure logs.
// closeCtx, when set, is used instead of close: a closer that waits on a peer
// (the ingester's drain) takes what is left of the shutdown budget.
type closer struct {
	component string
	msg       string
	close     func() error
	closeCtx  func(ctx context.Context) error
}

// alwaysReady is the readiness of a component that owns no data directory:
// serving is enough. Peer outages surface on requests, never as readiness.
// The gateway, querier, and compactor all pass it.
func alwaysReady() error { return nil }

// Build assembles the process for cfg.Target. log must be component-free: it
// becomes api.Deps.Logger (see the doc comment on that field).
func Build(cfg *config.Config, log *slog.Logger) (*App, error) {
	switch cfg.Target {
	case config.TargetAllInOne:
		a, err := BuildAllInOne(cfg, log)
		if err != nil {
			return nil, err
		}
		return a.App(log), nil
	case config.TargetIngester:
		return buildIngester(cfg, log)
	case config.TargetStore:
		return buildStore(cfg, log)
	case config.TargetQuerier:
		return buildQuerier(cfg, log)
	case config.TargetGateway:
		return buildGateway(cfg, log)
	case config.TargetCompactor:
		return buildCompactor(cfg, log)
	default:
		// Logged here, where it originates, rather than by the caller: every
		// Build failure path (this one and every buildX's) then logs exactly
		// once, matching the single "startup failed"/specific-cause line main
		// has always produced instead of doubling up. Every named target now
		// builds, so this default branch is reached only by a Target string
		// outside the six config.Targets constants -- unreachable through
		// config.Load (validateTopology rejects it first), but Build is also
		// called directly in tests with a hand-built Config that bypasses that
		// validation.
		err := fmt.Errorf("app: unknown target %q", cfg.Target)
		observability.Component(log, "main").Error("startup failed",
			slog.String("target", string(cfg.Target)), slog.String("error", err.Error()))
		return nil, err
	}
}

// Run starts every background loop and blocks until ctx is done and all of
// them have returned. The maintenance loop does its final flush on the way out,
// so Run must return before Close. With zero loops (the gateway and querier
// register none) Run still blocks on ctx alone: callers rely on Run not
// returning before shutdown regardless of how many loops a target happens to
// register.
func (a *App) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, loop := range a.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

// Close is CloseContext with no shutdown budget of its own.
func (a *App) Close() { a.CloseContext(context.Background()) }

// CloseContext releases resources in the order they were registered, logging
// each failure under its own component. A closer that waits on a peer is
// bounded by ctx, the rest of the process's shutdown budget. Shutdown still
// completes: there is nothing left to retry at this point, and the next start
// replays from whatever reached disk.
func (a *App) CloseContext(ctx context.Context) {
	for _, c := range a.closers {
		var err error
		if c.closeCtx != nil {
			err = c.closeCtx(ctx)
		} else {
			err = c.close()
		}
		if err != nil {
			observability.Component(a.log, c.component).Error(c.msg, slog.String("error", err.Error()))
		}
	}
}
