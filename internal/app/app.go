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
type closer struct {
	component string
	msg       string
	close     func() error
}

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
	default:
		// Logged here, where it originates, rather than by the caller: every
		// Build failure path (this one and BuildAllInOne's) then logs exactly
		// once, matching the single "startup failed"/specific-cause line main
		// has always produced instead of doubling up.
		err := fmt.Errorf("app: target %q is not available yet", cfg.Target)
		observability.Component(log, "main").Error("startup failed",
			slog.String("target", string(cfg.Target)), slog.String("error", err.Error()))
		return nil, err
	}
}

// Run starts every background loop and blocks until ctx is done and all of
// them have returned. The maintenance loop does its final flush on the way out,
// so Run must return before Close. With zero loops (a target with nothing to
// run in the background, e.g. a future gateway or querier) Run still blocks
// on ctx alone: callers rely on Run not returning before shutdown regardless
// of how many loops a target happens to register.
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

// Close releases resources in the order they were registered, logging each
// failure under its own component. Shutdown still completes: there is nothing
// left to retry at this point, and the next start replays from whatever
// reached disk.
func (a *App) Close() {
	for _, c := range a.closers {
		if err := c.close(); err != nil {
			observability.Component(a.log, c.component).Error(c.msg, slog.String("error", err.Error()))
		}
	}
}
