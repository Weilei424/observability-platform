package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	// Probe mode runs before the logger, the data directory, and every store: a
	// healthcheck must not create files or replay a WAL. Config is loaded first
	// so the probe follows OBS_HTTP_ADDR automatically.
	if healthcheckRequested(os.Args[1:]) {
		url, err := probeURL(cfg.HTTPAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
			os.Exit(1)
		}
		os.Exit(runHealthcheck(url, &http.Client{Timeout: 2 * time.Second}, os.Stderr))
	}

	log, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(1)
	}
	// Route package-level slog calls (e.g. WAL replay recovery warnings) through
	// the structured JSON application logger instead of the stdlib text default.
	slog.SetDefault(log)

	// log itself is api.Deps.Logger and must stay component-free (see the doc
	// comment on that field in internal/api/server.go), so main()'s own
	// lifecycle logging goes through a derived logger instead of log directly.
	mainLog := observability.Component(log, "main")

	// Install the signal handler before app.Build, not after: Build's storage
	// bring-up (WAL replay) can run long enough to matter, and a SIGINT sent
	// while it is still ignored -- its inherited disposition in any `&` child
	// of a shell without job control, e.g. a script, `bash -c`, a Makefile
	// recipe, or CI -- is discarded by the kernel outright, never queued, so
	// installing the handler any later would lose a shutdown signal sent
	// during startup. Once installed here, a signal delivered during Build is
	// captured on ctx, so <-ctx.Done() below returns immediately instead of
	// waiting for a second signal.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.Build(cfg, log)
	if err != nil {
		// app.Build already logged the specific failure.
		os.Exit(1)
	}

	runDone := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(runDone)
	}()

	// Bind synchronously so a bind failure (e.g. address already in use) is fatal
	// immediately, rather than surfacing later during graceful shutdown. With
	// port 0 the OS assigns a free ephemeral port; ln.Addr() reports the actual
	// bound address, which we optionally publish to OBS_ADDR_FILE so a supervising
	// process (e.g. the k6 bench runner) can discover it race-free.
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		mainLog.Error("failed to bind HTTP address", slog.String("addr", cfg.HTTPAddr), slog.String("error", err.Error()))
		os.Exit(1)
	}
	boundAddr := ln.Addr().String()
	if addrFile := os.Getenv("OBS_ADDR_FILE"); addrFile != "" {
		if err := os.WriteFile(addrFile, []byte(boundAddr+"\n"), 0o644); err != nil {
			mainLog.Error("failed to write OBS_ADDR_FILE", slog.String("path", addrFile), slog.String("error", err.Error()))
			_ = ln.Close()
			os.Exit(1)
		}
	}

	httpSrv := &http.Server{Handler: a.Handler}
	go func() {
		mainLog.Info("starting server", slog.String("addr", boundAddr), slog.String("data_dir", cfg.DataDir))
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			mainLog.Error("server stopped", slog.String("error", err.Error()))
			stop() // unblock shutdown below
		}
	}()

	<-ctx.Done()
	mainLog.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		mainLog.Error("http shutdown error", slog.String("error", err.Error()))
	}

	<-runDone // the maintenance loop performs its final flush on ctx cancellation
	a.Close()
	mainLog.Info("shutdown complete")
}

// serverComponents are the all-in-one collaborators cmd/server's tests drive:
// the API server, plus the storage handles they close.
type serverComponents struct {
	Server      *api.Server
	Store       *metrics.WALStore
	BlockStore  *metrics.BlockStore
	WAL         *wal.WAL
	LogStore    *logs.Store
	Maintenance *observability.Metrics
}

// buildServer is the all-in-one assembly main() runs through app.Build, kept
// as the entry point these tests use: a regression in production's wiring (for
// example a component-stamped api.Deps.Logger) shows up here, where a
// hand-assembled api.Deps in another package's tests would never see it.
func buildServer(cfg *config.Config, log *slog.Logger) (*serverComponents, error) {
	a, err := app.BuildAllInOne(cfg, log)
	if err != nil {
		return nil, err
	}
	return &serverComponents{
		Server:      a.Server,
		Store:       a.Store,
		BlockStore:  a.BlockStore,
		WAL:         a.WAL,
		LogStore:    a.LogStore,
		Maintenance: a.Maintenance,
	}, nil
}
