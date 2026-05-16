package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Error("failed to create data directory",
			slog.String("data_dir", cfg.DataDir),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	srv := api.New(cfg, log)

	log.Info("starting server",
		slog.String("addr", cfg.HTTPAddr),
		slog.String("data_dir", cfg.DataDir),
	)

	if err := http.ListenAndServe(cfg.HTTPAddr, srv); err != nil {
		log.Error("server stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
