package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type Server struct {
	cfg         *config.Config
	log         *slog.Logger
	router      chi.Router
	ingester    metrics.Ingester
	engine      *metrics.QueryEngine
	reg         *prometheus.Registry
	logIngester logs.Ingester
}

func New(cfg *config.Config, log *slog.Logger, ingester metrics.Ingester, engine *metrics.QueryEngine, reg *prometheus.Registry, logIngester logs.Ingester) *Server {
	s := &Server{
		cfg:         cfg,
		log:         log,
		ingester:    ingester,
		engine:      engine,
		reg:         reg,
		logIngester: logIngester,
	}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
