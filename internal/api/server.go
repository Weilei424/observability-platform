package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type Server struct {
	cfg      *config.Config
	log      *slog.Logger
	router   chi.Router
	ingester metrics.Ingester
}

func New(cfg *config.Config, log *slog.Logger, ingester metrics.Ingester) *Server {
	s := &Server{
		cfg:      cfg,
		log:      log,
		ingester: ingester,
	}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
