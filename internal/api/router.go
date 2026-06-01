package api

import (
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/api/middleware"
)

func (s *Server) buildRouter() chi.Router {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	r.Use(middleware.Logger(s.log))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	r.Post("/api/v1/ingest/metrics", s.handleIngestMetrics)
	r.Get("/api/v1/query", s.handleQuery)
	r.Post("/api/v1/query", s.handleQuery)
	r.Get("/api/v1/query_range", s.handleQueryRange)
	r.Post("/api/v1/query_range", s.handleQueryRange)
	r.Get("/api/v1/labels", s.handleLabels)
	r.Post("/api/v1/labels", s.handleLabels)
	r.Get("/api/v1/label/{name}/values", s.handleLabelValues)
	r.Post("/api/v1/label/{name}/values", s.handleLabelValues)
	r.Get("/api/v1/series", s.handleSeries)
	r.Post("/api/v1/series", s.handleSeries)

	return r
}
