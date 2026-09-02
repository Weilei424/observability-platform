package api

import (
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/api/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) buildRouter() chi.Router {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	r.Use(middleware.Logger(s.log))
	r.Use(middleware.Metrics(s.http))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// A Deps without a Registry is normal in tests. Registering promhttp with a
	// nil registry panics at request time, which would surface as an unrelated
	// handler test failing.
	if s.reg != nil {
		r.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	}

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

	r.Post("/loki/api/v1/push", s.handleLokiPush)
	r.Get("/loki/api/v1/query", s.handleLokiQuery)
	r.Get("/loki/api/v1/query_range", s.handleLokiQueryRange)
	r.Get("/loki/api/v1/labels", s.handleLokiLabels)
	r.Get("/loki/api/v1/label/{name}/values", s.handleLokiLabelValues)

	return r
}
