package api

import (
	"net/http"

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

	if s.routes.writes() {
		r.Method(http.MethodPost, "/api/v1/ingest/metrics", s.write(s.handleIngestMetrics))
		r.Method(http.MethodPost, "/loki/api/v1/push", s.write(s.handleLokiPush))
	}

	if s.routes.reads() {
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			r.Method(m, "/api/v1/query", s.promRead(s.handleQuery))
			r.Method(m, "/api/v1/query_range", s.promRead(s.handleQueryRange))
			r.Method(m, "/api/v1/labels", s.promRead(s.handleLabels))
			r.Method(m, "/api/v1/label/{name}/values", s.promRead(s.handleLabelValues))
			r.Method(m, "/api/v1/series", s.promRead(s.handleSeries))
		}

		r.Method(http.MethodGet, "/loki/api/v1/query", s.lokiRead(s.handleLokiQuery))
		r.Method(http.MethodGet, "/loki/api/v1/query_range", s.lokiRead(s.handleLokiQueryRange))
		r.Method(http.MethodGet, "/loki/api/v1/labels", s.lokiRead(s.handleLokiLabels))
		r.Method(http.MethodGet, "/loki/api/v1/label/{name}/values", s.lokiRead(s.handleLokiLabelValues))
	}

	if s.internal != nil {
		r.Route("/internal/v1", s.internal)
	}

	return r
}

// write, promRead, and lokiRead return the local handler, or on a gateway the
// proxy for that route family.
func (s *Server) write(local http.HandlerFunc) http.Handler {
	if s.gateway != nil {
		return s.gateway.write
	}
	return local
}

func (s *Server) promRead(local http.HandlerFunc) http.Handler {
	if s.gateway != nil {
		return s.gateway.promRead
	}
	return local
}

func (s *Server) lokiRead(local http.HandlerFunc) http.Handler {
	if s.gateway != nil {
		return s.gateway.lokiRead
	}
	return local
}
