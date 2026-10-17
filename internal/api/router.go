package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/api/middleware"
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) buildRouter() chi.Router {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	r.Use(middleware.Logger(s.log))
	r.Use(middleware.Metrics(s.http))
	r.Use(s.middleware...)

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// A Deps without a Registry is normal in tests. Registering promhttp with a
	// nil registry panics at request time, which would surface as an unrelated
	// handler test failing.
	if s.reg != nil {
		r.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	}

	// Each public route family resolves the tenant with its own error shape.
	// /internal/v1, /healthz, /readyz and /metrics never go through it.
	tenantFor := func(reject func(http.ResponseWriter, int, string)) func(http.Handler) http.Handler {
		return tenant.Middleware(s.tenantRequired, reject)
	}
	promTenant := tenantFor(func(w http.ResponseWriter, st int, msg string) { writePromError(w, st, promErrorType(st), msg) })
	lokiTenant := tenantFor(func(w http.ResponseWriter, st int, msg string) { writeLokiError(w, st, msg) })
	writeTenant := tenantFor(func(w http.ResponseWriter, st int, msg string) { writeJSON(w, st, map[string]string{"error": msg}) })

	if s.routes.writes() {
		w := r.With(writeTenant)
		w.Method(http.MethodPost, "/api/v1/ingest/metrics", s.write(s.handleIngestMetrics))
		w.Method(http.MethodPost, "/loki/api/v1/push", s.write(s.handleLokiPush))
	}

	if s.routes.reads() {
		pr := r.With(promTenant)
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			pr.Method(m, "/api/v1/query", s.promRead(s.handleQuery))
			pr.Method(m, "/api/v1/query_range", s.promRead(s.handleQueryRange))
			pr.Method(m, "/api/v1/labels", s.promRead(s.handleLabels))
			pr.Method(m, "/api/v1/label/{name}/values", s.promRead(s.handleLabelValues))
			pr.Method(m, "/api/v1/series", s.promRead(s.handleSeries))
		}

		lr := r.With(lokiTenant)
		lr.Method(http.MethodGet, "/loki/api/v1/query", s.lokiRead(s.handleLokiQuery))
		lr.Method(http.MethodGet, "/loki/api/v1/query_range", s.lokiRead(s.handleLokiQueryRange))
		lr.Method(http.MethodGet, "/loki/api/v1/labels", s.lokiRead(s.handleLokiLabels))
		lr.Method(http.MethodGet, "/loki/api/v1/label/{name}/values", s.lokiRead(s.handleLokiLabelValues))
	}

	if s.internal != nil {
		r.Route("/internal/v1", s.internal)
	}

	return r
}

// write returns the local handler: a gateway runs the same handler and routes
// the validated batch through Deps.Writes (see handleIngestMetrics, handleLokiPush).
func (s *Server) write(local http.HandlerFunc) http.Handler { return local }

// promRead returns the local handler, or on a gateway the proxy for the
// Prometheus route family.
func (s *Server) promRead(local http.HandlerFunc) http.Handler {
	if s.gateway != nil {
		return s.gateway.promRead
	}
	return local
}

// lokiRead is promRead for the Loki route family.
func (s *Server) lokiRead(local http.HandlerFunc) http.Handler {
	if s.gateway != nil {
		return s.gateway.lokiRead
	}
	return local
}

// promErrorType is the Prometheus errorType for a tenant rejection.
func promErrorType(status int) string {
	if status == http.StatusUnauthorized {
		return "unauthorized"
	}
	return "bad_data"
}
