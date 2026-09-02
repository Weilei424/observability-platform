package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
)

// Deps are the server's collaborators. This is a struct rather than a parameter
// list because the list had already reached seven same-shaped pointers, where a
// caller can transpose two arguments and still compile.
type Deps struct {
	Config      *config.Config
	Logger      *slog.Logger
	Ingester    metrics.Ingester
	Engine      *metrics.QueryEngine
	Registry    *prometheus.Registry
	LogIngester logs.Ingester
	LogQuery    *logs.QueryEngine

	// HTTP and Ingest are optional. When nil, New substitutes unregistered
	// instruments so handlers never nil-check and tests never panic.
	HTTP   *observability.HTTPMetrics
	Ingest *observability.IngestMetrics
}

type Server struct {
	cfg         *config.Config
	log         *slog.Logger
	router      chi.Router
	ingester    metrics.Ingester
	engine      *metrics.QueryEngine
	reg         *prometheus.Registry
	logIngester logs.Ingester
	logQuery    *logs.QueryEngine
	http        *observability.HTTPMetrics
	ingest      *observability.IngestMetrics
}

func New(d Deps) *Server {
	if d.HTTP == nil {
		d.HTTP = observability.NewHTTPMetrics()
	}
	if d.Ingest == nil {
		d.Ingest = observability.NewIngestMetrics()
	}
	s := &Server{
		cfg:         d.Config,
		log:         d.Logger,
		ingester:    d.Ingester,
		engine:      d.Engine,
		reg:         d.Registry,
		logIngester: d.LogIngester,
		logQuery:    d.LogQuery,
		http:        d.HTTP,
		ingest:      d.Ingest,
	}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
