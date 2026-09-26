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

// RouteSet selects which public routes a Server serves.
type RouteSet int

const (
	// RoutesAll serves every public route. It is the zero value, so all-in-one
	// and every Deps written before route sets existed keep serving everything.
	RoutesAll RouteSet = iota
	// RoutesWrite serves the two write routes: the ingester.
	RoutesWrite
	// RoutesRead serves the nine read routes: the querier.
	RoutesRead
	// RoutesNone serves only /healthz, /readyz, and /metrics: the store and the compactor.
	RoutesNone
)

func (r RouteSet) writes() bool { return r == RoutesAll || r == RoutesWrite }
func (r RouteSet) reads() bool  { return r == RoutesAll || r == RoutesRead }

// Deps are the server's collaborators. This is a struct rather than a parameter
// list because the list had already reached seven same-shaped pointers, where a
// caller can transpose two arguments and still compile.
type Deps struct {
	Config *config.Config

	// Logger must NOT already carry a "component" attribute (for example via
	// observability.Component(log, "...")). The request middleware (see
	// internal/api/middleware.Logger) derives the request-scoped context
	// logger from exactly this value, and handlers add their own component on
	// top of that with observability.Component(observability.FromContext(ctx),
	// "<subsystem>"). slog does not deduplicate attributes added by separate
	// With calls, so a Logger that already has "component" set produces TWO
	// "component" keys on every handler log line instead of one, and the
	// field stops working as a grouping key. Pass the plain, component-free
	// logger here; "component" should only ever be added by the access-log
	// line and by each handler's own subsystem tag.
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

	// Routes selects the public routes served; the zero value serves them all.
	Routes RouteSet

	// Internal, when set, mounts a component's internal API under
	// /internal/v1, behind the same request-ID, logging, and metrics
	// middleware as the public routes.
	Internal func(r chi.Router)

	// Ready, when set, replaces the default readiness check (creating and
	// removing a temp file in Config.DataDir). Stateless components pass one
	// that always succeeds: they own no data directory to prove writable.
	Ready func() error
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
	routes      RouteSet
	internal    func(chi.Router)
	ready       func() error
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
		routes:      d.Routes,
		internal:    d.Internal,
		ready:       d.Ready,
	}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// Router exposes the built router so callers can enumerate what this server
// actually serves (chi.Walk) rather than re-deriving it from source. The docs
// route-coverage tests depend on seeing the real registrations, including the
// conditional /metrics.
func (s *Server) Router() chi.Router { return s.router }
