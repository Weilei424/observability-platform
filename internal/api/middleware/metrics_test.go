package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// routerWith builds a chi router carrying the metrics middleware and one
// parameterised route, which is the case that matters: the label must be the
// PATTERN, not the resolved path.
func routerWith(m *observability.HTTPMetrics) chi.Router {
	r := chi.NewRouter()
	r.Use(Metrics(m))
	r.Get("/api/v1/label/{name}/values", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/boom", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	r.Get("/silent", func(w http.ResponseWriter, r *http.Request) {})
	r.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	return r
}

func do(r chi.Router, method, target string) {
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, target, nil))
}

// The whole point of the route label. Two requests for different label names are
// one time series, not two — otherwise a client scanning label values grows the
// registry without bound.
func TestRouteLabelIsThePatternNotThePath(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/api/v1/label/instance/values")
	do(r, http.MethodGet, "/api/v1/label/job/values")

	got := testutil.ToFloat64(m.Requests.WithLabelValues("/api/v1/label/{name}/values", "GET", "200"))
	if got != 2 {
		t.Errorf("requests for the pattern = %v, want 2", got)
	}
	if n := testutil.CollectAndCount(m.Requests); n != 1 {
		t.Errorf("counter has %d series, want 1; the resolved path must not become a label value", n)
	}
}

// A 404 has no route pattern. Recording the raw path there would reopen exactly
// the cardinality hole the pattern label closes, so it must collapse to a
// sentinel.
func TestUnmatchedRouteCollapsesToASentinel(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/nope/1")
	do(r, http.MethodGet, "/nope/2")

	got := testutil.ToFloat64(m.Requests.WithLabelValues(observability.UnmatchedRoute, "GET", "404"))
	if got != 2 {
		t.Errorf("unmatched requests = %v, want 2", got)
	}
}

// net/http accepts any RFC 7230 token as a method, and chi only classifies one
// as invalid from inside ServeHTTP — after this middleware would already have
// recorded it. Several different garbage methods must collapse to ONE series,
// not one each, or a client sending arbitrary method strings grows the registry
// without bound, exactly like the raw-path hole the route label closes above.
//
// chi's own method table only recognizes the nine standard methods, and it
// rejects anything else (405) before it ever resolves a route pattern, so these
// also land on the UnmatchedRoute/405 path — that's incidental to what this
// test pins, which is the method label collapsing to one value.
func TestNonStandardMethodCollapsesToASentinel(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, "EVIL0", "/api/v1/label/instance/values")
	do(r, "EVIL1", "/api/v1/label/instance/values")
	do(r, "EVIL2", "/api/v1/label/instance/values")

	got := testutil.ToFloat64(m.Requests.WithLabelValues(observability.UnmatchedRoute, observability.UnknownMethod, "405"))
	if got != 3 {
		t.Errorf("non-standard-method requests = %v, want 3", got)
	}
	if n := testutil.CollectAndCount(m.Requests); n != 1 {
		t.Errorf("counter has %d series, want 1; non-standard methods must not become label values", n)
	}
}

func TestStatusLabelRecordsTheResponseCode(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/boom")

	if got := testutil.ToFloat64(m.Requests.WithLabelValues("/boom", "GET", "500")); got != 1 {
		t.Errorf("500 counter = %v, want 1", got)
	}
}

// A handler that returns without calling WriteHeader still sends 200. The wrapped
// writer reports status 0 in that case, and recording "0" would put a status code
// that does not exist on the dashboard.
func TestHandlerThatNeverCallsWriteHeaderRecords200(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/silent")

	if got := testutil.ToFloat64(m.Requests.WithLabelValues("/silent", "GET", "200")); got != 1 {
		t.Errorf("silent-handler counter = %v, want 1", got)
	}
}

// Without a defer around the Observe call, a panicking handler never reaches
// it: the request vanishes from telemetry entirely (no count, no duration, no
// access-log line). The middleware must not recover the panic itself — that is
// a separate decision — so this test recovers it locally to observe both that
// the panic still propagates and that the counter still moved.
//
// It also pins the recorded STATUS, not just that some series exists: a
// wrapped writer that never had WriteHeader called reports 0 the same way for
// a panic as for a handler that silently returns, and recording either as 200
// would hide the panic inside a "success" series where the dashboard's 5xx
// expression would never see it.
func TestPanickingHandlerStillRecordsMetrics(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("handler panic did not propagate through the middleware")
			}
		}()
		do(r, http.MethodGet, "/panic")
	}()

	if n := testutil.CollectAndCount(m.Requests); n != 1 {
		t.Errorf("counter has %d series after a panicking handler, want 1; the request must not vanish from telemetry", n)
	}
	if got := testutil.ToFloat64(m.Requests.WithLabelValues("/panic", "GET", "500")); got != 1 {
		t.Errorf("panic counter (status=500) = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Requests.WithLabelValues("/panic", "GET", "200")); got != 0 {
		t.Errorf("panic counter (status=200) = %v, want 0; a panic must never be recorded as a success", got)
	}
}

func TestDurationIsObservedPerRoute(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/boom")

	if n := testutil.CollectAndCount(m.Duration); n != 1 {
		t.Errorf("duration histogram has %d series, want 1", n)
	}

	// CollectAndCount alone would still pass if d.Seconds() became
	// d.Nanoseconds(): the series count is unchanged, only the value explodes
	// into the +Inf bucket, and every latency quantile on the dashboard would
	// silently break. This in-process request takes microseconds, so the
	// observed sum must be a small positive fraction of a second — nanoseconds
	// would instead report a number in the billions.
	reg := prometheus.NewRegistry()
	reg.MustRegister(m.Duration)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var sum float64
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "obs_http_request_duration_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			sum = metric.GetHistogram().GetSampleSum()
			found = true
		}
	}
	if !found {
		t.Fatal("obs_http_request_duration_seconds family not found in Gather output")
	}
	if sum <= 0 || sum >= 1 {
		t.Errorf("observed duration sum = %v seconds, want a small positive fraction of a second (a units bug like d.Nanoseconds() would report a huge value instead)", sum)
	}
}
