package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/observability"
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

func TestDurationIsObservedPerRoute(t *testing.T) {
	m := observability.NewHTTPMetrics()
	r := routerWith(m)

	do(r, http.MethodGet, "/boom")

	if n := testutil.CollectAndCount(m.Duration); n != 1 {
		t.Errorf("duration histogram has %d series, want 1", n)
	}
}
