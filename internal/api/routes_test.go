package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

type downMetrics struct{}

func (downMetrics) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, fmt.Errorf("%w: store metrics/select: connection refused", rpc.ErrUnavailable)
}
func (downMetrics) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("%w: store", rpc.ErrUnavailable)
}
func (downMetrics) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("%w: store", rpc.ErrUnavailable)
}

type downLogs struct{}

func (downLogs) SelectStreams(context.Context, []index.Pair, int64, int64) ([]logs.StreamData, error) {
	return nil, fmt.Errorf("%w: store logs/select", rpc.ErrUnavailable)
}
func (downLogs) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("%w: store", rpc.ErrUnavailable)
}
func (downLogs) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("%w: store", rpc.ErrUnavailable)
}

// deadlineMetrics reproduces the rpc client's own deadline shape: an error
// wrapping BOTH rpc.ErrUnavailable and context.DeadlineExceeded together. It
// must still answer 503, never 499 -- only a bare context.Canceled maps to 499.
type deadlineMetrics struct{}

func (deadlineMetrics) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, fmt.Errorf("%w: %w: store metrics/select: deadline exceeded", rpc.ErrUnavailable, context.DeadlineExceeded)
}
func (deadlineMetrics) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("%w: %w", rpc.ErrUnavailable, context.DeadlineExceeded)
}
func (deadlineMetrics) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("%w: %w", rpc.ErrUnavailable, context.DeadlineExceeded)
}

type deadlineLogs struct{}

func (deadlineLogs) SelectStreams(context.Context, []index.Pair, int64, int64) ([]logs.StreamData, error) {
	return nil, fmt.Errorf("%w: %w: store logs/select: deadline exceeded", rpc.ErrUnavailable, context.DeadlineExceeded)
}
func (deadlineLogs) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("%w: %w", rpc.ErrUnavailable, context.DeadlineExceeded)
}
func (deadlineLogs) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("%w: %w", rpc.ErrUnavailable, context.DeadlineExceeded)
}

// canceledMetrics and canceledLogs simulate the query engines honouring
// request cancellation (Task 2): the source returns an error wrapping only
// context.Canceled, never rpc.ErrUnavailable -- a client that went away, not
// an outage.
type canceledMetrics struct{}

func (canceledMetrics) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, fmt.Errorf("metrics/select: %w", context.Canceled)
}
func (canceledMetrics) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("metrics/select: %w", context.Canceled)
}
func (canceledMetrics) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("metrics/select: %w", context.Canceled)
}

type canceledLogs struct{}

func (canceledLogs) SelectStreams(context.Context, []index.Pair, int64, int64) ([]logs.StreamData, error) {
	return nil, fmt.Errorf("logs/select: %w", context.Canceled)
}
func (canceledLogs) SelectLabelNames(context.Context) ([]string, error) {
	return nil, fmt.Errorf("logs/select: %w", context.Canceled)
}
func (canceledLogs) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("logs/select: %w", context.Canceled)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func serve(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(`{}`)))
	return rec
}

func TestRouteSetsServeOnlyTheirRoutes(t *testing.T) {
	store := metrics.NewMemoryStore()
	base := api.Deps{Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Ingester: store,
		Engine: metrics.NewQueryEngine(store), LogIngester: logs.NewMemoryStore()}

	write := base
	write.Routes = api.RoutesWrite
	w := api.New(write)
	if rec := serve(w, http.MethodGet, "/api/v1/query?query=up"); rec.Code != http.StatusNotFound {
		t.Errorf("write-only server answered a query with %d, want 404", rec.Code)
	}
	if rec := serve(w, http.MethodPost, "/api/v1/ingest/metrics"); rec.Code == http.StatusNotFound {
		t.Error("write-only server does not serve ingest")
	}

	read := base
	read.Routes = api.RoutesRead
	r := api.New(read)
	if rec := serve(r, http.MethodPost, "/loki/api/v1/push"); rec.Code != http.StatusNotFound {
		t.Errorf("read-only server answered a push with %d, want 404", rec.Code)
	}

	none := base
	none.Routes = api.RoutesNone
	n := api.New(none)
	for _, path := range []string{"/api/v1/query", "/api/v1/ingest/metrics"} {
		if rec := serve(n, http.MethodGet, path); rec.Code != http.StatusNotFound {
			t.Errorf("RoutesNone served %s with %d", path, rec.Code)
		}
	}
	if rec := serve(n, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("RoutesNone /healthz = %d, want 200", rec.Code)
	}
}

func TestInternalRoutesMountUnderInternalV1WithTheSameMiddleware(t *testing.T) {
	srv := api.New(api.Deps{
		Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Routes: api.RoutesNone,
		Internal: func(r chi.Router) {
			r.Get("/ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("pong")) })
		},
	})
	if rec := serve(srv, http.MethodGet, "/internal/v1/ping"); rec.Code != http.StatusOK || rec.Body.String() != "pong" {
		t.Fatalf("/internal/v1/ping = %d %q", rec.Code, rec.Body.String())
	}
}

func TestReadyOverride(t *testing.T) {
	srv := api.New(api.Deps{
		Config: &config.Config{DataDir: "/nonexistent/for/sure"}, Logger: quiet(), Routes: api.RoutesNone,
		Ready: func() error { return nil },
	})
	if rec := serve(srv, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz with a passing override = %d, want 200 without touching the data dir", rec.Code)
	}
	failing := api.New(api.Deps{
		Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Routes: api.RoutesNone,
		Ready: func() error { return errors.New("not yet") },
	})
	if rec := serve(failing, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with a failing override = %d, want 503", rec.Code)
	}
}

func TestPeerOutagesAnswer503Unavailable(t *testing.T) {
	srv := api.New(api.Deps{
		Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Routes: api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(downMetrics{}),
		LogQuery: logs.NewQueryEngineFromSource(downLogs{}),
	})
	for _, target := range []string{
		"/api/v1/query?query=up&time=1",
		"/api/v1/query_range?query=up&start=1&end=2&step=1",
		"/api/v1/labels",
		"/api/v1/label/job/values",
		"/api/v1/series?match[]=up",
	} {
		rec := serve(srv, http.MethodGet, target)
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"errorType":"unavailable"`) {
			t.Errorf("%s = %d %s, want 503 errorType unavailable", target, rec.Code, rec.Body.String())
		}
	}
	for _, target := range []string{
		"/loki/api/v1/query_range?query=" + url.QueryEscape(`{service="api"}`),
		"/loki/api/v1/query?query=" + url.QueryEscape(`{service="api"}`),
		"/loki/api/v1/query_range?query=" + url.QueryEscape(`count_over_time({service="api"}[1m])`),
		"/loki/api/v1/labels",
		"/loki/api/v1/label/service/values",
	} {
		rec := serve(srv, http.MethodGet, target)
		if rec.Code != http.StatusServiceUnavailable || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
			t.Errorf("%s = %d (%s), want a plain-text 503", target, rec.Code, rec.Header().Get("Content-Type"))
		}
	}
}

// TestDeadlineExceededStillAnswers503 pins the priority the shared evaluation
// error writers must apply: the rpc client wraps BOTH rpc.ErrUnavailable and
// context.DeadlineExceeded for a peer deadline, and that combination is an
// outage (503), never the 499 "client went away" answer -- only a bare
// context.Canceled (never paired with rpc.ErrUnavailable) means 499.
func TestDeadlineExceededStillAnswers503(t *testing.T) {
	srv := api.New(api.Deps{
		Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Routes: api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(deadlineMetrics{}),
		LogQuery: logs.NewQueryEngineFromSource(deadlineLogs{}),
	})
	if rec := serve(srv, http.MethodGet, "/api/v1/query?query=up&time=1"); rec.Code != http.StatusServiceUnavailable ||
		!strings.Contains(rec.Body.String(), `"errorType":"unavailable"`) {
		t.Errorf("prometheus deadline-exceeded query = %d %s, want 503 errorType unavailable", rec.Code, rec.Body.String())
	}
	if rec := serve(srv, http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`{service="api"}`)); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("loki deadline-exceeded query = %d, want 503", rec.Code)
	}
}

// TestCanceledQueryAnswers499WithoutAnErrorLog covers the controller ruling
// (carried from Task 18): a query abandoned by its caller -- Grafana closing a
// dashboard refresh or zoom -- ends with an error wrapping context.Canceled,
// which must answer 499 (Prometheus's own statusClientClosedConnection
// convention) rather than 500, and must not be logged at ERROR: it is not a
// failure, and logging it as one would inflate the self-observability
// dashboard's error rate for something that never failed.
func TestCanceledQueryAnswers499WithoutAnErrorLog(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv := api.New(api.Deps{
		Config: &config.Config{DataDir: t.TempDir()}, Logger: log, Routes: api.RoutesRead,
		Engine:   metrics.NewQueryEngineFromSource(canceledMetrics{}),
		LogQuery: logs.NewQueryEngineFromSource(canceledLogs{}),
	})

	buf.Reset()
	rec := serve(srv, http.MethodGet, "/api/v1/query?query=up&time=1")
	if rec.Code != statusClientClosedConnectionForTest || !strings.Contains(rec.Body.String(), `"errorType":"canceled"`) {
		t.Errorf("prometheus canceled query = %d %s, want 499 errorType canceled", rec.Code, rec.Body.String())
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("prometheus canceled query logged an ERROR line: %s", buf.String())
	}

	buf.Reset()
	rec = serve(srv, http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(`{service="api"}`))
	if rec.Code != statusClientClosedConnectionForTest {
		t.Errorf("loki canceled query = %d, want 499", rec.Code)
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("loki canceled query logged an ERROR line: %s", buf.String())
	}
}

// statusClientClosedConnectionForTest mirrors the unexported
// statusClientClosedConnection in unavailable.go; this file is package
// api_test and cannot see it directly.
const statusClientClosedConnectionForTest = 499
