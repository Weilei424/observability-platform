package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type seenRequest struct {
	method, path, rawQuery, body, requestID string
}

func recordingUpstream(t *testing.T) (*httptest.Server, func() []seenRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []seenRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, seenRequest{r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Get("X-Request-Id")})
		mu.Unlock()
		if strings.Contains(r.URL.RawQuery, "fail400") {
			http.Error(w, "upstream says no", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []seenRequest { mu.Lock(); defer mu.Unlock(); return append([]seenRequest(nil), seen...) }
}

func gateway(t *testing.T, ingester, querier string, logTo io.Writer) *api.Server {
	t.Helper()
	iu, _ := url.Parse(ingester)
	qu, _ := url.Parse(querier)
	return api.New(api.Deps{
		Config:    &config.Config{DataDir: t.TempDir()},
		Logger:    slog.New(slog.NewJSONHandler(logTo, nil)),
		Ready:     func() error { return nil },
		Upstreams: &api.Upstreams{Ingester: iu, Querier: qu},
	})
}

func routeTable(t *testing.T, r chi.Router) []string {
	t.Helper()
	var out []string
	_ = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		out = append(out, method+" "+strings.TrimSuffix(route, "/"))
		return nil
	})
	slices.Sort(out)
	return out
}

func TestGatewayServesExactlyTheAllInOneRouteTable(t *testing.T) {
	store := metrics.NewMemoryStore()
	aio := api.New(api.Deps{Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(),
		Ingester: store, Engine: metrics.NewQueryEngine(store), LogIngester: logs.NewMemoryStore()})
	gw := gateway(t, "http://127.0.0.1:1", "http://127.0.0.1:1", io.Discard)
	if a, g := routeTable(t, aio.Router()), routeTable(t, gw.Router()); !slices.Equal(a, g) {
		t.Fatalf("route tables differ:\n all-in-one %v\n gateway    %v", a, g)
	}
}

func TestGatewayRoutesByFamilyWithOneRequestID(t *testing.T) {
	ing, ingSeen := recordingUpstream(t)
	qry, qrySeen := recordingUpstream(t)
	var logs bytes.Buffer
	gw := gateway(t, ing.URL, qry.URL, &logs)

	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", strings.NewReader(`{"metrics":[]}`)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("proxied ingest = %d", rec.Code)
	}
	gw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up&time=1", nil))
	gw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))

	i := ingSeen()
	if len(i) != 1 || i[0].path != "/api/v1/ingest/metrics" || i[0].body != `{"metrics":[]}` {
		t.Fatalf("ingester saw %+v", i)
	}
	q := qrySeen()
	if len(q) != 2 || q[0].path != "/api/v1/query" || q[0].rawQuery != "query=up&time=1" || q[1].path != "/loki/api/v1/labels" {
		t.Fatalf("querier saw %+v", q)
	}
	if i[0].requestID == "" || !strings.Contains(logs.String(), `"request_id":"`+i[0].requestID+`"`) {
		t.Fatalf("upstream request ID %q is not the gateway's own; gateway log:\n%s", i[0].requestID, logs.String())
	}
}

func TestGatewayAnswersAnOutageInEachFamilysShape(t *testing.T) {
	gw := gateway(t, "http://127.0.0.1:1", "http://127.0.0.1:1", io.Discard)

	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", strings.NewReader(`{}`)))
	var write map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &write)
	if rec.Code != http.StatusServiceUnavailable || write["error"] != "ingester unavailable" {
		t.Errorf("write outage = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"errorType":"unavailable"`) {
		t.Errorf("Prometheus outage = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query="+url.QueryEscape(`{a="b"}`), nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != "querier unavailable" {
		t.Errorf("Loki outage = %d %q", rec.Code, rec.Body.String())
	}
}

func TestGatewayPassesUpstreamAnswersThrough(t *testing.T) {
	qry, _ := recordingUpstream(t)
	gw := gateway(t, "http://127.0.0.1:1", qry.URL, io.Discard)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?query=fail400", nil))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "upstream says no") {
		t.Fatalf("upstream 400 became %d %q", rec.Code, rec.Body.String())
	}
}

func TestGatewayNeverProxiesInternalRoutes(t *testing.T) {
	ing, ingSeen := recordingUpstream(t)
	qry, qrySeen := recordingUpstream(t)
	gw := gateway(t, ing.URL, qry.URL, io.Discard)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/labels", nil))
	if rec.Code != http.StatusNotFound || len(ingSeen())+len(qrySeen()) != 0 {
		t.Fatalf("/internal via the gateway = %d, upstream requests %d; want 404 and none", rec.Code, len(ingSeen())+len(qrySeen()))
	}
}
