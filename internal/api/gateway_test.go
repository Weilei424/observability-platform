package api_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// syncBuffer is a mutex-guarded bytes.Buffer: several of the tests below have
// one goroutine still writing the gateway's access log while another polls it
// for the line it's waiting on, which a bare bytes.Buffer cannot do without a
// data race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

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

func gateway(t *testing.T, querier string, logTo io.Writer) *api.Server {
	t.Helper()
	qu, _ := url.Parse(querier)
	return api.New(api.Deps{
		Config:    &config.Config{DataDir: t.TempDir()},
		Logger:    slog.New(slog.NewJSONHandler(logTo, nil)),
		Ready:     func() error { return nil },
		Upstreams: &api.Upstreams{Querier: qu},
		Writes:    &fakeRouter{},
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
	gw := gateway(t, "http://127.0.0.1:1", io.Discard)
	if a, g := routeTable(t, aio.Router()), routeTable(t, gw.Router()); !slices.Equal(a, g) {
		t.Fatalf("route tables differ:\n all-in-one %v\n gateway    %v", a, g)
	}
}

func TestGatewayRoutesByFamilyWithOneRequestID(t *testing.T) {
	qry, qrySeen := recordingUpstream(t)
	var logs bytes.Buffer
	gw := gateway(t, qry.URL, &logs)

	gw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up&time=1", nil))
	gw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))

	q := qrySeen()
	if len(q) != 2 || q[0].path != "/api/v1/query" || q[0].rawQuery != "query=up&time=1" || q[1].path != "/loki/api/v1/labels" {
		t.Fatalf("querier saw %+v", q)
	}
	if q[0].requestID == "" || !strings.Contains(logs.String(), `"request_id":"`+q[0].requestID+`"`) {
		t.Fatalf("upstream request ID %q is not the gateway's own; gateway log:\n%s", q[0].requestID, logs.String())
	}
}

func TestGatewayAnswersAnOutageInEachFamilysShape(t *testing.T) {
	gw := gateway(t, "http://127.0.0.1:1", io.Discard)

	rec := httptest.NewRecorder()
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
	gw := gateway(t, qry.URL, io.Discard)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?query=fail400", nil))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "upstream says no") {
		t.Fatalf("upstream 400 became %d %q", rec.Code, rec.Body.String())
	}
}

func TestGatewayNeverProxiesInternalRoutes(t *testing.T) {
	qry, qrySeen := recordingUpstream(t)
	gw := gateway(t, qry.URL, io.Discard)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/labels", nil))
	if rec.Code != http.StatusNotFound || len(qrySeen()) != 0 {
		t.Fatalf("/internal via the gateway = %d, upstream requests %d; want 404 and none", rec.Code, len(qrySeen()))
	}
}

// TestNewPanicsWhenGatewayMisconfigured pins the contract documented on
// Upstreams and Deps.Upstreams: a gateway must serve exactly the full public
// route table (Routes == RoutesAll) and must have both upstream URLs, or New
// panics at construction instead of silently building a gateway that proxies
// only some routes (a RoutesRead or RoutesWrite Deps) or none at all
// (RoutesNone), or that would nil-dereference inside SetURL on its first
// proxied request (a nil Querier URL).
func TestNewPanicsWhenGatewayMisconfigured(t *testing.T) {
	u := func() *url.URL { u, _ := url.Parse("http://127.0.0.1:1"); return u }
	base := func() api.Deps {
		return api.Deps{Config: &config.Config{DataDir: t.TempDir()}, Logger: quiet(), Writes: &fakeRouter{}}
	}

	cases := []struct {
		name string
		d    api.Deps
	}{
		{"RoutesWrite", func() api.Deps {
			d := base()
			d.Routes = api.RoutesWrite
			d.Upstreams = &api.Upstreams{Querier: u()}
			return d
		}()},
		{"RoutesRead", func() api.Deps {
			d := base()
			d.Routes = api.RoutesRead
			d.Upstreams = &api.Upstreams{Querier: u()}
			return d
		}()},
		{"RoutesNone", func() api.Deps {
			d := base()
			d.Routes = api.RoutesNone
			d.Upstreams = &api.Upstreams{Querier: u()}
			return d
		}()},
		{"nil Querier", func() api.Deps {
			d := base()
			d.Upstreams = &api.Upstreams{}
			return d
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("api.New did not panic for %s", tc.name)
				}
			}()
			api.New(tc.d)
		})
	}
}

// TestGatewayClientCancelDuringUpstreamCallIs499 covers the controller ruling
// (finding 1): a caller that leaves while the gateway is still waiting on the
// upstream (Grafana abandoning a dashboard refresh or zoom) must answer 499,
// exactly like the query handlers do for the same thing, and must never be
// logged as "upstream unavailable" -- it is not an outage; the upstream in
// this test is healthy and simply hasn't answered yet.
func TestGatewayClientCancelDuringUpstreamCallIs499(t *testing.T) {
	reachedUpstream := make(chan struct{})
	qry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(reachedUpstream)
		<-r.Context().Done() // held open until the gateway's outbound call is canceled
	}))
	defer qry.Close()

	var logBuf syncBuffer
	gw := gateway(t, qry.URL, &logBuf)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		gw.ServeHTTP(rec, req)
	}()

	select {
	case <-reachedUpstream:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never saw the request")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway never returned after the client canceled")
	}

	if rec.Code != statusClientClosedConnectionForTest {
		t.Fatalf("client cancel during the upstream call = %d, want %d", rec.Code, statusClientClosedConnectionForTest)
	}
	if strings.Contains(logBuf.String(), "upstream unavailable") {
		t.Fatalf("client cancel was logged as an upstream outage: %s", logBuf.String())
	}
}

// TestGatewayMidStreamClientCancelIsNotRecordedAsServerError covers the
// controller's folded-in ruling (finding 3): once the upstream's headers have
// already reached the client, ReverseProxy answers a client that then leaves
// mid-body by panicking with http.ErrAbortHandler instead of calling
// ErrorHandler. Without recovering that panic before chi's access-log and
// metrics middleware see it, both record a 500 for a request that never
// failed -- it was abandoned by a client that had already gotten (partial)
// success. This needs a real listener on both legs: ReverseProxy only takes
// the panicking path when the inbound request carries an *http.Server context
// key, which httptest.NewRecorder()+ServeHTTP never provides.
func TestGatewayMidStreamClientCancelIsNotRecordedAsServerError(t *testing.T) {
	headerSent := make(chan struct{})
	qry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Paced, one byte every 20ms for up to a second: the gateway's copy
		// loop is then reliably sitting in a blocked read waiting for the next
		// byte (not still draining an already-buffered response) at whatever
		// moment the test cancels, rather than racing a single Write to
		// finish before cancellation lands.
		rc := http.NewResponseController(w)
		w.WriteHeader(http.StatusOK)
		var sentHeader sync.Once
		for i := 0; i < 50; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			_ = rc.Flush()
			sentHeader.Do(func() { close(headerSent) })
			select {
			case <-r.Context().Done(): // the client left; stop pretending to stream
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}))
	defer qry.Close()

	var logBuf syncBuffer
	gw := gateway(t, qry.URL, &logBuf)
	gwSrv := httptest.NewServer(gw)
	defer gwSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gwSrv.URL+"/api/v1/query?query=up", nil)
	if err != nil {
		t.Fatal(err)
	}

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	select {
	case <-headerSent:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received the request")
	}
	cancel()
	<-clientDone

	deadline := time.After(2 * time.Second)
	for !strings.Contains(logBuf.String(), `"path":"/api/v1/query"`) {
		select {
		case <-deadline:
			t.Fatalf("gateway never logged the request: %s", logBuf.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if strings.Contains(logBuf.String(), `"status":500`) {
		t.Fatalf("a mid-stream client cancel was recorded as a server error: %s", logBuf.String())
	}
}

// TestGatewayLogsAnUnavailableUpstreamAtError covers finding 6: a real outage
// (dial failure, as opposed to the client leaving) is a genuine failure and
// must be logged at Error under component "gateway", matching Task 19's own
// peer-outage logging convention, not Warn.
func TestGatewayLogsAnUnavailableUpstreamAtError(t *testing.T) {
	var logBuf syncBuffer
	gw := gateway(t, "http://127.0.0.1:1", &logBuf)
	gw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil))
	if !strings.Contains(logBuf.String(), `"level":"ERROR"`) || !strings.Contains(logBuf.String(), "upstream unavailable") {
		t.Fatalf("unreachable upstream was not logged at ERROR under component gateway: %s", logBuf.String())
	}
}

// TestGatewayForwardsRawQueryByteForByte covers the controller ruling (finding
// 4): ReverseProxy silently re-encodes a raw query it cannot fully parse (a
// `;` separator, here) before Rewrite ever runs, which can reorder or drop
// pairs outright. All-in-one parses the query itself and answers 400 for
// exactly this input, so the gateway must forward the client's bytes
// unchanged rather than "fixing" them into something the client didn't send.
func TestGatewayForwardsRawQueryByteForByte(t *testing.T) {
	qry, qrySeen := recordingUpstream(t)
	gw := gateway(t, qry.URL, io.Discard)

	const rawQuery = "query=up&time=1;"
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?"+rawQuery, nil))

	seen := qrySeen()
	if len(seen) != 1 || seen[0].rawQuery != rawQuery {
		t.Fatalf("querier saw raw query %+v, want %q byte-identical", seen, rawQuery)
	}
}

// TestGatewayRoutesEveryRouteAndAnswers503PerFamily covers finding 5(a): a
// table built from chi.Walk over the gateway's own registered route table
// (rather than a hand-picked sample) so a route added to all-in-one and
// picked up here automatically cannot silently go unrouted or answer the
// wrong outage shape. For every method+pattern it asserts both that the
// request reaches the correct upstream (reads to the querier) and that the family's own 503 shape is used
// when that upstream is unreachable.
func TestGatewayRoutesEveryRouteAndAnswers503PerFamily(t *testing.T) {
	probe := gateway(t, "http://127.0.0.1:1", io.Discard)

	type route struct{ method, path string }
	var routes []route
	_ = chi.Walk(probe.Router(), func(method, pattern string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		switch {
		case pattern == "/healthz", pattern == "/readyz", pattern == "/metrics":
			return nil
		case strings.HasPrefix(pattern, "/internal"):
			return nil
		}
		trimmed := strings.TrimSuffix(pattern, "/")
		if trimmed == "/api/v1/ingest/metrics" || trimmed == "/loki/api/v1/push" {
			return nil // writes are validated and routed locally; see gateway_write_test.go
		}
		routes = append(routes, route{method, trimmed})
		return nil
	})
	if len(routes) == 0 {
		t.Fatal("chi.Walk found no data routes to test")
	}

	isLokiRead := func(path string) bool { return strings.HasPrefix(path, "/loki/") }

	for _, rt := range routes {
		rt := rt
		t.Run(rt.method+"_"+rt.path, func(t *testing.T) {
			path := strings.ReplaceAll(rt.path, "{name}", "job")

			// The request reaches the right upstream, and only that one.
			qry, qrySeen := recordingUpstream(t)
			up := gateway(t, qry.URL, io.Discard)
			up.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(rt.method, path, strings.NewReader(`{}`)))
			if len(qrySeen()) != 1 {
				t.Fatalf("%s %s: querier saw %d request(s); want 1", rt.method, path, len(qrySeen()))
			}

			// An unreachable upstream answers 503 in that family's own shape.
			down := gateway(t, "http://127.0.0.1:1", io.Discard)
			rec := httptest.NewRecorder()
			down.ServeHTTP(rec, httptest.NewRequest(rt.method, path, strings.NewReader(`{}`)))
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s %s outage = %d, want 503", rt.method, path, rec.Code)
			}
			switch {
			case isLokiRead(path):
				if rec.Body.String() != "querier unavailable" {
					t.Fatalf("%s %s outage body = %q, want the plain-text querier-unavailable shape", rt.method, path, rec.Body.String())
				}
			default: // a Prometheus-envelope read
				if !strings.Contains(rec.Body.String(), `"errorType":"unavailable"`) {
					t.Fatalf("%s %s outage body = %q, want errorType unavailable", rt.method, path, rec.Body.String())
				}
			}
		})
	}
}

// TestGatewayPassesThroughTheQueriersOwnOutageBody covers finding 5(b): when
// the querier itself is reachable but is the one answering 503 (for example
// because its own upstream, the store, is down), that is an ordinary upstream
// answer like any other status code -- ReverseProxy's ErrorHandler is never
// invoked for it -- and must pass through byte-for-byte, not be replaced by
// the gateway's own synthesized "querier unavailable" shape.
func TestGatewayPassesThroughTheQueriersOwnOutageBody(t *testing.T) {
	const body = `{"status":"error","errorType":"unavailable","error":"store unavailable"}`
	qry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(body))
	}))
	defer qry.Close()

	gw := gateway(t, qry.URL, io.Discard)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != body {
		t.Fatalf("querier's own 503 became %d %q, want %d %q", rec.Code, rec.Body.String(), http.StatusServiceUnavailable, body)
	}
}
