package api_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeRouter struct {
	err     error
	samples []metrics.PendingSample
	entries []logs.PendingEntry
}

func (f *fakeRouter) PushSamples(_ context.Context, s []metrics.PendingSample) error {
	f.samples = append(f.samples, s...)
	return f.err
}
func (f *fakeRouter) PushEntries(_ context.Context, e []logs.PendingEntry) error {
	f.entries = append(f.entries, e...)
	return f.err
}

func newRoutingGateway(t *testing.T, r api.WriteRouter) (*api.Server, *observability.IngestMetrics) {
	t.Helper()
	q, _ := url.Parse("http://127.0.0.1:1") // reads are not exercised here
	im := observability.NewIngestMetrics()
	return api.New(api.Deps{
		Config:    &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:    slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Ingest:    im,
		Upstreams: &api.Upstreams{Querier: q},
		Writes:    r,
	}), im
}

func post(t *testing.T, srv http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

const okMetrics = `{"metrics":[{"name":"m","labels":{"job":"x"},"timestamp_ms":1,"value":2}]}`
const okLogs = `{"streams":[{"stream":{"service":"api"},"values":[["10","line"]]}]}`

func TestGatewayRoutesValidWrites(t *testing.T) {
	fr := &fakeRouter{}
	srv, im := newRoutingGateway(t, fr)
	if rr := post(t, srv, "/api/v1/ingest/metrics", okMetrics); rr.Code != http.StatusNoContent {
		t.Fatalf("metrics = %d %s", rr.Code, rr.Body)
	}
	if rr := post(t, srv, "/loki/api/v1/push", okLogs); rr.Code != http.StatusNoContent {
		t.Fatalf("logs = %d %s", rr.Code, rr.Body)
	}
	if len(fr.samples) != 1 || fr.samples[0].Value != 2 || len(fr.entries) != 1 || fr.entries[0].Line != "line" {
		t.Fatalf("routed %+v / %+v", fr.samples, fr.entries)
	}
	if testutil.ToFloat64(im.SamplesIngested) != 0 || testutil.ToFloat64(im.LogLinesIngested) != 0 {
		t.Error("the gateway counted ingested writes; the ingesters count those")
	}
}

// Invalid input is refused by the gateway exactly as all-in-one refuses it, and
// never reaches the router.
func TestGatewayValidationMatchesAllInOne(t *testing.T) {
	fr := &fakeRouter{}
	gw, _ := newRoutingGateway(t, fr)
	aio, _ := newQueryTestServer(t) // all-in-one handlers over a MemoryStore
	for _, tc := range []struct{ path, body string }{
		{"/api/v1/ingest/metrics", `{"metrics":[{"name":"","timestamp_ms":1,"value":1}]}`},
		{"/api/v1/ingest/metrics", `{"metrics":[]}`},
		{"/api/v1/ingest/metrics", `{"metrics":[{"name":"m","value":1}]}`},
		{"/loki/api/v1/push", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`},
		{"/loki/api/v1/push", `{"streams":[{"stream":{"a":"b"},"values":[["nope","x"]]}]}`},
	} {
		g, a := post(t, gw, tc.path, tc.body), post(t, aio, tc.path, tc.body)
		if g.Code != a.Code || g.Body.String() != a.Body.String() {
			t.Errorf("%s %s:\n gateway   %d %s\n all-in-one %d %s", tc.path, tc.body, g.Code, g.Body, a.Code, a.Body)
		}
	}
	if len(fr.samples)+len(fr.entries) != 0 {
		t.Error("an invalid batch reached the router")
	}
}

func TestGatewayWriteOutcomes(t *testing.T) {
	for _, tc := range []struct {
		err     error
		code    int
		bodyHas string
	}{
		{fmt.Errorf("%w: ingester x", rpc.ErrUnavailable), http.StatusServiceUnavailable, `"ingester unavailable"`},
		{errors.New("rpc: ingester x answered 400"), http.StatusInternalServerError, `"internal error"`},
		{context.Canceled, 499, ""},
	} {
		srv, _ := newRoutingGateway(t, &fakeRouter{err: tc.err})
		for _, w := range []struct{ path, body string }{{"/api/v1/ingest/metrics", okMetrics}, {"/loki/api/v1/push", okLogs}} {
			rr := post(t, srv, w.path, w.body)
			if rr.Code != tc.code || !strings.Contains(rr.Body.String(), tc.bodyHas) {
				t.Errorf("%v on %s: %d %s, want %d containing %s", tc.err, w.path, rr.Code, rr.Body, tc.code, tc.bodyHas)
			}
		}
	}
}

func TestGatewayRequiresWrites(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a gateway without Writes did not panic")
		}
	}()
	q, _ := url.Parse("http://127.0.0.1:1")
	api.New(api.Deps{Config: &config.Config{}, Logger: slog.Default(), Upstreams: &api.Upstreams{Querier: q}})
}
