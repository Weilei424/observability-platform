package rpc_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

func TestClientSendsTheContextTenant(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get(tenant.Header))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"series":[]}`))
	}))
	t.Cleanup(srv.Close)
	c := client(t, srv.URL)
	src := rpc.NewMetricsSource(c)
	p := metrics.SelectParams{Selector: metrics.Selector{MetricName: "up"}, MaxT: 10}
	if _, err := src.Select(tenant.Inject(context.Background(), "team-b"), p); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Select(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "team-b" || got[1] != tenant.Default {
		t.Fatalf("headers = %q, want [team-b anonymous]", got)
	}
}

func tenantReadsServer(t *testing.T) *httptest.Server {
	t.Helper()
	ms := metrics.NewMemoryStore()
	for id, v := range map[string]float64{tenant.Default: 1, "team-b": 2} {
		l, _ := metrics.NewLabels(map[string]string{"__name__": "up", "job": "a"})
		if err := ms.Append(labels.WithTenant(l, id), 1000, v); err != nil {
			t.Fatal(err)
		}
	}
	head, err := logs.OpenHead(filepath.Join(t.TempDir(), "wal"), 1<<20, 1, 1<<30, nil, logs.HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	if err := head.Append(labels.WithTenant(sl, "team-b"), 1, "b-line"); err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountReads(r, ms, head)
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func TestInternalReadsRefuseAMissingTenant(t *testing.T) {
	srv := tenantReadsServer(t)
	for path, in := range map[string]string{
		"/metrics/select": `{"matchers":[],"min_ms":0,"max_ms":10}`,
		"/logs/select":    `{"matchers":[],"min_ns":0,"max_ns":10}`,
	} {
		resp, body := postNoTenant(t, srv.URL+"/internal/v1"+path, in)
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "missing tenant") {
			t.Errorf("POST %s = %d %s, want 400 missing tenant", path, resp.StatusCode, body)
		}
	}
	for _, path := range []string{"/metrics/labels", "/logs/labels", "/metrics/label-values?name=job"} {
		resp, err := http.Get(srv.URL + "/internal/v1" + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s = %d, want 400", path, resp.StatusCode)
		}
	}

	c := client(t, srv.URL)
	ctx := tenant.Inject(context.Background(), "team-b")
	sds, err := rpc.NewMetricsSource(c).Select(ctx, metrics.SelectParams{
		Selector: metrics.Selector{MetricName: "up"}, MinT: 0, MaxT: 2000,
	})
	if err != nil || len(sds) != 1 || sds[0].Samples[0].Value != 2 {
		t.Fatalf("team-b Select = %+v, %v", sds, err)
	}
	if _, ok := sds[0].Labels.Get(tenant.Label); ok {
		t.Fatalf("labels %v leak the tenant label", sds[0].Labels.Map())
	}
	lsd, err := rpc.NewLogsSource(c).SelectStreams(ctx, []index.Pair{{Name: "service", Value: "api"}}, 0, 10)
	if err != nil || len(lsd) != 1 {
		t.Fatalf("team-b SelectStreams = %+v, %v", lsd, err)
	}
	if _, ok := lsd[0].Labels.Get(tenant.Label); ok {
		t.Fatalf("stream labels leak the tenant label")
	}
	other, err := rpc.NewLogsSource(c).SelectStreams(context.Background(), []index.Pair{{Name: "service", Value: "api"}}, 0, 10)
	if err != nil || len(other) != 0 {
		t.Fatalf("default tenant saw team-b logs: %+v, %v", other, err)
	}
}

func TestInternalPushRefusesAMissingTenant(t *testing.T) {
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountWrites(r, nil, nil, observability.NewIngestMetrics(), nil, nil)
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	for _, path := range []string{"/metrics/push", "/logs/push"} {
		resp, body := postNoTenant(t, srv.URL+"/internal/v1"+path, `{}`)
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "missing tenant") {
			t.Errorf("POST %s = %d %s, want 400 missing tenant", path, resp.StatusCode, body)
		}
	}
}

func TestFlushRoutesNeedNoTenant(t *testing.T) {
	f := newStoreFixture(t)
	body := fmt.Sprintf(`{"series":[{"labels":{"__name__":"a"},"chunks":[%q]}]}`, sealedChunkBytes(t, 0))
	resp, out := post(t, f.srv.URL+"/internal/v1/metrics/flush", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("flush without a tenant = %d %s, want 200", resp.StatusCode, out)
	}
}

// postNoTenant is a raw POST without X-Scope-OrgID, as a buggy peer sends it.
func postNoTenant(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}
