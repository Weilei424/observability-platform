package api_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

type tenantOpts struct {
	required bool
	limits   *tenant.Limits
	onReject func(tenant, limit string)
}

func newTenantServer(t *testing.T, o tenantOpts) (*api.Server, *metrics.MemoryStore) {
	t.Helper()
	store := metrics.NewMemoryStore()
	dir := t.TempDir()
	logStore, err := logs.NewStore(filepath.Join(dir, "wal"), filepath.Join(dir, "chunks"), filepath.Join(dir, "index"), 1<<20, 1, 8<<20)
	if err != nil {
		t.Fatalf("logs store: %v", err)
	}
	t.Cleanup(func() { _ = logStore.Close() })
	return api.New(api.Deps{
		Config:         &config.Config{HTTPAddr: ":8080", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:         slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Ingester:       store,
		Engine:         metrics.NewQueryEngineFromSource(metrics.TenantSource(store)),
		LogIngester:    logStore,
		LogQuery:       logs.NewQueryEngineFromSource(logs.TenantSource(logStore)),
		Tenants:        o.limits,
		TenantRequired: o.required,
		OnTenantReject: o.onReject,
	}), store
}

func tenantReq(srv *api.Server, method, target, org string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if org != "" {
		req.Header.Set(tenant.Header, org)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func pushUp(srv *api.Server, org, job string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]any{"metrics": []map[string]any{
		{"name": "up", "labels": map[string]string{"job": job}, "timestamp_ms": 1000, "value": 1},
	}})
	return tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", org, b)
}

func TestPublicRoutesResolveTheTenant(t *testing.T) {
	srv, _ := newTenantServer(t, tenantOpts{required: true})

	rr := tenantReq(srv, http.MethodGet, "/api/v1/query?query=up", "", nil)
	if rr.Code != http.StatusUnauthorized || !strings.Contains(rr.Body.String(), `"status":"error"`) {
		t.Fatalf("prom: %d %s", rr.Code, rr.Body)
	}
	rr = tenantReq(srv, http.MethodGet, "/loki/api/v1/labels", "", nil)
	if rr.Code != http.StatusUnauthorized || strings.HasPrefix(rr.Body.String(), "{") {
		t.Fatalf("loki: %d %s", rr.Code, rr.Body)
	}
	rr = pushUp(srv, "", "a")
	var e map[string]string
	if rr.Code != http.StatusUnauthorized || json.Unmarshal(rr.Body.Bytes(), &e) != nil || e["error"] == "" {
		t.Fatalf("write: %d %s", rr.Code, rr.Body)
	}
	rr = tenantReq(srv, http.MethodGet, "/api/v1/query?query=up", "a|b", nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("a|b: %d %s", rr.Code, rr.Body)
	}
	if rr = tenantReq(srv, http.MethodGet, "/healthz", "", nil); rr.Code != http.StatusOK {
		t.Fatalf("healthz must not need a tenant: %d", rr.Code)
	}
}

func TestWritesStampTheTenant(t *testing.T) {
	srv, store := newTenantServer(t, tenantOpts{})
	if rr := pushUp(srv, "team-b", "a"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body)
	}
	if rr := pushUp(srv, "", "plain"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body)
	}
	sel, _ := metrics.ParseSelector("up")
	got, err := store.SelectSeries(sel)
	if err != nil || len(got) != 2 {
		t.Fatalf("series: %v %v", got, err)
	}
	owners := map[string]string{}
	for _, ms := range got {
		job, _ := ms.Labels.Get("job")
		owners[job] = labels.TenantOf(ms.Labels)
		if _, has := ms.Labels.Get(tenant.Label); has != (job == "a") {
			t.Fatalf("job %s: tenant label presence wrong", job)
		}
	}
	if owners["a"] != "team-b" || owners["plain"] != tenant.Default {
		t.Fatalf("owners = %v", owners)
	}
}

func TestLogsPushStampsTheTenant(t *testing.T) {
	srv, _ := newTenantServer(t, tenantOpts{})
	body := []byte(`{"streams":[{"stream":{"svc":"x"},"values":[["1000000000","hello"]]}]}`)
	if rr := tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "team-b", body); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body)
	}
	if rr := tenantReq(srv, http.MethodGet, "/loki/api/v1/labels", "", nil); strings.Contains(rr.Body.String(), "svc") {
		t.Fatalf("default tenant sees team-b: %s", rr.Body)
	}
	if rr := tenantReq(srv, http.MethodGet, "/loki/api/v1/labels", "team-b", nil); !strings.Contains(rr.Body.String(), "svc") || strings.Contains(rr.Body.String(), tenant.Label) {
		t.Fatalf("team-b labels: %s", rr.Body)
	}
}

func TestPushCapIsPerTenant(t *testing.T) {
	small := int64(64)
	limits := tenant.NewLimits(tenant.Defaults{MaxMetricsPushBytes: 1 << 20, MaxLogsPushBytes: 4 << 20},
		map[string]tenant.Override{"team-b": {MaxMetricsPushBytes: &small, MaxLogsPushBytes: &small}})
	type rej struct{ tenant, limit string }
	var rejected []rej
	srv, _ := newTenantServer(t, tenantOpts{limits: limits, onReject: func(tn, l string) { rejected = append(rejected, rej{tn, l}) }})

	big, _ := json.Marshal(map[string]any{"metrics": []map[string]any{
		{"name": "up", "labels": map[string]string{"job": strings.Repeat("x", 150)}, "timestamp_ms": 1000, "value": 1},
	}})
	if len(big) < 200 {
		t.Fatalf("fixture too small: %d", len(big))
	}
	rr := tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", "team-b", big)
	if rr.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rr.Body.String(), "team-b") {
		t.Fatalf("team-b: %d %s", rr.Code, rr.Body)
	}
	if rr = tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", "", big); rr.Code != http.StatusNoContent {
		t.Fatalf("default: %d %s", rr.Code, rr.Body)
	}
	logBody := []byte(`{"streams":[{"stream":{"svc":"` + strings.Repeat("x", 100) + `"},"values":[["1000000000","hello"]]}]}`)
	if rr = tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "team-b", logBody); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("logs team-b: %d %s", rr.Code, rr.Body)
	}
	if rr = tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "", logBody); rr.Code != http.StatusNoContent {
		t.Fatalf("logs default: %d %s", rr.Code, rr.Body)
	}
	if len(rejected) != 2 || rejected[0] != (rej{"team-b", "push_bytes"}) {
		t.Fatalf("rejected = %v", rejected)
	}
}

func TestTenantAIsolatedFromTenantBOnEveryReadRoute(t *testing.T) {
	srv, _ := newTenantServer(t, tenantOpts{})
	if rr := pushUp(srv, "", "jobdefault"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if rr := pushUp(srv, "team-b", "jobteamb"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	routes := []string{
		"/api/v1/query?query=up&time=1",
		"/api/v1/query_range?query=up&start=0&end=2&step=1",
		"/api/v1/series?match[]=up&start=0&end=2",
		"/api/v1/labels",
		"/api/v1/label/job/values",
	}
	check := func(org, mine, theirs string) {
		for _, p := range routes {
			rr := tenantReq(srv, http.MethodGet, p, org, nil)
			body := rr.Body.String()
			if rr.Code != 200 || strings.Contains(body, theirs) || strings.Contains(body, tenant.Label) {
				t.Fatalf("%s as %q leaks: %d %s", p, org, rr.Code, body)
			}
			if strings.Contains(p, "label/job") || strings.Contains(p, "query") || strings.Contains(p, "series") {
				if !strings.Contains(body, mine) {
					t.Fatalf("%s as %q lost its own data: %s", p, org, body)
				}
			}
		}
	}
	check("", "jobdefault", "jobteamb")
	check("team-b", "jobteamb", "jobdefault")
}

// Without a tenant resolver the push caps are the fixed safety caps, not a
// tenant's limit: a refusal is still a 413, but it is not counted as a
// per-tenant limit rejection.
func TestFixedPushCapIsNotATenantRejection(t *testing.T) {
	var rejected []string
	srv, _ := newTenantServer(t, tenantOpts{onReject: func(tn, l string) { rejected = append(rejected, tn+"/"+l) }})
	huge := []byte(`{"metrics":[]` + strings.Repeat(" ", 5<<20) + `}`)
	if rr := tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", "team-b", huge); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("metrics over the fixed cap: %d %s", rr.Code, rr.Body)
	}
	hugeLogs := []byte(`{"streams":[]` + strings.Repeat(" ", 5<<20) + `}`)
	if rr := tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "team-b", hugeLogs); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("logs over the fixed cap: %d %s", rr.Code, rr.Body)
	}
	if len(rejected) != 0 {
		t.Fatalf("fixed-cap refusals counted as tenant rejections: %v", rejected)
	}
}
