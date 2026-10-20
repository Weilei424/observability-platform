package app_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// doAs is do acting for tenant id; an empty id sends no tenant header.
func doAs(h http.Handler, id, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if id != "" {
		req.Header.Set(tenant.Header, id)
	}
	h.ServeHTTP(rec, req)
	return rec
}

func buildTarget(t *testing.T, cfg *config.Config) http.Handler {
	t.Helper()
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build(%s): %v", cfg.Target, err)
	}
	t.Cleanup(a.Close)
	return a.Handler
}

func ints(n int) *int { return &n }

// All-in-one answers each tenant from its own data, metrics and logs alike.
func TestAllInOneIsolatesTenants(t *testing.T) {
	h := buildTarget(t, testConfig(t, config.TargetAllInOne))
	for id, v := range map[string]string{"team-a": "1", "team-b": "2"} {
		body := `{"metrics":[{"name":"iso","labels":{"job":"x"},"timestamp_ms":1000,"value":` + v + `}]}`
		if rec := doAs(h, id, http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != http.StatusNoContent {
			t.Fatalf("%s ingest = %d: %s", id, rec.Code, rec.Body)
		}
		line := `{"streams":[{"stream":{"service":"iso"},"values":[["1000000000","line-` + v + `"]]}]}`
		if rec := doAs(h, id, http.MethodPost, "/loki/api/v1/push", line); rec.Code != http.StatusNoContent {
			t.Fatalf("%s push = %d: %s", id, rec.Code, rec.Body)
		}
	}
	for id, v := range map[string]string{"team-a": "1", "team-b": "2"} {
		other := map[string]string{"1": "2", "2": "1"}[v]
		rec := doAs(h, id, http.MethodGet, "/api/v1/query?query=iso&time=1", "")
		body := rec.Body.String()
		if rec.Code != http.StatusOK || !strings.Contains(body, `"`+v+`"]`) || strings.Contains(body, `"`+other+`"]`) {
			t.Errorf("%s query = %d %s, want only its value %s", id, rec.Code, body, v)
		}
		if strings.Contains(body, tenant.Label) {
			t.Errorf("%s query leaks the tenant label: %s", id, body)
		}
		rec = doAs(h, id, http.MethodGet, `/loki/api/v1/query_range?query={service="iso"}&start=0&end=2000000000`, "")
		body = rec.Body.String()
		if rec.Code != http.StatusOK || !strings.Contains(body, "line-"+v) || strings.Contains(body, "line-"+other) {
			t.Errorf("%s query_range = %d %s, want only line-%s", id, rec.Code, body, v)
		}
	}
	// The default tenant wrote nothing and sees nothing.
	if rec := doAs(h, "", http.MethodGet, "/api/v1/query?query=iso&time=1", ""); strings.Contains(rec.Body.String(), `"1"]`) || strings.Contains(rec.Body.String(), `"2"]`) {
		t.Errorf("default tenant sees another tenant's data: %s", rec.Body)
	}
}

// All-in-one enforces each tenant's active series and stream limits as-is,
// counts each refusal, and exports the per-tenant active gauges.
func TestAllInOneEnforcesTenantLimits(t *testing.T) {
	cfg := testConfig(t, config.TargetAllInOne)
	cfg.TenantOverrides = map[string]tenant.Override{"team-b": {MaxActiveSeries: ints(2), MaxActiveStreams: ints(1)}}
	h := buildTarget(t, cfg)
	for i, want := range []int{http.StatusNoContent, http.StatusNoContent, http.StatusTooManyRequests} {
		body := `{"metrics":[{"name":"lim","labels":{"n":"` + string(rune('a'+i)) + `"},"timestamp_ms":1000,"value":1}]}`
		if rec := doAs(h, "team-b", http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != want {
			t.Errorf("series %d = %d, want %d", i, rec.Code, want)
		}
	}
	for i, want := range []int{http.StatusNoContent, http.StatusTooManyRequests} {
		body := `{"streams":[{"stream":{"service":"s` + string(rune('a'+i)) + `"},"values":[["1000000000","x"]]}]}`
		if rec := doAs(h, "team-b", http.MethodPost, "/loki/api/v1/push", body); rec.Code != want {
			t.Errorf("stream %d = %d, want %d", i, rec.Code, want)
		}
	}
	// Another tenant has no limit.
	for i := range 3 {
		body := `{"metrics":[{"name":"lim","labels":{"n":"` + string(rune('a'+i)) + `"},"timestamp_ms":1000,"value":1}]}`
		if rec := doAs(h, "team-a", http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != http.StatusNoContent {
			t.Errorf("team-a series %d = %d, want 204", i, rec.Code)
		}
	}
	m := doAs(h, "", http.MethodGet, "/metrics", "").Body.String()
	for _, want := range []string{
		`obs_tenant_limit_rejections_total{limit="series",tenant="team-b"} 1`,
		`obs_tenant_limit_rejections_total{limit="streams",tenant="team-b"} 1`,
		`obs_tenant_active_series{tenant="team-b"} 2`,
		`obs_tenant_active_series{tenant="team-a"} 3`,
		`obs_tenant_active_streams{tenant="team-b"} 1`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}

// A querier with TenantRequired refuses a public read without the header.
func TestQuerierTenantRequired(t *testing.T) {
	cfg := withPeers(testConfig(t, config.TargetQuerier))
	cfg.TenantRequired = true
	h := buildTarget(t, cfg)
	if rec := doAs(h, "", http.MethodGet, "/api/v1/query?query=x&time=1", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("querier read without a tenant = %d, want 401", rec.Code)
	}
}

// A gateway with TenantRequired refuses a write without the header; its push
// cap is per tenant, and each refusal is counted.
func TestGatewayTenantRequiredAndPushBytes(t *testing.T) {
	cfg := withPeers(testConfig(t, config.TargetGateway))
	cfg.TenantRequired = true
	cfg.TenantOverrides = map[string]tenant.Override{"team-b": {MaxMetricsPushBytes: func() *int64 { n := int64(10); return &n }()}}
	h := buildTarget(t, cfg)
	if rec := doAs(h, "", http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusUnauthorized {
		t.Errorf("gateway write without a tenant = %d, want 401", rec.Code)
	}
	if rec := doAs(h, "team-b", http.MethodPost, "/api/v1/ingest/metrics", oneSample); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("team-b write over its cap = %d, want 413", rec.Code)
	}
	m := doAs(h, "team-b", http.MethodGet, "/metrics", "").Body.String()
	if !strings.Contains(m, `obs_tenant_limit_rejections_total{limit="push_bytes",tenant="team-b"} 1`) {
		t.Errorf("gateway /metrics lacks the push_bytes rejection")
	}
}

// The ingester's internal reads require a tenant; its gates enforce each
// tenant's share of the limit (RF 1 over 2 ingesters: half of 4).
func TestIngesterTenantReadsAndShare(t *testing.T) {
	cfg := withPeers(testConfig(t, config.TargetIngester))
	cfg.ReplicationFactor = 1
	cfg.IngesterURLs = []string{"http://a:1", "http://b:1"}
	cfg.TenantOverrides = map[string]tenant.Override{"team-b": {MaxActiveSeries: ints(4), MaxActiveStreams: ints(2)}}
	h := buildTarget(t, cfg)
	if rec := doAs(h, "", http.MethodPost, "/internal/v1/metrics/select", `{"matchers":[],"min_ms":0,"max_ms":1}`); rec.Code != http.StatusBadRequest {
		t.Errorf("ingester select without a tenant = %d, want 400", rec.Code)
	}
	for i, want := range []int{http.StatusNoContent, http.StatusNoContent, http.StatusTooManyRequests} {
		body := `{"metrics":[{"name":"lim","labels":{"n":"` + string(rune('a'+i)) + `"},"timestamp_ms":1000,"value":1}]}`
		if rec := doAs(h, "team-b", http.MethodPost, "/api/v1/ingest/metrics", body); rec.Code != want {
			t.Errorf("public series %d = %d, want %d", i, rec.Code, want)
		}
	}
	for i, want := range []int{http.StatusNoContent, http.StatusTooManyRequests} {
		body := `{"streams":[{"labels":{"service":"s` + string(rune('a'+i)) + `","__tenant__":"team-b"},"entries":[[1000000000,"x"]]}]}`
		if rec := doAs(h, "team-b", http.MethodPost, "/internal/v1/logs/push", body); rec.Code != want {
			t.Errorf("internal stream %d = %d, want %d: %s", i, rec.Code, want, rec.Body)
		}
	}
	m := doAs(h, "", http.MethodGet, "/metrics", "").Body.String()
	for _, want := range []string{
		`obs_tenant_limit_rejections_total{limit="series",tenant="team-b"} 1`,
		`obs_tenant_limit_rejections_total{limit="streams",tenant="team-b"} 1`,
		`obs_tenant_active_series{tenant="team-b"} 2`,
		`obs_tenant_active_streams{tenant="team-b"} 1`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}
