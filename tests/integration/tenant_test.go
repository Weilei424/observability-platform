package integration_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// tenantDo sends one request acting for id ("" sends no tenant header) and
// returns the status and body.
func tenantDo(t *testing.T, method, target, id, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if id != "" {
		req.Header.Set(tenant.Header, id)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// isoTenants are the two tenants the isolation tests write as: the default
// tenant (no header) and team-b. tag is a label name only that tenant uses.
var isoTenants = []struct{ id, name, tag, value string }{
	{id: "", name: "default", tag: "only_default", value: "1"},
	{id: "team-b", name: "team-b", tag: "only_team_b", value: "2"},
}

// writeIsolationData has each tenant write the same visible series and stream
// (iso_metric{job="shared"}, {service="iso"}) plus one of its own
// (job=<name>, service=iso-<name>, carrying its tag label), at baseMs.
func writeIsolationData(t *testing.T, baseURL string, baseMs int64) {
	t.Helper()
	for _, tn := range isoTenants {
		body := fmt.Sprintf(`{"metrics":[`+
			`{"name":"iso_metric","labels":{"job":"shared"},"timestamp_ms":%d,"value":%s},`+
			`{"name":"iso_metric","labels":{"job":%q,%q:"x"},"timestamp_ms":%d,"value":%s}]}`,
			baseMs, tn.value, tn.name, tn.tag, baseMs, tn.value)
		if code, b := tenantDo(t, http.MethodPost, baseURL+"/api/v1/ingest/metrics", tn.id, body); code != http.StatusNoContent {
			t.Fatalf("%s ingest = %d: %s", tn.name, code, b)
		}
		ns := baseMs * 1e6
		line := fmt.Sprintf(`{"streams":[`+
			`{"stream":{"service":"iso"},"values":[["%d","line-%s"]]},`+
			`{"stream":{"service":"iso-%s",%q:"x"},"values":[["%d","own-%s"]]}]}`,
			ns, tn.name, tn.name, tn.tag, ns, tn.name)
		if code, b := tenantDo(t, http.MethodPost, baseURL+"/loki/api/v1/push", tn.id, line); code != http.StatusNoContent {
			t.Fatalf("%s push = %d: %s", tn.name, code, b)
		}
	}
}

// assertIsolated checks every metrics and Loki read route answers each tenant
// with only its own data and never the tenant label.
func assertIsolated(t *testing.T, stage, baseURL string, baseMs int64) {
	t.Helper()
	sec := func(ms int64) string { return fmt.Sprintf("%.3f", float64(ms)/1000) }
	startNs, endNs := (baseMs-60_000)*1e6, (baseMs+60_000)*1e6
	for i, tn := range isoTenants {
		other := isoTenants[1-i]
		routes := map[string]string{
			"query":             "/api/v1/query?" + url.Values{"query": {"iso_metric"}, "time": {sec(baseMs)}}.Encode(),
			"query_range":       "/api/v1/query_range?" + url.Values{"query": {"iso_metric"}, "start": {sec(baseMs - 30_000)}, "end": {sec(baseMs + 30_000)}, "step": {"15"}}.Encode(),
			"series":            "/api/v1/series?" + url.Values{"match[]": {"iso_metric"}}.Encode(),
			"labels":            "/api/v1/labels",
			"label values":      "/api/v1/label/job/values",
			"loki query_range":  lokiRange(`{service="iso"}`, startNs, endNs),
			"loki own":          lokiRange(`{service="iso-`+tn.name+`"}`, startNs, endNs),
			"loki other":        lokiRange(`{service="iso-`+other.name+`"}`, startNs, endNs),
			"loki labels":       "/loki/api/v1/labels?" + url.Values{"start": {fmt.Sprint(startNs)}, "end": {fmt.Sprint(endNs)}}.Encode(),
			"loki label values": "/loki/api/v1/label/service/values?" + url.Values{"start": {fmt.Sprint(startNs)}, "end": {fmt.Sprint(endNs)}}.Encode(),
		}
		want := map[string][]string{
			"query":             {`"job":"shared"`, `"job":"` + tn.name + `"`, `"` + tn.value + `"]`},
			"query_range":       {`"job":"shared"`, `"job":"` + tn.name + `"`},
			"series":            {`"job":"shared"`, `"job":"` + tn.name + `"`},
			"labels":            {`"job"`, `"` + tn.tag + `"`},
			"label values":      {`"shared"`, `"` + tn.name + `"`},
			"loki query_range":  {"line-" + tn.name},
			"loki own":          {"own-" + tn.name},
			"loki labels":       {`"service"`, `"` + tn.tag + `"`},
			"loki label values": {`"iso"`, `"iso-` + tn.name + `"`},
		}
		forbid := map[string][]string{
			"query":             {`"job":"` + other.name + `"`, `"` + other.value + `"]`},
			"query_range":       {`"job":"` + other.name + `"`, `"` + other.value + `"]`},
			"series":            {`"job":"` + other.name + `"`},
			"labels":            {`"` + other.tag + `"`},
			"label values":      {`"` + other.name + `"`},
			"loki query_range":  {"line-" + other.name},
			"loki other":        {"own-" + other.name},
			"loki labels":       {`"` + other.tag + `"`},
			"loki label values": {`"iso-` + other.name + `"`},
		}
		for name, path := range routes {
			code, body := tenantDo(t, http.MethodGet, baseURL+path, tn.id, "")
			if code != http.StatusOK {
				t.Errorf("%s: %s %s = %d: %s", stage, tn.name, name, code, body)
				continue
			}
			for _, w := range want[name] {
				if !strings.Contains(body, w) {
					t.Errorf("%s: %s %s lacks %s: %s", stage, tn.name, name, w, body)
				}
			}
			for _, f := range append(forbid[name], tenant.Label) {
				if strings.Contains(body, f) {
					t.Errorf("%s: %s %s shows %s: %s", stage, tn.name, name, f, body)
				}
			}
		}
	}
}

func lokiRange(query string, startNs, endNs int64) string {
	return "/loki/api/v1/query_range?" + url.Values{"query": {query},
		"start": {fmt.Sprint(startNs)}, "end": {fmt.Sprint(endNs)}}.Encode()
}

// TestAllInOneTenantIsolation: two tenants with identical visible series and
// streams read only their own, from the head, from blocks and chunks, and
// after a restart.
func TestAllInOneTenantIsolation(t *testing.T) {
	cfg := &config.Config{
		Target: config.TargetAllInOne, DataDir: t.TempDir(), LogLevel: "info",
		WALSegmentMaxBytes: 1 << 20, WALSyncEveryN: 1, LogsFlushThresholdBytes: 1 << 20,
		MaxMetricsPushBytes: 1 << 20, MaxLogsPushBytes: 4 << 20,
		MaintenanceInterval: time.Hour, FlushInterval: time.Hour, FlushSealedChunks: 1,
		CompactionBaseRange: 2 * time.Hour, CompactionMultiplier: 4, CompactionLevels: 3,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	open := func() (*app.AllInOne, *app.App, *httptest.Server) {
		built, err := app.BuildAllInOne(cfg, log)
		if err != nil {
			t.Fatal(err)
		}
		a := built.App(log)
		return built, a, httptest.NewServer(a.Handler)
	}
	built, a, srv := open()
	base := time.Now().Add(-time.Minute).UnixMilli()
	writeIsolationData(t, srv.URL, base)
	assertIsolated(t, "head", srv.URL, base)

	if err := built.Store.Drain(context.Background()); err != nil {
		t.Fatalf("metrics flush: %v", err)
	}
	if err := built.LogStore.Flush(); err != nil {
		t.Fatalf("logs flush: %v", err)
	}
	if blocks, _ := built.BlockStore.StorageStats(); blocks == 0 {
		t.Fatal("no block after the flush")
	}
	assertIsolated(t, "flushed", srv.URL, base)

	srv.Close()
	a.Close()
	_, a, srv = open()
	t.Cleanup(func() { srv.Close(); a.Close() })
	assertIsolated(t, "restarted", srv.URL, base)
}

// TestSplitTenantIsolation is the same through the split cluster: 3 ingesters
// at RF 3, 4 stores at R_s 3, read and written through the gateway.
func TestSplitTenantIsolation(t *testing.T) {
	c := startClusterStores(t, 3, 3, 4, 3)
	base := time.Now().Add(-time.Minute).UnixMilli()
	writeIsolationData(t, c.gatewayURL, base)
	assertIsolated(t, "heads", c.gatewayURL, base)

	for _, u := range c.ingesterURLs {
		if code, body := postDrain(t, u); code != http.StatusOK {
			t.Fatalf("drain %s = %d %s", u, code, body)
		}
	}
	assertIsolated(t, "stores", c.gatewayURL, base)

	for _, p := range append(append([]*process(nil), c.ingesters...), c.stores...) {
		p.stop()
		p.start()
	}
	httpClient.CloseIdleConnections()
	assertIsolated(t, "restarted", c.gatewayURL, base)
}

// pushSeriesAs pushes one sample of lim_metric{n=<i>} through the gateway
// acting for id and returns the status.
func pushSeriesAs(t *testing.T, c *cluster, id string, i int, ts int64) int {
	t.Helper()
	body := fmt.Sprintf(`{"metrics":[{"name":"lim_metric","labels":{"n":"%d"},"timestamp_ms":%d,"value":1}]}`, i, ts)
	code, _ := tenantDo(t, http.MethodPost, c.gatewayURL+"/api/v1/ingest/metrics", id, body)
	return code
}

// TestSplitSeriesLimitIsEnforced: team-b's limit of 30 at RF 3 over 3
// ingesters is a share of 30 on each. Each ingester's own gate refuses with a
// real 429, which the gateway's router answers as a 429. The cluster-wide
// count is approximate by design, so only its bounds are asserted.
func TestSplitSeriesLimitIsEnforced(t *testing.T) {
	c := startClusterWith(t, 3, 3, 1, 1, func(cfg *config.Config) {
		cfg.TenantOverrides = map[string]tenant.Override{"team-b": {MaxActiveSeries: intPtr(30), MaxActiveStreams: intPtr(3)}}
	})
	ts := time.Now().UnixMilli()
	accepted, refused := 0, 0
	for i := range 40 {
		switch code := pushSeriesAs(t, c, "team-b", i, ts); code {
		case http.StatusNoContent:
			accepted++
		case http.StatusTooManyRequests:
			refused++
		default:
			t.Fatalf("team-b push %d = %d, want 204 or 429", i, code)
		}
	}
	if accepted < 30 || accepted > 40 || refused == 0 {
		t.Errorf("team-b: %d accepted, %d refused; want 30 <= accepted <= 40 and some refused", accepted, refused)
	}
	for i := range 40 {
		if code := pushSeriesAs(t, c, "", i, ts); code != http.StatusNoContent {
			t.Fatalf("default tenant push %d = %d, want 204: another tenant's limit must not apply", i, code)
		}
	}

	// Streams: a limit of 3 (share 3) through the gateway's log route.
	accepted, refused = 0, 0
	for i := range 6 {
		body := fmt.Sprintf(`{"streams":[{"stream":{"service":"lim-%d"},"values":[["%d","x"]]}]}`, i, ts*1e6)
		switch code, b := tenantDo(t, http.MethodPost, c.gatewayURL+"/loki/api/v1/push", "team-b", body); code {
		case http.StatusNoContent:
			accepted++
		case http.StatusTooManyRequests:
			refused++
		default:
			t.Fatalf("team-b log push %d = %d: %s", i, code, b)
		}
	}
	if accepted < 3 || refused == 0 {
		t.Errorf("team-b streams: %d accepted, %d refused; want >= 3 accepted and some refused", accepted, refused)
	}

	for _, p := range c.ingesters {
		m := metricsText(t, "http://"+p.cfg.HTTPAddr)
		for _, want := range []string{
			`obs_tenant_limit_rejections_total{limit="series",tenant="team-b"}`,
			`obs_tenant_active_series{tenant="team-b"} 30`,
			`obs_tenant_active_series{tenant="anonymous"} 40`,
		} {
			if !strings.Contains(m, want) {
				t.Errorf("ingester %s /metrics lacks %s", p.cfg.HTTPAddr, want)
			}
		}
	}
}

func intPtr(n int) *int { return &n }

func metricsText(t *testing.T, baseURL string) string {
	t.Helper()
	_, body := tenantDo(t, http.MethodGet, baseURL+"/metrics", "", "")
	return body
}

// selectAs reads name from one component's internal select route acting for
// id, over all time.
func selectAs(t *testing.T, baseURL, id, name string) []metrics.SeriesData {
	t.Helper()
	cl, err := rpc.NewClient("peer", baseURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewMetricsSource(cl).Select(tenant.Inject(context.Background(), id), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: name}, MinT: math.MinInt64, MaxT: math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sds
}

// streamsAs reads service=<service> from one component's internal stream
// select acting for id, over all time.
func streamsAs(t *testing.T, baseURL, id, service string) []logs.StreamData {
	t.Helper()
	cl, err := rpc.NewClient("peer", baseURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewLogsSource(cl).SelectStreams(tenant.Inject(context.Background(), id),
		[]index.Pair{{Name: "service", Value: service}}, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	return sds
}

// TestSplitRetentionPerTenant: old data for both tenants is flushed to the
// stores; the compactor's maintenance pass, with a policy that expires only
// team-b (1h; the default keeps everything), deletes team-b's blocks and
// chunks from every store and keeps the default tenant's.
func TestSplitRetentionPerTenant(t *testing.T) {
	c := startClusterWith(t, 3, 3, 4, 3, func(cfg *config.Config) {
		if cfg.Target == config.TargetCompactor {
			cfg.Retention = 0
			cfg.TenantOverrides = map[string]tenant.Override{"team-b": {Retention: durPtr(time.Hour)}}
		}
	})
	old := time.Now().Add(-3 * time.Hour).UnixMilli()
	for _, id := range []string{"", "team-b"} {
		body := fmt.Sprintf(`{"metrics":[{"name":"old_metric","labels":{"job":"r"},"timestamp_ms":%d,"value":7}]}`, old)
		if code, b := tenantDo(t, http.MethodPost, c.gatewayURL+"/api/v1/ingest/metrics", id, body); code != http.StatusNoContent {
			t.Fatalf("ingest as %q = %d: %s", id, code, b)
		}
		line := fmt.Sprintf(`{"streams":[{"stream":{"service":"old"},"values":[["%d","old line"]]}]}`, old*1e6)
		if code, b := tenantDo(t, http.MethodPost, c.gatewayURL+"/loki/api/v1/push", id, line); code != http.StatusNoContent {
			t.Fatalf("push as %q = %d: %s", id, code, b)
		}
	}
	for _, u := range c.ingesterURLs {
		if code, body := postDrain(t, u); code != http.StatusOK {
			t.Fatalf("drain %s = %d %s", u, code, body)
		}
	}
	held := func(id string) (series, streams int) {
		for _, u := range c.storeURLs {
			series += len(selectAs(t, u, id, "old_metric"))
			streams += len(streamsAs(t, u, id, "old"))
		}
		return series, streams
	}
	eventually(t, "team-b's expired data leaves every store", func() bool {
		s, l := held("team-b")
		return s == 0 && l == 0
	})
	// A few more passes: the default tenant's data must survive them.
	time.Sleep(5 * c.compactor.cfg.MaintenanceInterval)
	if s, l := held(tenant.Default); s != 3 || l != 3 {
		t.Errorf("default tenant: %d series and %d streams across the stores, want 3 and 3 (R_s 3)", s, l)
	}
	if s, l := held("team-b"); s != 0 || l != 0 {
		t.Errorf("team-b: %d series and %d streams left, want none", s, l)
	}
}

func durPtr(d time.Duration) *time.Duration { return &d }

// TestTenantRequired: with OBS_TENANT_REQUIRED on the gateway, a write or
// read without the header is a 401 and an invalid tenant a 400; with it, the
// request succeeds and the samples the router sends each ingester carry the
// stamped tenant.
func TestTenantRequired(t *testing.T) {
	c := startClusterWith(t, 3, 3, 1, 1, func(cfg *config.Config) {
		if cfg.Target == config.TargetGateway {
			cfg.TenantRequired = true
		}
	})
	ts := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"metrics":[{"name":"req_metric","labels":{},"timestamp_ms":%d,"value":5}]}`, ts)
	line := fmt.Sprintf(`{"streams":[{"stream":{"service":"req"},"values":[["%d","x"]]}]}`, ts*1e6)
	query := c.gatewayURL + "/api/v1/query?" + url.Values{"query": {"req_metric"}, "time": {fmt.Sprintf("%.3f", float64(ts)/1000)}}.Encode()

	for _, tc := range []struct {
		method, target, body string
	}{
		{http.MethodPost, c.gatewayURL + "/api/v1/ingest/metrics", body},
		{http.MethodPost, c.gatewayURL + "/loki/api/v1/push", line},
		{http.MethodGet, query, ""},
	} {
		if code, b := tenantDo(t, tc.method, tc.target, "", tc.body); code != http.StatusUnauthorized {
			t.Errorf("%s %s without a tenant = %d, want 401: %s", tc.method, tc.target, code, b)
		}
		if code, b := tenantDo(t, tc.method, tc.target, "bad tenant", tc.body); code != http.StatusBadRequest {
			t.Errorf("%s %s with an invalid tenant = %d, want 400: %s", tc.method, tc.target, code, b)
		}
	}
	if code, b := tenantDo(t, http.MethodPost, c.gatewayURL+"/api/v1/ingest/metrics", "team-b", body); code != http.StatusNoContent {
		t.Fatalf("ingest with a tenant = %d: %s", code, b)
	}
	if code, b := tenantDo(t, http.MethodPost, c.gatewayURL+"/loki/api/v1/push", "team-b", line); code != http.StatusNoContent {
		t.Fatalf("push with a tenant = %d: %s", code, b)
	}
	if code, b := tenantDo(t, http.MethodGet, query, "team-b", ""); code != http.StatusOK || !strings.Contains(b, `"5"]`) {
		t.Errorf("query with a tenant = %d %s, want 200 with the sample", code, b)
	}
	// Every replica holds the sample under team-b, none under the default.
	for _, u := range c.ingesterURLs {
		if got := selectAs(t, u, "team-b", "req_metric"); len(got) != 1 {
			t.Errorf("ingester %s holds %d team-b series, want 1", u, len(got))
		}
		if got := selectAs(t, u, tenant.Default, "req_metric"); len(got) != 0 {
			t.Errorf("ingester %s holds the sample under the default tenant", u)
		}
		if got := streamsAs(t, u, "team-b", "req"); len(got) != 1 {
			t.Errorf("ingester %s holds %d team-b streams, want 1", u, len(got))
		}
	}
}
