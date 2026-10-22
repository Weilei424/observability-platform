package api_test

import (
	"context"
	"encoding/json"
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
	"github.com/masonwheeler/observability-platform/internal/limits"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

type memCounter struct{ s *metrics.MemoryStore }

func (c memCounter) Count(t string) int { return c.s.TenantSeries(t) }
func (c memCounter) Has(id uint64) bool { return c.s.HasSeries(metrics.SeriesID(id)) }

func gatedServer(t *testing.T, lim *tenant.Limits) (*api.Server, *metrics.MemoryStore) {
	t.Helper()
	store := metrics.NewMemoryStore()
	gate := limits.NewGate(limits.Series, func(id string) int { return lim.MaxActiveSeries(id) }, memCounter{store}, nil)
	return api.New(api.Deps{
		Config:     &config.Config{HTTPAddr: ":8080", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:     slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Ingester:   store,
		Engine:     metrics.NewQueryEngineFromSource(metrics.TenantSource(store)),
		Tenants:    lim,
		SeriesGate: gate,
	}), store
}

func pushJobs(srv *api.Server, org string, jobs ...string) *httptest.ResponseRecorder {
	var ms []map[string]any
	for _, j := range jobs {
		ms = append(ms, map[string]any{"name": "up", "labels": map[string]string{"job": j}, "timestamp_ms": 1000, "value": 1})
	}
	b, _ := json.Marshal(map[string]any{"metrics": ms})
	return tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", org, b)
}

func seriesLimits(o map[string]int) *tenant.Limits {
	ov := map[string]tenant.Override{}
	for k, v := range o {
		v := v
		ov[k] = tenant.Override{MaxActiveSeries: &v}
	}
	return tenant.NewLimits(tenant.Defaults{MaxMetricsPushBytes: 1 << 20, MaxLogsPushBytes: 4 << 20}, ov)
}

func TestSeriesLimitAnswers429(t *testing.T) {
	lim := seriesLimits(map[string]int{"team-b": 2})
	srv, store := gatedServer(t, lim)
	if rr := pushJobs(srv, "team-b", "a", "b"); rr.Code != http.StatusNoContent {
		t.Fatalf("first: %d %s", rr.Code, rr.Body)
	}
	rr := pushJobs(srv, "team-b", "c")
	if rr.Code != http.StatusTooManyRequests || !strings.Contains(rr.Body.String(), "would exceed") {
		t.Fatalf("third: %d %s", rr.Code, rr.Body)
	}
	if got := store.TenantSeries("team-b"); got != 2 {
		t.Fatalf("series = %d, want 2", got)
	}
	if rr := pushJobs(srv, "team-b", "a", "b"); rr.Code != http.StatusNoContent {
		t.Fatalf("existing: %d %s", rr.Code, rr.Body)
	}
	for i := range 5 {
		if rr := pushJobs(srv, "", fmt.Sprintf("j%d", i)); rr.Code != http.StatusNoContent {
			t.Fatalf("default: %d %s", rr.Code, rr.Body)
		}
	}
}

func TestSeriesLimitAppliesToAnonymous(t *testing.T) {
	lim := seriesLimits(map[string]int{"anonymous": 1})
	srv, _ := gatedServer(t, lim)
	if rr := pushJobs(srv, "", "a", "b"); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("anonymous: %d %s", rr.Code, rr.Body)
	}
}

type limitedWrites struct{}

func (limitedWrites) PushSamples(context.Context, []metrics.PendingSample) error {
	return &rpc.QuorumError{Kind: "series", Ingesters: []string{"http://10.0.0.7:8080"}, Cause: fmt.Errorf("%w: http://10.0.0.7:8080/internal/v1/push: tenant team-b would exceed its active series limit (2)", rpc.ErrLimited)}
}
func (limitedWrites) PushEntries(context.Context, []logs.PendingEntry) error { return nil }

func TestGatewayAnswers429ForLimitedQuorum(t *testing.T) {
	q, _ := url.Parse("http://127.0.0.1:1")
	srv := api.New(api.Deps{
		Config:    &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:    slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Upstreams: &api.Upstreams{Querier: q},
		Writes:    limitedWrites{},
		Tenants:   seriesLimits(map[string]int{"team-b": 7}),
	})
	rr := pushJobs(srv, "team-b", "a")
	want := `{"error":"tenant team-b would exceed its active series limit (7)"}`
	if rr.Code != http.StatusTooManyRequests || strings.TrimSpace(rr.Body.String()) != want {
		t.Fatalf("gateway: %d %s", rr.Code, rr.Body)
	}
	if strings.Contains(rr.Body.String(), "/internal/") || strings.Contains(rr.Body.String(), "10.0.0.7") {
		t.Fatalf("body leaks peer: %s", rr.Body)
	}
}

func TestNamedTenantRefuses255UserLabels(t *testing.T) {
	srv, _ := gatedServer(t, seriesLimits(nil))
	lbls := map[string]string{}
	for i := 0; i < 254; i++ { // plus __name__ = 255 labels
		lbls[fmt.Sprintf("l%03d", i)] = "v"
	}
	body := func() []byte {
		b, _ := json.Marshal(map[string]any{"metrics": []map[string]any{{"name": "up", "labels": lbls, "timestamp_ms": 1000, "value": 1}}})
		return b
	}
	if rr := tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", "team-b", body()); rr.Code != http.StatusBadRequest {
		t.Fatalf("named tenant at 255 labels: %d %s", rr.Code, rr.Body)
	}
	if rr := tenantReq(srv, http.MethodPost, "/api/v1/ingest/metrics", "", body()); rr.Code != http.StatusNoContent {
		t.Fatalf("default tenant at 255 labels: %d %s", rr.Code, rr.Body)
	}
}

func TestNamedTenantRefuses255LogLabels(t *testing.T) {
	srv, _ := newTenantServer(t, tenantOpts{})
	lbls := map[string]string{}
	for i := 0; i < 255; i++ {
		lbls[fmt.Sprintf("l%03d", i)] = "v"
	}
	b, _ := json.Marshal(map[string]any{"streams": []map[string]any{{"stream": lbls, "values": [][]string{{"1700000000000000000", "x"}}}}})
	if rr := tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "team-b", b); rr.Code != http.StatusBadRequest {
		t.Fatalf("named tenant: %d %s", rr.Code, rr.Body)
	}
	if rr := tenantReq(srv, http.MethodPost, "/loki/api/v1/push", "", b); rr.Code != http.StatusNoContent {
		t.Fatalf("default tenant: %d %s", rr.Code, rr.Body)
	}
}
