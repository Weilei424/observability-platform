package app_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// storePeer is a store server holding the series "stored_series" at t=1s.
func storePeer(t *testing.T) *httptest.Server {
	t.Helper()
	ms := metrics.NewMemoryStore()
	lbls, err := metrics.NewLabels(map[string]string{"__name__": "stored_series"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Append(lbls, 1000, 7); err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { rpc.MountReads(r, ms, emptyLogs{}) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func TestQuerierSkipsUpToQuorumMinusOneStores(t *testing.T) {
	stores := []*httptest.Server{storePeer(t), storePeer(t), storePeer(t), storePeer(t)}
	urls := make([]string, len(stores))
	for i, s := range stores {
		urls[i] = s.URL
	}
	cfg := testConfig(t, config.TargetQuerier)
	cfg.IngesterURLs = []string{readsPeer(t)}
	cfg.StoreURLs = urls
	cfg.StoreReplicationFactor = 3
	var buf bytes.Buffer
	a, err := app.Build(cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	const q = "/api/v1/query?query=stored_series&time=1"

	stores[0].Close()
	rec := do(a.Handler, http.MethodGet, q, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "stored_series") {
		t.Fatalf("one store down: %d %s", rec.Code, rec.Body)
	}
	if !strings.Contains(buf.String(), "read answered by replication") || !strings.Contains(buf.String(), "store ") {
		t.Errorf("replication note does not name the store: %s", buf.String())
	}

	stores[1].Close()
	if rec := do(a.Handler, http.MethodGet, q, ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("two stores down: %d, want 503", rec.Code)
	}

	metricsBody := do(a.Handler, http.MethodGet, "/metrics", "").Body.String()
	re := regexp.MustCompile(`(?m)^obs_querier_store_reads_total\{outcome="unavailable",store="[^"]+"\} (\S+)$`)
	got := 0
	for _, m := range re.FindAllStringSubmatch(metricsBody, -1) {
		if m[1] != "0" {
			got++
		}
	}
	if got != 2 {
		t.Errorf("unavailable store counters = %d, want 2:\n%s", got, metricsBody)
	}
}
