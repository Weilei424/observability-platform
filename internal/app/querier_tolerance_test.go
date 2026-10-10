package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

type emptyLogs struct{}

func (emptyLogs) SelectStreams(context.Context, []index.Pair, int64, int64) ([]logs.StreamData, error) {
	return nil, nil
}
func (emptyLogs) SelectLabelNames(context.Context) ([]string, error) { return nil, nil }
func (emptyLogs) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, nil
}

func readsPeer(t *testing.T) string {
	t.Helper()
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountReads(r, metrics.NewMemoryStore(), emptyLogs{})
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv.URL
}

func downPeer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func labelsStatus(t *testing.T, rf int, ingesters ...string) (int, string) {
	t.Helper()
	return labelsStatusWithStore(t, readsPeer(t), rf, ingesters...)
}

func labelsStatusWithStore(t *testing.T, store string, rf int, ingesters ...string) (int, string) {
	t.Helper()
	cfg := testConfig(t, config.TargetQuerier)
	cfg.IngesterURLs = ingesters
	cfg.StoreURL = store
	cfg.ReplicationFactor = rf
	var buf bytes.Buffer
	a, err := app.Build(cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	rec := do(a.Handler, http.MethodGet, "/api/v1/labels", "")
	return rec.Code, buf.String()
}

func TestQuerierToleratesIngestersUpToQuorum(t *testing.T) {
	up1, up2, down1, down2 := readsPeer(t), readsPeer(t), downPeer(t), downPeer(t)

	code, logged := labelsStatus(t, 3, up1, up2, down1)
	if code != http.StatusOK {
		t.Fatalf("one of three down, rf 3: status %d", code)
	}
	if !strings.Contains(logged, "read answered by replication") || !strings.Contains(logged, `"request_id"`) {
		t.Errorf("no request-scoped replication warn line: %s", logged)
	}
	if code, _ := labelsStatus(t, 3, up1, down1, down2); code != http.StatusServiceUnavailable {
		t.Errorf("two of three down, rf 3: status %d, want 503", code)
	}
	if code, _ := labelsStatus(t, 1, up1, up2, down1); code != http.StatusServiceUnavailable {
		t.Errorf("one down, rf 1: status %d, want 503", code)
	}
}

// readCounters scrapes the querier's obs_querier_ingester_reads_total, keyed
// "<ingester> <outcome>".
func readCounters(t *testing.T, h http.Handler) map[string]string {
	t.Helper()
	rec := do(h, http.MethodGet, "/metrics", "")
	re := regexp.MustCompile(`(?m)^obs_querier_ingester_reads_total\{ingester="([^"]+)",outcome="([^"]+)"\} (\S+)$`)
	out := map[string]string{}
	for _, m := range re.FindAllStringSubmatch(rec.Body.String(), -1) {
		out[m[1]+" "+m[2]] = m[3]
	}
	if len(out) == 0 {
		t.Fatalf("no obs_querier_ingester_reads_total in /metrics:\n%s", rec.Body)
	}
	return out
}

func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

// The read counter records each ingester's outcome: the down one as
// unavailable, the others as ok. A read the caller cancels is not an ingester
// failure and is not counted at all.
func TestQuerierCountsIngesterReadsButNotCancellations(t *testing.T) {
	up1, up2, down := readsPeer(t), readsPeer(t), downPeer(t)
	cfg := testConfig(t, config.TargetQuerier)
	cfg.IngesterURLs = []string{up1, up2, down}
	cfg.StoreURL = readsPeer(t)
	cfg.ReplicationFactor = 3
	a, err := app.Build(cfg, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if rec := do(a.Handler, http.MethodGet, "/api/v1/labels", ""); rec.Code != http.StatusOK {
		t.Fatalf("labels: %d %s", rec.Code, rec.Body)
	}
	got := readCounters(t, a.Handler)
	for _, want := range []struct {
		peer, outcome, value string
	}{
		{up1, "ok", "1"}, {up2, "ok", "1"}, {down, "unavailable", "1"},
		{up1, "error", "0"}, {up2, "error", "0"}, {down, "error", "0"}, {down, "ok", "0"},
	} {
		if key := hostOf(t, want.peer) + " " + want.outcome; got[key] != want.value {
			t.Errorf("reads{%s} = %q, want %s (all: %v)", key, got[key], want.value, got)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/labels", nil).WithContext(ctx)
	a.Handler.ServeHTTP(httptest.NewRecorder(), req)
	after := readCounters(t, a.Handler)
	for key, v := range got {
		if after[key] != v {
			t.Errorf("a cancelled read moved reads{%s} from %s to %s", key, v, after[key])
		}
	}
}

// The replication warning describes a read that succeeded: a read that
// skipped an ingester but then failed at the store answers an error and says
// nothing about being answered by replication.
func TestQuerierWarnsOnlyWhenTheWholeReadSucceeds(t *testing.T) {
	code, logged := labelsStatusWithStore(t, downPeer(t), 3, readsPeer(t), readsPeer(t), downPeer(t))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("store down: status %d, want 503", code)
	}
	if strings.Contains(logged, "read answered by replication") {
		t.Errorf("a failed read logged that replication answered it: %s", logged)
	}
}

// One request, one warning: /api/v1/series reads the sources once per
// match[] selector, and every read skips the same down ingester, but the
// request logs "read answered by replication" once, after it succeeded.
func TestQuerierWarnsOncePerRequest(t *testing.T) {
	cfg := testConfig(t, config.TargetQuerier)
	cfg.IngesterURLs = []string{readsPeer(t), readsPeer(t), downPeer(t)}
	cfg.StoreURL = readsPeer(t)
	cfg.ReplicationFactor = 3
	var buf bytes.Buffer
	a, err := app.Build(cfg, slog.New(slog.NewJSONHandler(&buf, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	q := url.Values{"match[]": {"a", "b"}}
	rec := do(a.Handler, http.MethodGet, "/api/v1/series?"+q.Encode(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("series with one ingester down: %d %s", rec.Code, rec.Body)
	}
	if n := strings.Count(buf.String(), "read answered by replication"); n != 1 {
		t.Errorf("logged %d replication warnings for one request with two selectors, want 1:\n%s", n, buf.String())
	}
}
