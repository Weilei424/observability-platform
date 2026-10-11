package app_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
)

// flushStore answers store flushes with the given status (200 with a valid
// body when 0) and counts the metrics flushes it received.
type flushStore struct {
	url string
	mu  sync.Mutex
	got int
}

func (f *flushStore) flushes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got
}

func startFlushStores(t *testing.T, status ...int) []*flushStore {
	t.Helper()
	out := make([]*flushStore, len(status))
	for i, code := range status {
		f := &flushStore{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if strings.HasSuffix(r.URL.Path, "metrics/flush") {
				f.mu.Lock()
				f.got++
				f.mu.Unlock()
			}
			if code != 0 {
				w.WriteHeader(code)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "metrics/flush") {
				_, _ = io.WriteString(w, `{"block_id":"b","series":1,"samples":1}`)
			} else {
				_, _ = io.WriteString(w, `{"streams":1,"entries":1}`)
			}
		}))
		t.Cleanup(srv.Close)
		f.url = srv.URL
		out[i] = f
	}
	return out
}

// drainThroughStores pushes 130 samples of one series (enough to seal a chunk)
// into an ingester replicating to the stores with factor 3, then drains it.
func drainThroughStores(t *testing.T, stores []*flushStore) (int, string) {
	t.Helper()
	cfg := testConfig(t, config.TargetIngester)
	for _, s := range stores {
		cfg.StoreURLs = append(cfg.StoreURLs, s.url)
	}
	cfg.StoreURL = stores[0].url
	cfg.StoreReplicationFactor = 3
	cfg.StoreRetryMaxBytes = 1 << 20
	cfg.StoreRetryMaxAge = time.Hour
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(a.Close)
	var b strings.Builder
	b.WriteString(`{"metrics":[`)
	for i := 0; i < 130; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"name":"fan","labels":{},"timestamp_ms":%d,"value":%d}`, 1000+i*1000, i)
	}
	b.WriteString(`]}`)
	if rec := do(a.Handler, http.MethodPost, "/api/v1/ingest/metrics", b.String()); rec.Code != http.StatusNoContent {
		t.Fatalf("push = %d, want 204: %s", rec.Code, rec.Body)
	}
	rec := do(a.Handler, http.MethodPost, "/internal/v1/drain", "")
	return rec.Code, do(a.Handler, http.MethodGet, "/metrics", "").Body.String()
}

func TestIngesterDrainMeetsStoreQuorumWithOneStoreDown(t *testing.T) {
	stores := startFlushStores(t, 0, 0, http.StatusServiceUnavailable)
	code, metricsBody := drainThroughStores(t, stores)
	if code != http.StatusOK {
		t.Fatalf("drain = %d, want 200 (quorum met with one store down)", code)
	}
	for i, s := range stores[:2] {
		if s.flushes() == 0 {
			t.Errorf("healthy store %d received no metrics/flush", i)
		}
	}
	if !strings.Contains(metricsBody, `obs_ingester_store_flush_total{outcome="degraded"} 1`) {
		t.Errorf("metrics lack degraded flush 1:\n%s", metricsBody)
	}
	if strings.Contains(metricsBody, "obs_ingester_store_retry_pending_bytes 0\n") ||
		!strings.Contains(metricsBody, "obs_ingester_store_retry_pending_bytes ") {
		t.Errorf("retry_pending_bytes should be > 0:\n%s", metricsBody)
	}
}

func TestIngesterDrainFailsWithTwoStoresDown(t *testing.T) {
	stores := startFlushStores(t, 0, http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	code, metricsBody := drainThroughStores(t, stores)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("drain = %d, want 503 (quorum missed)", code)
	}
	if !strings.Contains(metricsBody, `obs_ingester_store_flush_total{outcome="failed"} `) ||
		strings.Contains(metricsBody, `obs_ingester_store_flush_total{outcome="failed"} 0`) {
		t.Errorf("metrics lack a failed flush:\n%s", metricsBody)
	}
}
