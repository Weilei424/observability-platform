package api_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/drain"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// drainingServer serves the public write routes over heads behind a closed
// drain gate, as an ingester does once a drain has started.
func drainingServer(t *testing.T) (*api.Server, *metrics.MemoryStore, *logs.MemoryStore) {
	t.Helper()
	mstore, lstore := metrics.NewMemoryStore(), logs.NewMemoryStore()
	g := drain.NewGate()
	if err := g.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg, _ := observability.NewRegistry(observability.RegistryOptions{Cardinality: mstore})
	return api.New(api.Deps{
		Config:      &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"},
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Ingester:    g.Metrics(mstore),
		Engine:      metrics.NewQueryEngine(mstore),
		Registry:    reg,
		LogIngester: g.Logs(lstore),
	}), mstore, lstore
}

func TestWritesToADrainingIngesterAnswer503(t *testing.T) {
	srv, mstore, lstore := drainingServer(t)

	rr := postIngest(t, srv, map[string]any{"metrics": []map[string]any{
		{"name": "m", "labels": map[string]string{"job": "a"}, "timestamp_ms": 1, "value": 1},
		{"name": "m", "labels": map[string]string{"job": "a"}, "timestamp_ms": 2, "value": 2},
	}})
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("metrics ingest: status = %d, want 503; body %s", rr.Code, rr.Body)
	}
	if n := len(mstore.LabelNames()); n != 0 {
		t.Errorf("a draining ingester stored metrics: label names %v", mstore.LabelNames())
	}

	rr = postPush(t, srv, `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x"]]}]}`, "application/json")
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("loki push: status = %d, want 503; body %s", rr.Code, rr.Body)
	}
	if n := lstore.StreamCount(); n != 0 {
		t.Errorf("a draining ingester stored %d streams", n)
	}
}
