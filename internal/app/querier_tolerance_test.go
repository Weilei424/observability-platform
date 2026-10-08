package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	cfg := testConfig(t, config.TargetQuerier)
	cfg.IngesterURLs = ingesters
	cfg.StoreURL = readsPeer(t)
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
	if !strings.Contains(logged, "read answered by replication") {
		t.Errorf("no replication warn line: %s", logged)
	}
	if code, _ := labelsStatus(t, 3, up1, down1, down2); code != http.StatusServiceUnavailable {
		t.Errorf("two of three down, rf 3: status %d, want 503", code)
	}
	if code, _ := labelsStatus(t, 1, up1, up2, down1); code != http.StatusServiceUnavailable {
		t.Errorf("one down, rf 1: status %d, want 503", code)
	}
}
