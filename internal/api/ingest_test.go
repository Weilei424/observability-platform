package api_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

func newIngestTestServer(t *testing.T) (*api.Server, *metrics.MemoryStore) {
	t.Helper()
	store := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(store)
	cfg := &config.Config{
		HTTPAddr: ":8080",
		DataDir:  t.TempDir(),
		LogLevel: "info",
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	reg, _ := observability.NewRegistry(store, nil)
	return api.New(cfg, log, store, engine, reg, logs.NewMemoryStore()), store
}

func postIngest(t *testing.T, srv *api.Server, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func TestIngestMetrics_ValidSingleSample_Returns204(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "http_requests_total",
				"labels":       map[string]string{"service": "api"},
				"timestamp_ms": int64(1000),
				"value":        float64(42),
			},
		},
	})

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
}

func TestIngestMetrics_ValidBatch_AllSamplesStored(t *testing.T) {
	srv, store := newIngestTestServer(t)

	labelsA, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "host": "a"})
	labelsB, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "host": "b"})

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "cpu_usage", "labels": map[string]string{"host": "a"}, "timestamp_ms": int64(1000), "value": float64(0.5)},
			map[string]any{"name": "cpu_usage", "labels": map[string]string{"host": "b"}, "timestamp_ms": int64(1000), "value": float64(0.8)},
		},
	})

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}

	gotA, err := store.QueryRange(metrics.SeriesID(labelsA.Hash()), 0, 2000)
	if err != nil {
		t.Fatalf("QueryRange A: %v", err)
	}
	gotB, err := store.QueryRange(metrics.SeriesID(labelsB.Hash()), 0, 2000)
	if err != nil {
		t.Fatalf("QueryRange B: %v", err)
	}

	if len(gotA) != 1 || gotA[0].Value != 0.5 {
		t.Errorf("series A: unexpected samples %v", gotA)
	}
	if len(gotB) != 1 || gotB[0].Value != 0.8 {
		t.Errorf("series B: unexpected samples %v", gotB)
	}
}

func TestIngestMetrics_MissingName_Returns400WithError(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{
				"labels":       map[string]string{"service": "api"},
				"timestamp_ms": int64(1000),
				"value":        float64(1),
			},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	errs, ok := body["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Errorf("expected non-empty errors array, got: %v", body)
	}
	first := errs[0].(map[string]any)
	if first["index"].(float64) != 0 {
		t.Errorf("expected error at index 0, got %v", first["index"])
	}
}

func TestIngestMetrics_InvalidMetricName_Returns400(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "123invalid", "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
}

func TestIngestMetrics_InvalidLabelName_Returns400(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{
				"name":         "http_requests_total",
				"labels":       map[string]string{"123bad": "value"},
				"timestamp_ms": int64(1000),
				"value":        float64(1),
			},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
}

func TestIngestMetrics_MixedBatch_NoSamplesWritten(t *testing.T) {
	srv, store := newIngestTestServer(t)

	validLabels, _ := metrics.NewLabels(map[string]string{"__name__": "valid_metric"})

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "valid_metric", "timestamp_ms": int64(1000), "value": float64(1)},
			map[string]any{"name": "123bad", "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}

	got, err := store.QueryRange(metrics.SeriesID(validLabels.Hash()), 0, 2000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no samples written on mixed-batch rejection, got %d", len(got))
	}
}

func TestIngestMetrics_RepeatedWrites_AppendToSameSeries(t *testing.T) {
	srv, store := newIngestTestServer(t)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "counter", "env": "prod"})

	postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "counter", "labels": map[string]string{"env": "prod"}, "timestamp_ms": int64(1000), "value": float64(1)},
		},
	})
	postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "counter", "labels": map[string]string{"env": "prod"}, "timestamp_ms": int64(2000), "value": float64(2)},
		},
	})

	got, err := store.QueryRange(metrics.SeriesID(labels.Hash()), 0, 3000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2", len(got))
	}
	if got[0].Value != 1 || got[1].Value != 2 {
		t.Errorf("unexpected values: got [%v, %v], want [1, 2]", got[0].Value, got[1].Value)
	}
}

func TestIngestMetrics_MissingTimestamp_Returns400(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "http_requests_total", "value": float64(1)},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	errs, ok := body["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Errorf("expected errors array, got: %v", body)
	}
	first := errs[0].(map[string]any)
	if first["field"] != "timestamp_ms" {
		t.Errorf("expected field=timestamp_ms, got %v", first["field"])
	}
}

func TestIngestMetrics_MissingValue_Returns400(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	rr := postIngest(t, srv, map[string]any{
		"metrics": []any{
			map[string]any{"name": "http_requests_total", "timestamp_ms": int64(1000)},
		},
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	errs, ok := body["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Errorf("expected errors array, got: %v", body)
	}
	first := errs[0].(map[string]any)
	if first["field"] != "value" {
		t.Errorf("expected field=value, got %v", first["field"])
	}
}

func TestIngestMetrics_MalformedJSON_Returns400(t *testing.T) {
	srv, _ := newIngestTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewBufferString("{not valid json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}
