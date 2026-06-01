package api_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// postEmpty sends a POST with no body to the given path.
func postEmpty(t *testing.T, srv interface{ ServeHTTP(http.ResponseWriter, *http.Request) }, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

// --- /api/v1/labels ---

func TestMetadata_Labels_ReturnsSortedLabelNames(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "host": "a"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "mem_usage", "region": "us"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	rr := getQuery(t, srv, "/api/v1/labels")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	want := []string{"__name__", "host", "region"}
	if len(data) != len(want) {
		t.Fatalf("data len = %d, want %d; got %v", len(data), len(want), data)
	}
	for i, w := range want {
		if data[i].(string) != w {
			t.Errorf("data[%d] = %v, want %q", i, data[i], w)
		}
	}
}

func TestMetadata_Labels_EmptyStore_ReturnsEmptyArray(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/labels")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	if len(data) != 0 {
		t.Errorf("data = %v, want []", data)
	}
}

func TestMetadata_Labels_POST_Returns200(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(la, 1000, 1.0)

	rr := postEmpty(t, srv, "/api/v1/labels")
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/labels status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
}

func TestMetadata_Labels_InvalidStartParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/labels?start=bad")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestMetadata_Labels_EndBeforeStart_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/labels?start=100&end=50")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

// --- /api/v1/label/{name}/values ---

func TestMetadata_LabelValues_ReturnsMetricNames(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "mem_usage"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	rr := getQuery(t, srv, "/api/v1/label/__name__/values")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	want := []string{"cpu_usage", "mem_usage"}
	if len(data) != len(want) {
		t.Fatalf("data len = %d, want %d; got %v", len(data), len(want), data)
	}
	for i, w := range want {
		if data[i].(string) != w {
			t.Errorf("data[%d] = %v, want %q", i, data[i], w)
		}
	}
}

func TestMetadata_LabelValues_ExistingLabel_ReturnsSortedValues(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "service": "worker"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "mem_usage", "service": "api"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	rr := getQuery(t, srv, "/api/v1/label/service/values")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	want := []string{"api", "worker"}
	if len(data) != len(want) {
		t.Fatalf("data len = %d, want %d; got %v", len(data), len(want), data)
	}
	for i, w := range want {
		if data[i].(string) != w {
			t.Errorf("data[%d] = %v, want %q", i, data[i], w)
		}
	}
}

func TestMetadata_LabelValues_NonexistentLabel_ReturnsEmptyArray(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(la, 1000, 1.0)

	rr := getQuery(t, srv, "/api/v1/label/nonexistent/values")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	if len(data) != 0 {
		t.Errorf("data = %v, want []", data)
	}
}

// --- /api/v1/series ---

func TestMetadata_Series_ReturnsMatchingSeriesLabelSets(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "host": "a"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "mem_usage", "host": "b"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	u := "/api/v1/series?" + url.Values{"match[]": {"cpu_usage"}}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["status"] != "success" {
		t.Fatalf("status = %v, want success", body["status"])
	}
	data := body["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("data len = %d, want 1; got %v", len(data), data)
	}
	m := data[0].(map[string]any)
	if m["__name__"] != "cpu_usage" {
		t.Errorf("__name__ = %v, want cpu_usage", m["__name__"])
	}
}

func TestMetadata_Series_MultipleMatchSelectors_ReturnsUnion(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	lb, _ := metrics.NewLabels(map[string]string{"__name__": "mem_usage"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)

	rr := getQuery(t, srv, "/api/v1/series?match[]=cpu_usage&match[]=mem_usage")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	data := body["data"].([]any)
	if len(data) != 2 {
		t.Fatalf("data len = %d, want 2; got %v", len(data), data)
	}
}

func TestMetadata_Series_DuplicateMatchSelectors_Deduplicated(t *testing.T) {
	srv, store := newQueryTestServer(t)

	la, _ := metrics.NewLabels(map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(la, 1000, 1.0)

	// Same selector twice — same series must appear only once.
	rr := getQuery(t, srv, "/api/v1/series?match[]=cpu_usage&match[]=cpu_usage")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	data := body["data"].([]any)
	if len(data) != 1 {
		t.Errorf("data len = %d, want 1 (deduplicated); got %v", len(data), data)
	}
}

func TestMetadata_Series_NoMatchParam_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/series")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
	if body["error"] != "at least one match[] parameter is required" {
		t.Errorf("error = %v, want 'at least one match[] parameter is required'", body["error"])
	}
}

func TestMetadata_Series_InvalidMatchSelector_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	u := "/api/v1/series?" + url.Values{"match[]": {"bad{selector"}}.Encode()
	rr := getQuery(t, srv, u)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestMetadata_LabelValues_InvalidTimeRange_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/label/host/values?start=bad")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}

func TestMetadata_Series_EndBeforeStart_Returns400(t *testing.T) {
	srv, _ := newQueryTestServer(t)

	rr := getQuery(t, srv, "/api/v1/series?match[]=cpu_usage&start=100&end=50")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	body := decodePromResponse(t, rr)
	if body["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", body["errorType"])
	}
}
