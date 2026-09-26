package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

type brokenMetrics struct{ metrics.Source }

func (brokenMetrics) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, errors.New("disk on fire")
}

type emptyLogs struct{}

func (emptyLogs) SelectStreams(context.Context, []index.Pair, int64, int64) ([]logs.StreamData, error) {
	return nil, nil
}
func (emptyLogs) SelectLabelNames(context.Context) ([]string, error) { return []string{"service"}, nil }
func (emptyLogs) SelectLabelValues(context.Context, string) ([]string, error) {
	return []string{"api"}, nil
}

func readsServer(t *testing.T, m metrics.Source, l logs.Source) *httptest.Server {
	t.Helper()
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { rpc.MountReads(r, m, l) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return resp, b.String()
}

func TestMetricsSelectRoute(t *testing.T) {
	ms := metrics.NewMemoryStore()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "m", "job": "a"})
	for i := range 3 {
		if err := ms.Append(l, int64(i*10), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	srv := readsServer(t, ms, emptyLogs{})

	resp, body := post(t, srv.URL+"/internal/v1/metrics/select",
		`{"matchers":[{"name":"__name__","value":"m"}],"min_ms":10,"max_ms":20,"anchor":true}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var got struct {
		Series []struct {
			Labels  map[string]string   `json:"labels"`
			Anchor  []json.RawMessage   `json:"anchor"`
			Samples [][]json.RawMessage `json:"samples"`
		} `json:"series"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	if len(got.Series) != 1 || len(got.Series[0].Samples) != 2 || string(got.Series[0].Anchor[1]) != `"0"` {
		t.Fatalf("answer = %s, want one series, two samples, anchor value \"0\"", body)
	}

	for _, bad := range []string{
		`{"matchers":[],"min_ms":0,"max_ms":1,"any_time":true}`,        // AnyTime without SeriesOnly
		`{"matchers":[{"name":"","value":"x"}],"min_ms":0,"max_ms":1}`, // empty matcher name
		`{"matchers":[],"min_ms":0,"max_ms":1,"surprise":true}`,        // unknown field
		`not json`,
	} {
		if resp, body := post(t, srv.URL+"/internal/v1/metrics/select", bad); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s), want 400", bad, resp.StatusCode, body)
		}
	}
}

func TestMetricsSelectRouteReportsASourceFailureAs500(t *testing.T) {
	srv := readsServer(t, brokenMetrics{metrics.NewMemoryStore()}, emptyLogs{})
	resp, body := post(t, srv.URL+"/internal/v1/metrics/select", `{"matchers":[],"min_ms":0,"max_ms":1}`)
	if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(body, "disk on fire") {
		t.Fatalf("status %d body %s, want 500 carrying the cause", resp.StatusCode, body)
	}
}

func TestLabelRoutes(t *testing.T) {
	ms := metrics.NewMemoryStore()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "m", "job": "a b/c"})
	if err := ms.Append(l, 1, 1); err != nil {
		t.Fatal(err)
	}
	srv := readsServer(t, ms, emptyLogs{})
	for path, want := range map[string]string{
		"/internal/v1/metrics/labels":                 `{"names":["__name__","job"]}`,
		"/internal/v1/metrics/label-values?name=job":  `{"values":["a b/c"]}`,
		"/internal/v1/logs/labels":                    `{"names":["service"]}`,
		"/internal/v1/logs/label-values?name=service": `{"values":["api"]}`,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || strings.TrimSpace(b.String()) != want {
			t.Errorf("GET %s = %d %s, want 200 %s", path, resp.StatusCode, b.String(), want)
		}
	}
	if resp, _ := http.Get(srv.URL + "/internal/v1/metrics/label-values"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("label-values without a name = %d, want 400", resp.StatusCode)
	}
}

func TestLogsSelectRoute(t *testing.T) {
	srv := readsServer(t, metrics.NewMemoryStore(), emptyLogs{})
	resp, body := post(t, srv.URL+"/internal/v1/logs/select", `{"matchers":[{"name":"service","value":"api"}],"min_ns":0,"max_ns":10}`)
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(body) != `{"streams":[]}` {
		t.Fatalf("status %d body %s, want 200 {\"streams\":[]}", resp.StatusCode, body)
	}
}
