package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// decodedIngest is the receiving half of the ingest contract, mirroring the
// shape internal/api/ingest.go decodes.
type decodedIngest struct {
	Metrics []struct {
		Name        string            `json:"name"`
		Labels      map[string]string `json:"labels"`
		TimestampMs *int64            `json:"timestamp_ms"`
		Value       *float64          `json:"value"`
	} `json:"metrics"`
}

// wantSeries is the exact series set of the design, keyed by the string the
// test renders each sample into.
var wantSeries = []string{
	`sample_app_requests_total{method=GET,service=sample-app,status=200}`,
	`sample_app_requests_total{method=POST,service=sample-app,status=201}`,
	`sample_app_errors_total{method=GET,service=sample-app,status=500}`,
	`sample_app_errors_total{method=POST,service=sample-app,status=503}`,
	`sample_app_request_duration_seconds{method=GET,service=sample-app}`,
	`sample_app_request_duration_seconds{method=POST,service=sample-app}`,
	`sample_app_active_workers{service=sample-app}`,
}

// seriesKey renders one sample's identity with labels in sorted order, so the
// comparison does not depend on map iteration.
func seriesKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

// TestSamples_ExactlyTheDocumentedSeries pins the series set. A stray metric
// name or a dropped label would still render a dashboard panel — an empty one —
// so nothing else in the suite would notice.
func TestSamples_ExactlyTheDocumentedSeries(t *testing.T) {
	w := newWorkload()
	w.tick(rand.New(rand.NewSource(1)))

	got := w.samples(1_700_000_000_000)
	if len(got) != len(wantSeries) {
		t.Fatalf("samples() returned %d series, want %d", len(got), len(wantSeries))
	}
	for i, s := range got {
		if key := seriesKey(s.Name, s.Labels); key != wantSeries[i] {
			t.Errorf("series %d = %q, want %q", i, key, wantSeries[i])
		}
		if s.TimestampMs != 1_700_000_000_000 {
			t.Errorf("series %d timestamp = %d, want the value passed to samples()", i, s.TimestampMs)
		}
	}
}

// TestSamples_ErrorCountersArePresentBeforeAnyError is the reason the error
// counters are emitted unconditionally. rate() needs two samples inside its
// window; a counter that first appears on its own increment cannot supply them,
// so the Error Rate panel would read empty exactly when the demo is healthiest.
//
// Deliberately asserted on a fresh workload with no tick at all: that is the
// strongest form of the invariant, and it does not depend on which way a
// particular rand seed rolls.
func TestSamples_ErrorCountersArePresentBeforeAnyError(t *testing.T) {
	w := newWorkload()

	var found int
	for _, s := range w.samples(1) {
		if s.Name == "sample_app_errors_total" {
			found++
			if s.Value != 0 {
				t.Errorf("error counter %v = %v before any error occurred, want 0", s.Labels, s.Value)
			}
		}
	}
	if found != 2 {
		t.Errorf("found %d sample_app_errors_total series, want 2 even with no errors yet", found)
	}
}

// TestTick_CountersAreMonotonicAndBounded runs the simulation long enough to
// exercise both clamp directions of the worker random walk.
func TestTick_CountersAreMonotonicAndBounded(t *testing.T) {
	w := newWorkload()
	r := rand.New(rand.NewSource(7))

	prev := map[string]float64{}
	for i := 0; i < 2000; i++ {
		w.tick(r)
		for _, s := range w.samples(int64(i)) {
			key := seriesKey(s.Name, s.Labels)
			switch s.Name {
			case "sample_app_requests_total", "sample_app_errors_total":
				if s.Value < prev[key] {
					t.Fatalf("tick %d: counter %s went backwards: %v -> %v", i, key, prev[key], s.Value)
				}
			case "sample_app_active_workers":
				if s.Value < 1 || s.Value > 8 {
					t.Fatalf("tick %d: %s = %v, want within [1, 8]", i, key, s.Value)
				}
			case "sample_app_request_duration_seconds":
				if s.Value <= 0 || s.Value > 0.5 {
					t.Fatalf("tick %d: %s = %v, want a plausible latency in (0, 0.5]", i, key, s.Value)
				}
			}
			prev[key] = s.Value
		}
	}
	if prev[`sample_app_requests_total{method=GET,service=sample-app,status=200}`] != 2000 {
		t.Errorf("GET request counter = %v after 2000 ticks, want 2000 (one per tick)",
			prev[`sample_app_requests_total{method=GET,service=sample-app,status=200}`])
	}
}

// TestEncodeMetrics_PayloadPassesRealValidators runs generated output through
// the same validators the ingest handler uses, so a generator change ingest
// would reject fails here instead of silently producing an empty dashboard.
func TestEncodeMetrics_PayloadPassesRealValidators(t *testing.T) {
	w := newWorkload()
	r := rand.New(rand.NewSource(3))
	var all []metricSample
	for i := 0; i < 50; i++ {
		w.tick(r)
		all = append(all, w.samples(1_700_000_000_000+int64(i)*1000)...)
	}

	body, err := encodeMetrics(all)
	if err != nil {
		t.Fatalf("encodeMetrics: %v", err)
	}
	var payload decodedIngest
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("payload is not valid ingest JSON: %v", err)
	}
	if len(payload.Metrics) != len(all) {
		t.Fatalf("payload carries %d metrics, want %d", len(payload.Metrics), len(all))
	}

	for i, m := range payload.Metrics {
		if m.TimestampMs == nil || m.Value == nil {
			t.Fatalf("metric %d omitted a required field; the handler rejects null timestamp_ms/value", i)
		}
		// The handler folds the name in as __name__ before validating.
		labelMap := make(map[string]string, len(m.Labels)+1)
		for k, v := range m.Labels {
			labelMap[k] = v
		}
		labelMap["__name__"] = m.Name
		if _, err := metrics.NewLabels(labelMap); err != nil {
			t.Fatalf("metric %d labels %v rejected by metrics.NewLabels: %v", i, labelMap, err)
		}
		if err := metrics.ValidateSample(metrics.Sample{TimestampMs: *m.TimestampMs, Value: *m.Value}); err != nil {
			t.Fatalf("metric %d rejected by metrics.ValidateSample: %v", i, err)
		}
	}
}

// TestPostMetrics_OnlyExactly204CountsAsDelivered mirrors postBatch's rule for
// the metrics path: the ingest handler's contract is 204, and a regression to
// 200 or 202 must fail loudly rather than keep incrementing a success counter.
func TestPostMetrics_OnlyExactly204CountsAsDelivered(t *testing.T) {
	cases := []struct {
		status  int
		wantErr bool
	}{
		{http.StatusNoContent, false},
		{http.StatusOK, true},
		{http.StatusAccepted, true},
		{http.StatusBadRequest, true},
		{http.StatusInternalServerError, true},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":"detail"}`))
		}))
		err := postMetrics(srv.Client(), srv.URL, []byte(`{"metrics":[]}`))
		srv.Close()

		if tc.wantErr && err == nil {
			t.Errorf("status %d: postMetrics returned nil, want an error", tc.status)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("status %d: postMetrics returned %v, want nil", tc.status, err)
		}
		if tc.wantErr && err != nil && !strings.Contains(err.Error(), "204") {
			t.Errorf("status %d: error %q does not name the expected status", tc.status, err)
		}
	}
}

// TestPostMetrics_RequestShape pins the path, method, and content type the
// backend routes on.
func TestPostMetrics_RequestShape(t *testing.T) {
	var gotPath, gotMethod, gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotType = r.URL.Path, r.Method, r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := postMetrics(srv.Client(), srv.URL, []byte(`{"metrics":[]}`)); err != nil {
		t.Fatalf("postMetrics: %v", err)
	}
	if gotPath != "/api/v1/ingest/metrics" {
		t.Errorf("path = %q, want /api/v1/ingest/metrics", gotPath)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotType)
	}
}
