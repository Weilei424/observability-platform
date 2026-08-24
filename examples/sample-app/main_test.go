package main

import (
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
)

// decodePush is the receiving half of the push contract, mirroring the shape
// internal/api/loki_push.go decodes.
type decodedPush struct {
	Streams []struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	} `json:"streams"`
}

// TestEncodePush_PayloadPassesRealValidators runs generated output through the
// same validators the push handler uses, so a generator change that ingest would
// reject fails here instead of silently producing an empty demo.
func TestEncodePush_PayloadPassesRealValidators(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	var all []entry
	for i := 0; i < 200; i++ {
		all = append(all, buildBatch(r, int64(1_700_000_000_000_000_000+i))...)
	}

	body, err := encodePush(all)
	if err != nil {
		t.Fatalf("encodePush: %v", err)
	}
	var payload decodedPush
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("payload is not valid Loki push JSON: %v", err)
	}
	if len(payload.Streams) == 0 {
		t.Fatal("payload has no streams")
	}

	total := 0
	for _, s := range payload.Streams {
		if _, err := logs.NewStreamLabels(s.Stream); err != nil {
			t.Fatalf("stream labels %v rejected by logs.NewStreamLabels: %v", s.Stream, err)
		}
		if s.Stream["env"] != "local" {
			t.Errorf("stream %v missing env=local", s.Stream)
		}
		for _, v := range s.Values {
			ts, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				t.Fatalf("timestamp %q is not a decimal int64: %v", v[0], err)
			}
			if err := logs.ValidateEntry(logs.LogEntry{TimestampNs: ts, Line: v[1]}); err != nil {
				t.Fatalf("entry rejected by logs.ValidateEntry: %v", err)
			}
			total++
		}
	}
	if total != 800 {
		t.Errorf("total entries = %d, want 800 (200 batches x 4)", total)
	}
}

// TestBuildBatch_OnlyTheDocumentedFiveStreams pins the stream set the dashboard
// and runbook are written against.
func TestBuildBatch_OnlyTheDocumentedFiveStreams(t *testing.T) {
	want := map[string]bool{
		"api/info": true, "api/warn": true, "api/error": true,
		"worker/info": true, "worker/error": true,
	}
	seen := map[string]bool{}
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 2000; i++ {
		batch := buildBatch(r, 1)
		if len(batch) != 4 {
			t.Fatalf("batch size = %d, want 4", len(batch))
		}
		for _, e := range batch {
			k := e.service + "/" + e.level
			if !want[k] {
				t.Fatalf("unexpected stream %q", k)
			}
			seen[k] = true
		}
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("stream %q never generated in 2000 batches", k)
		}
	}
}

// TestEncodePush_GroupsByStream verifies entries of one stream become one stream
// object with several values, which is what makes the payload Loki-shaped.
func TestEncodePush_GroupsByStream(t *testing.T) {
	body, err := encodePush([]entry{
		{"api", "info", 100, "first"},
		{"api", "error", 200, "second"},
		{"api", "info", 300, "third"},
	})
	if err != nil {
		t.Fatalf("encodePush: %v", err)
	}
	var payload decodedPush
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload.Streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(payload.Streams))
	}
	for _, s := range payload.Streams {
		want := 1
		if s.Stream["level"] == "info" {
			want = 2
		}
		if len(s.Values) != want {
			t.Errorf("stream %v has %d values, want %d", s.Stream, len(s.Values), want)
		}
	}
	if payload.Streams[0].Values[0][0] != "100" {
		t.Errorf("first timestamp = %q, want \"100\"", payload.Streams[0].Values[0][0])
	}
}

// TestPostBatch_OnlyExactly204CountsAsDelivered pins the Loki push contract.
// The generator previously accepted any 2xx, so a backend that regressed to 200
// or 202 — which no longer speaks the Loki push API — would still have every
// batch counted as delivered and nothing in the demo would report a problem.
func TestPostBatch_OnlyExactly204CountsAsDelivered(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"204 No Content — the contract", http.StatusNoContent, false},
		{"200 OK", http.StatusOK, true},
		{"201 Created", http.StatusCreated, true},
		{"202 Accepted", http.StatusAccepted, true},
		{"400 Bad Request", http.StatusBadRequest, true},
		{"500 Internal Server Error", http.StatusInternalServerError, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != http.StatusNoContent {
					http.Error(w, "backend says no", tc.status)
					return
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			err := postBatch(srv.Client(), srv.URL, []byte(`{"streams":[]}`))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("postBatch with status %d returned nil; want an error", tc.status)
				}
				// Both numbers must be in the message: an operator reading the
				// demo log needs to see what arrived and what was required.
				for _, want := range []string{strconv.Itoa(tc.status), "204"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not mention %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("postBatch with status %d returned error: %v", tc.status, err)
			}
		})
	}
}

// TestPostBatch_RequestShape pins what the generator actually sends, so the
// status assertion above is testing the real push request and not an incidental
// one the backend would reject for unrelated reasons.
func TestPostBatch_RequestShape(t *testing.T) {
	var gotPath, gotMethod, gotType, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotType = r.URL.Path, r.Method, r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	body := []byte(`{"streams":[{"stream":{"service":"api"},"values":[["1","hello"]]}]}`)
	if err := postBatch(srv.Client(), srv.URL, body); err != nil {
		t.Fatalf("postBatch: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/loki/api/v1/push" {
		t.Errorf("path = %q, want /loki/api/v1/push", gotPath)
	}
	if gotType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotType)
	}
	if gotBody != string(body) {
		t.Errorf("body = %q, want %q", gotBody, body)
	}
}

// TestPostBatch_TransportErrorIsReported covers the other failure path main()
// counts: a backend that is not listening at all must return an error rather
// than a nil that would be read as a delivered batch.
func TestPostBatch_TransportErrorIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close() // nothing is listening on addr now

	if err := postBatch(&http.Client{Timeout: time.Second}, addr, []byte(`{}`)); err == nil {
		t.Fatal("postBatch against a closed server returned nil; want an error")
	}
}

// TestTickerInterval_RejectsEveryRateThatWouldPanic covers the inputs a bare
// `rate <= 0` guard lets through. NaN fails every comparison, and a large enough
// rate divides to a sub-nanosecond interval that truncates to zero; both reach
// time.NewTicker as a non-positive duration and panic.
func TestTickerInterval_RejectsEveryRateThatWouldPanic(t *testing.T) {
	// wantMsg is asserted because the direction of the diagnostic is the point:
	// an overflowing interval wraps negative and would otherwise be reported as
	// "too large" when the rate is in fact too small.
	rejected := []struct {
		name    string
		rate    float64
		wantMsg string
	}{
		{"zero", 0, "greater than 0"},
		{"negative", -1, "greater than 0"},
		{"NaN", math.NaN(), "finite"},
		{"+Inf", math.Inf(1), "finite"},
		{"-Inf", math.Inf(-1), "finite"},
		{"sub-nanosecond interval", 1e10, "too large"},
		{"just past one batch per nanosecond", 2e9, "too large"},
		{"interval overflows int64", 1e-10, "too small"},
		{"far below the representable floor", 1e-300, "too small"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			d, err := tickerInterval(tc.rate)
			if err == nil {
				t.Fatalf("tickerInterval(%v) = %v, nil; want an error (time.NewTicker would panic)", tc.rate, d)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("tickerInterval(%v) error = %q, want it to mention %q", tc.rate, err, tc.wantMsg)
			}
		})
	}

	accepted := []struct {
		name string
		rate float64
		want time.Duration
	}{
		{"default demo rate", 2, 500 * time.Millisecond},
		{"one per second", 1, time.Second},
		{"fractional", 0.5, 2 * time.Second},
		{"smallest interval still representable", 1e9, time.Nanosecond},
		// The lower boundary: 1e9/1.1e-10 is ~9.09e18 ns, just inside int64's
		// range, so a rate this slow is useless but representable and accepted.
		{"largest interval still representable", 1.1e-10, time.Duration(float64(time.Second) / 1.1e-10)},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			d, err := tickerInterval(tc.rate)
			if err != nil {
				t.Fatalf("tickerInterval(%v) returned error: %v", tc.rate, err)
			}
			if d != tc.want {
				t.Errorf("tickerInterval(%v) = %v, want %v", tc.rate, d, tc.want)
			}
		})
	}
}

// TestTickerInterval_AcceptedRatesNeverPanicNewTicker closes the loop: every
// rate the validator accepts must actually be usable by time.NewTicker.
func TestTickerInterval_AcceptedRatesNeverPanicNewTicker(t *testing.T) {
	for _, rate := range []float64{0.25, 1, 2, 1000, 1e6, 1e9} {
		d, err := tickerInterval(rate)
		if err != nil {
			t.Fatalf("tickerInterval(%v) rejected a usable rate: %v", rate, err)
		}
		tk := time.NewTicker(d) // panics if d <= 0
		tk.Stop()
	}
}

// TestStartupLine_ExactRendering pins the banner's exact text. The rate field
// regressed once: %.1f rendered 0.01 as "0.0", the one value tickerInterval
// refuses to start on, so the banner reported a rate the program would have
// rejected. Nothing else in the suite would notice a return to %.1f.
func TestStartupLine_ExactRendering(t *testing.T) {
	cases := []struct {
		name            string
		rate            float64
		interval        time.Duration
		metricsRate     float64
		metricsInterval time.Duration
		duration        int
		want            string
	}{
		{
			name: "fractional rate that %.1f would flatten to zero",
			rate: 0.01, interval: 100 * time.Second,
			metricsRate: 0.01, metricsInterval: 100 * time.Second, duration: 0,
			want: "sample-app: addr=http://backend:8080 rate=0.01/s interval=1m40s metrics_rate=0.01/s metrics_interval=1m40s duration=0s",
		},
		{
			name: "default demo rates",
			rate: 2, interval: 500 * time.Millisecond,
			metricsRate: 1, metricsInterval: time.Second, duration: 0,
			want: "sample-app: addr=http://backend:8080 rate=2/s interval=500ms metrics_rate=1/s metrics_interval=1s duration=0s",
		},
		{
			name: "rate near the representable floor",
			rate: 1e-9, interval: 277777*time.Hour + 46*time.Minute + 40*time.Second,
			metricsRate: 1, metricsInterval: time.Second, duration: 30,
			want: "sample-app: addr=http://backend:8080 rate=1e-09/s interval=277777h46m40s metrics_rate=1/s metrics_interval=1s duration=30s",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := startupLine("http://backend:8080", tc.rate, tc.interval, tc.metricsRate, tc.metricsInterval, tc.duration)
			if got != tc.want {
				t.Errorf("startupLine =\n  %q\nwant\n  %q", got, tc.want)
			}
		})
	}
}

// TestStartupLine_NoAcceptedRateRendersAsZero is the invariant behind the
// formatting choice, stated independently of the verb used: every rate
// tickerInterval accepts must render as something an operator can tell apart
// from 0, because 0 is exactly what tickerInterval rejects. Both rate fields
// are varied, since both go through the same formatting.
func TestStartupLine_NoAcceptedRateRendersAsZero(t *testing.T) {
	for _, rate := range []float64{1e-9, 1e-6, 0.001, 0.01, 0.04, 0.25, 0.5, 1, 2, 100} {
		interval, err := tickerInterval(rate)
		if err != nil {
			t.Fatalf("tickerInterval(%v) rejected a rate this test assumes is valid: %v", rate, err)
		}
		for _, line := range []string{
			startupLine("http://backend:8080", rate, interval, 1, time.Second, 0),
			startupLine("http://backend:8080", 2, 500*time.Millisecond, rate, interval, 0),
		} {
			for _, zero := range []string{"rate=0/s", "rate=0.0/s", "rate=0.00/s"} {
				if strings.Contains(line, zero) {
					t.Errorf("rate %v renders as %q in %q — indistinguishable from a rate tickerInterval rejects", rate, zero, line)
				}
			}
		}
	}
}
