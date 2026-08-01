package main

import (
	"encoding/json"
	"math"
	"math/rand"
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
