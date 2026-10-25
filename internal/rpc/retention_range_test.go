package rpc

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// The longest retention a time.Duration holds survives the round trip: the
// client never sends more milliseconds than converts back without overflow,
// so split mode keeps the positive retention all-in-one applies.
func TestRetentionRequestStaysInRangeAtTheMaximum(t *testing.T) {
	maxD := time.Duration(math.MaxInt64)
	req := newRetentionRequest(time.UnixMilli(0), tenant.RetentionPolicy{Default: maxD, Overrides: map[string]time.Duration{"t": maxD}})
	for name, ms := range map[string]int64{"default": req.RetentionMs, "override": req.OverridesMs["t"]} {
		if ms > maxRetentionMs {
			t.Errorf("%s sent as %d ms, above the %d ms the store can convert", name, ms, maxRetentionMs)
		}
		if d := time.Duration(ms) * time.Millisecond; d <= 0 {
			t.Errorf("%s sent as %d ms converts back to %v, want positive", name, ms, d)
		}
	}
}

// The store refuses a retention it cannot convert to a time.Duration, instead
// of overflowing to a negative duration that reads as "keep forever".
func TestRetentionRouteRefusesOutOfRangeMilliseconds(t *testing.T) {
	var got tenant.RetentionPolicy
	applied := false
	apply := func(_ time.Time, p tenant.RetentionPolicy) (int, error) { got, applied = p, true; return 0, nil }
	post := func(req retentionRequest) int {
		b, _ := json.Marshal(req)
		rec := httptest.NewRecorder()
		retention(rec, httptest.NewRequest(http.MethodPost, "/metrics/retention", bytes.NewReader(b)), apply)
		return rec.Code
	}
	for _, tc := range []struct {
		name string
		req  retentionRequest
	}{
		{"default", retentionRequest{RetentionMs: maxRetentionMs + 1}},
		{"override", retentionRequest{OverridesMs: map[string]int64{"t": maxRetentionMs + 1}}},
		{"default at int64 max", retentionRequest{RetentionMs: math.MaxInt64}},
	} {
		applied = false
		if code := post(tc.req); code != http.StatusBadRequest || applied {
			t.Errorf("%s out of range: %d (applied %v), want 400 and nothing applied", tc.name, code, applied)
		}
	}
	if code := post(retentionRequest{RetentionMs: maxRetentionMs, OverridesMs: map[string]int64{"t": maxRetentionMs}}); code != http.StatusOK {
		t.Fatalf("the maximum = %d, want 200", code)
	}
	if got.Default <= 0 || got.Overrides["t"] <= 0 {
		t.Fatalf("the maximum applied as %+v, want positive durations", got)
	}
}
