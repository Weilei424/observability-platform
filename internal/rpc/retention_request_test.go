package rpc

import (
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// The wire carries whole milliseconds and a store reads 0 as "keep forever",
// so a positive retention is rounded up, never down to 0; 0 and negative stay 0.
func TestRetentionRequestRoundsPositiveRetentionUp(t *testing.T) {
	req := newRetentionRequest(time.UnixMilli(5000), tenant.RetentionPolicy{
		Default: 500 * time.Microsecond,
		Overrides: map[string]time.Duration{
			"sub-ms":  time.Nanosecond,
			"exact":   2 * time.Millisecond,
			"partial": 2*time.Millisecond + time.Nanosecond,
			"keep":    0,
		},
	})
	if req.RetentionMs != 1 {
		t.Errorf("default 500µs sent as %d ms, want 1", req.RetentionMs)
	}
	for id, want := range map[string]int64{"sub-ms": 1, "exact": 2, "partial": 3, "keep": 0} {
		if got := req.OverridesMs[id]; got != want {
			t.Errorf("%s sent as %d ms, want %d", id, got, want)
		}
	}
}
