package api

import (
	"math"
	"testing"
)

// TestSecondsToMillis_Boundary pins the exclusive upper bound. float64 cannot
// represent math.MaxInt64, so float64(math.MaxInt64) rounds *up* to 2^63 — one
// past the last representable int64. An inclusive `>` comparison therefore lets
// exactly 2^63 through and the int64 conversion wraps to MinInt64, turning a
// far-future timestamp into a far-past one.
func TestSecondsToMillis_Boundary(t *testing.T) {
	// Just under the boundary must still pass — the exclusive bound has to reject
	// only the unrepresentable value, not everything near the top of the range.
	// float64 spacing up here is 2048, so assert the sign rather than an exact
	// count: staying positive is precisely what the wrap destroys.
	if got, ok := secondsToMillis(9223372036854000); !ok || got <= 0 {
		t.Errorf("secondsToMillis(just below 2^63) = %d, %v; want a positive value and true", got, ok)
	}
	// -2^63 is exactly representable, so the lower bound stays inclusive.
	if got, ok := secondsToMillis(float64(math.MinInt64) / 1000); !ok || got != math.MinInt64 {
		t.Errorf("secondsToMillis(-2^63) = %d, %v; want %d, true", got, ok, int64(math.MinInt64))
	}
	for _, f := range []float64{
		float64(math.MaxInt64) / 1000, // exactly 2^63 milliseconds — unrepresentable
		9223372036854775,              // ×1000 lands on 2^63; wrapped to MinInt64 before the fix
		1e300,
		-1e300,
		math.Inf(1),
		math.Inf(-1),
		math.NaN(),
	} {
		if got, ok := secondsToMillis(f); ok {
			t.Errorf("secondsToMillis(%g) = %d, true; want ok=false", f, got)
		}
	}
}

// TestParseTimeParam_Boundary is the same case reached through the parameter
// parser the Prometheus endpoints actually call.
func TestParseTimeParam_Boundary(t *testing.T) {
	for _, bad := range []string{"9223372036854775", "1e300", "-1e300", "NaN", "Inf"} {
		if got, err := parseTimeParam("time", bad); err == nil {
			t.Errorf("parseTimeParam(%q) = %d, want an error", bad, got)
		}
	}
	if got, err := parseTimeParam("time", "1700000000.5"); err != nil || got != 1_700_000_000_500 {
		t.Errorf("parseTimeParam(1700000000.5) = %d, %v; want 1700000000500, nil", got, err)
	}
}

// TestParseDurationParam_Boundary covers the same guard on the step path, plus
// the grammar fallback.
func TestParseDurationParam_Boundary(t *testing.T) {
	for _, bad := range []string{"9223372036854775", "1e300", "NaN", "bogus", "106752d"} {
		if got, err := parseDurationParam("step", bad); err == nil {
			t.Errorf("parseDurationParam(%q) = %d, want an error", bad, got)
		}
	}
	for _, c := range []struct {
		in   string
		want int64
	}{
		{"15", 15_000},
		{"0.5", 500},
		{"1m", 60_000},
		{"1h30m", 5_400_000},
	} {
		if got, err := parseDurationParam("step", c.in); err != nil || got != c.want {
			t.Errorf("parseDurationParam(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
	}
}
