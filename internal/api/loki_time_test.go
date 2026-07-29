package api

import (
	"math"
	"testing"
)

// TestParseLokiTime pins the exact instant each accepted spelling maps to.
// These are direct assertions rather than HTTP-level ones because a bound
// misread as seconds instead of nanoseconds still lands *somewhere*, and for
// most fixtures that somewhere is still inside the queried window — only the
// parsed value itself separates the two interpretations.
func TestParseLokiTime(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int64
	}{
		// The digit-length rule, at and around its threshold.
		{"1700000000", 1_700_000_000_000_000_000},          // 10 digits → seconds
		{"17000000000", 17_000_000_000},                    // 11 digits → nanoseconds
		{"1700000000000000000", 1_700_000_000_000_000_000}, // 19 digits → nanoseconds

		// Loki measures the *raw* string, sign included: "-1234567890" is 11
		// characters, so it reads as nanoseconds (1969-12-31T23:59:58.765Z) and
		// not as seconds (1930-11-18Z) — a factor-of-1e9 difference in the bound.
		// One character shorter and the seconds branch takes over.
		{"-1234567890", -1_234_567_890},
		{"-123456789", -123_456_789_000_000_000},

		// Float seconds, rounded to milliseconds, either side of the epoch.
		{"1700000000.5", 1_700_000_000_500_000_000},
		{"-0.5", -500_000_000},

		// RFC3339, with and without a nanosecond fraction.
		{"2023-11-14T22:13:20Z", 1_700_000_000_000_000_000},
		{"2023-11-14T22:13:20.000000200Z", 1_700_000_000_000_000_200},

		// The representable window, exactly: the largest whole second on the
		// seconds branch, and the int64 extremes themselves on the ns branch.
		{"9223372036", 9_223_372_036_000_000_000},
		{"9223372036854775807", math.MaxInt64},
		{"-9223372036854775808", math.MinInt64},
	} {
		got, err := parseLokiTime(c.in)
		if err != nil {
			t.Errorf("parseLokiTime(%q) returned error %v, want %d", c.in, err, c.want)
			continue
		}
		if got != c.want {
			t.Errorf("parseLokiTime(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseLokiTime_Rejects(t *testing.T) {
	for _, bad := range []string{
		"",
		"not-a-time",
		"9223372036854775808",  // one nanosecond past the int64 maximum
		"-9223372036854775809", // one nanosecond past the int64 minimum
		"99999999999999999999",
		"1e999",
		"9223372037",           // one second past the representable maximum
		"9223372036.9",         // whole part in range, total nanoseconds is not
		"-9223372036.9",        // same at the negative boundary
		"2300-01-01T00:00:00Z", // past year 2262
		"1000-01-01T00:00:00Z", // before year 1678
	} {
		if got, err := parseLokiTime(bad); err == nil {
			t.Errorf("parseLokiTime(%q) = %d, want an error", bad, got)
		}
	}
}

// TestSinceStart covers the `since` duration arithmetic and, above all, the
// grammar: upstream parses it with Prometheus's model.ParseDuration, so the
// accepted set differs from Go's time.ParseDuration in both directions — `1d`
// and `1w` are Loki-valid and Go-invalid, `150ns` and `1.5h` are the reverse.
func TestSinceStart(t *testing.T) {
	const anchor int64 = 1_700_000_000_000_000_000

	for _, c := range []struct {
		in   string
		want int64
	}{
		{"0", anchor}, // the one value allowed without a unit
		{"500ms", anchor - 500_000_000},
		{"5m", anchor - 300_000_000_000},
		{"1h30m", anchor - 5_400_000_000_000},
		// Valid upstream, rejected by Go's parser, which stops at hours.
		{"1d", anchor - 86_400_000_000_000},
		{"1w", anchor - 604_800_000_000_000},
		{"1y", anchor - 31_536_000_000_000_000},
	} {
		got, err := sinceStart(c.in, anchor)
		if err != nil {
			t.Errorf("sinceStart(%q) returned error %v, want %d", c.in, err, c.want)
			continue
		}
		if got != c.want {
			t.Errorf("sinceStart(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	for _, bad := range []struct {
		in     string
		anchor int64
	}{
		{"", anchor},
		{"5", anchor},         // unitless and nonzero
		{"5 minutes", anchor}, // not a duration at all
		{"-5m", anchor},       // the grammar has no sign
		// Accepted by Go's parser, rejected upstream: sub-millisecond units and
		// fractional values are outside the Prometheus grammar.
		{"150ns", anchor},
		{"1.5h", anchor},
		{"30m1h", anchor},   // units must run longest-to-shortest
		{"99999999999w", 0}, // overflows int64 milliseconds while parsing
		{"106752d", 0},      // fits in milliseconds, overflows the ns conversion
		{"8000w", -1 << 62}, // reaches past the low end of the ns window
	} {
		if got, err := sinceStart(bad.in, bad.anchor); err == nil {
			t.Errorf("sinceStart(%q, %d) = %d, want an error", bad.in, bad.anchor, got)
		}
	}
}
