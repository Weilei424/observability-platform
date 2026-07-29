package metrics

import (
	"fmt"
	"math"
	"strconv"
)

const nanosPerMilli = 1_000_000

// Multipliers are nanoseconds because prometheus/common's model.ParseDuration is
// nanosecond-based (its result is a time.Duration). Working in milliseconds here
// would silently accept durations upstream rejects as out of range — "106752d"
// fits in int64 milliseconds but overflows int64 nanoseconds.
var promDurationUnit = map[string]struct {
	rank int
	mult int64
}{
	"y":  {7, 365 * 24 * 3600 * 1e9},
	"w":  {6, 7 * 24 * 3600 * 1e9},
	"d":  {5, 24 * 3600 * 1e9},
	"h":  {4, 3600 * 1e9},
	"m":  {3, 60 * 1e9},
	"s":  {2, 1e9},
	"ms": {1, nanosPerMilli},
}

// ParsePromDuration parses a Prometheus duration string like "15s", "1m", "1h30m",
// returning milliseconds — the unit the metrics query path works in. Callers that
// need the full resolution of the grammar should use ParsePromDurationNanos.
func ParsePromDuration(s string) (int64, error) {
	ns, err := ParsePromDurationNanos(s)
	if err != nil {
		return 0, err
	}
	return ns / nanosPerMilli, nil
}

// ParsePromDurationNanos parses a Prometheus duration string, returning
// nanoseconds. Units must appear in strictly decreasing order (longest-to-shortest)
// with no repeats. This mirrors prometheus/common's model.ParseDuration, which is
// also the grammar Loki uses for `since` and `step`.
func ParsePromDurationNanos(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	// Prometheus allows a bare "0" without a unit, and only that value.
	if s == "0" {
		return 0, nil
	}
	var total int64
	remaining := s
	lastRank := len(promDurationUnit) + 1
	for remaining != "" {
		i := 0
		for i < len(remaining) && remaining[i] >= '0' && remaining[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("expected digits in %q", s)
		}
		n, err := strconv.ParseInt(remaining[:i], 10, 64)
		if err != nil {
			return 0, err
		}
		remaining = remaining[i:]
		if remaining == "" {
			return 0, fmt.Errorf("missing unit in %q", s)
		}
		var unit string
		if len(remaining) >= 2 && remaining[:2] == "ms" {
			unit = "ms"
		} else {
			unit = string(remaining[0])
		}
		remaining = remaining[len(unit):]
		u, ok := promDurationUnit[unit]
		if !ok {
			return 0, fmt.Errorf("unknown unit %q in %q", unit, s)
		}
		if u.rank >= lastRank {
			return 0, fmt.Errorf("unit %q out of order or repeated in %q", unit, s)
		}
		lastRank = u.rank
		// Overflow wraps to a plausible-looking (often smaller) millisecond count,
		// which would silently become a valid range window or query bound.
		if n > (math.MaxInt64-total)/u.mult {
			return 0, fmt.Errorf("duration %q overflows int64 milliseconds", s)
		}
		total += n * u.mult
	}
	return total, nil
}
