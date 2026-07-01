package compactor

import (
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// Ranges returns ascending compaction ranges: base, base*multiplier,
// base*multiplier^2, ... for the given number of levels. multiplier is clamped
// to >= 2 and levels to >= 1.
func Ranges(base, multiplier int64, levels int) []int64 {
	if multiplier < 2 {
		multiplier = 2
	}
	if levels < 1 {
		levels = 1
	}
	out := make([]int64, 0, levels)
	r := base
	for i := 0; i < levels; i++ {
		out = append(out, r)
		r *= multiplier
	}
	return out
}

// Plan selects the first group of >=2 time-aligned blocks eligible to merge,
// scanning ranges smallest-first. A block is eligible for range R when its span
// is < R and it lies entirely within one R-aligned window [k*R, (k+1)*R).
// Returns the chosen group's IDs wrapped in a single-element slice, or nil when
// nothing should be compacted.
func Plan(infos []block.BlockInfo, ranges []int64) [][]string {
	sorted := make([]block.BlockInfo, len(infos))
	copy(sorted, infos)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].MinTime < sorted[j].MinTime })

	for _, r := range ranges {
		if r <= 0 {
			continue
		}
		groups := make(map[int64][]string)
		var windowOrder []int64
		for _, b := range sorted {
			// MaxTime >= MinTime for a valid block, so the difference only wraps
			// negative when the true span overflows int64; treat that as at/above
			// the range so extreme-timestamp blocks are never mis-grouped.
			span := b.MaxTime - b.MinTime
			if span < 0 || span >= r {
				continue // already at/above this range
			}
			if floorDiv(b.MinTime, r) != floorDiv(b.MaxTime, r) {
				continue // straddles a window boundary
			}
			w := floorDiv(b.MinTime, r)
			if _, ok := groups[w]; !ok {
				windowOrder = append(windowOrder, w)
			}
			groups[w] = append(groups[w], b.ID)
		}
		for _, w := range windowOrder {
			if len(groups[w]) >= 2 {
				return [][]string{groups[w]}
			}
		}
	}
	return nil
}

// floorDiv returns floor(a/b) for b > 0, correct for negative a (Go's integer
// division truncates toward zero).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
