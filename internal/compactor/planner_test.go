package compactor

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

func TestRanges(t *testing.T) {
	got := Ranges(2, 4, 3)
	want := []int64{2, 8, 32}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("Ranges = %v, want %v", got, want)
	}
}

func TestPlan_GroupsTwoAlignedBlocks(t *testing.T) {
	// range 100: both blocks fit window [0,100), span < 100.
	infos := []block.BlockInfo{
		{ID: "a", MinTime: 0, MaxTime: 40},
		{ID: "b", MinTime: 50, MaxTime: 90},
	}
	groups := Plan(infos, []int64{100})
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("Plan = %v, want one group of 2", groups)
	}
}

// TestPlan_PromotesToHigherTier exercises multi-tier promotion across successive
// calls: a lower-tier merge produces a block that then becomes eligible at the next
// tier alongside a wider block excluded from the lower tier.
func TestPlan_PromotesToHigherTier(t *testing.T) {
	ranges := []int64{100, 400}
	infos := []block.BlockInfo{
		{ID: "a", MinTime: 0, MaxTime: 40},
		{ID: "b", MinTime: 50, MaxTime: 90},
		{ID: "c", MinTime: 100, MaxTime: 390}, // span 290 ≥ 100: not tier-100 eligible, fits a 400 window
	}
	// First call: only a,b qualify, at tier 100.
	if g := Plan(infos, ranges); len(g) != 1 || len(g[0]) != 2 || g[0][0] != "a" || g[0][1] != "b" {
		t.Fatalf("first Plan = %v, want [[a b]] at tier 100", g)
	}
	// After a,b merge into m (span 90), m and c promote to tier 400 (c is still not
	// tier-100 eligible, so a 2-block group can only form at tier 400).
	infos2 := []block.BlockInfo{
		{ID: "m", MinTime: 0, MaxTime: 90},
		{ID: "c", MinTime: 100, MaxTime: 390},
	}
	if g := Plan(infos2, ranges); len(g) != 1 || len(g[0]) != 2 {
		t.Fatalf("second Plan = %v, want one tier-400 group of 2 (promotion)", g)
	}
}

func TestPlan_SkipsStraddlingBlock(t *testing.T) {
	// "b" straddles the window boundary at 100 → not eligible; only 1 block in
	// window 0 → no group.
	infos := []block.BlockInfo{
		{ID: "a", MinTime: 0, MaxTime: 40},
		{ID: "b", MinTime: 90, MaxTime: 110},
	}
	if groups := Plan(infos, []int64{100}); groups != nil {
		t.Fatalf("Plan = %v, want nil", groups)
	}
}

func TestPlan_ExcludesBlockAtOrAboveRange(t *testing.T) {
	// "a" spans the full range → already compacted at this tier; "b" alone → none.
	infos := []block.BlockInfo{
		{ID: "a", MinTime: 0, MaxTime: 100},
		{ID: "b", MinTime: 10, MaxTime: 20},
	}
	if groups := Plan(infos, []int64{100}); groups != nil {
		t.Fatalf("Plan = %v, want nil (only one eligible block)", groups)
	}
}

func TestPlan_SmallestTierFirst(t *testing.T) {
	// At range 100: only a,b share a window (window 0); c and d sit alone in
	// windows 2 and 3 → the sole ≥2 group is {a,b}.
	// At range 1000: all four share window 0 → group {a,b,c,d}.
	// Plan must return the range-100 group {a,b}; if the larger tier were scanned
	// first it would return all four. Asserting exactly {a,b} proves smallest-first.
	infos := []block.BlockInfo{
		{ID: "a", MinTime: 0, MaxTime: 40},
		{ID: "b", MinTime: 50, MaxTime: 90},
		{ID: "c", MinTime: 200, MaxTime: 240},
		{ID: "d", MinTime: 300, MaxTime: 340},
	}
	groups := Plan(infos, []int64{100, 1000})
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("Plan = %v, want exactly one 2-block group from the smallest tier", groups)
	}
	got := map[string]bool{groups[0][0]: true, groups[0][1]: true}
	if !got["a"] || !got["b"] {
		t.Fatalf("Plan returned %v, want the range-100 group {a,b}", groups[0])
	}
}

func TestPlan_EmptyAndSingle(t *testing.T) {
	if Plan(nil, []int64{100}) != nil {
		t.Fatal("Plan(nil) should be nil")
	}
	if Plan([]block.BlockInfo{{ID: "a", MinTime: 0, MaxTime: 10}}, []int64{100}) != nil {
		t.Fatal("Plan(single) should be nil")
	}
}
