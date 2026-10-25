// internal/limits/gate_test.go
package limits

import (
	"errors"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
)

type fakeCounter struct {
	mu  sync.Mutex
	ids map[string]map[uint64]bool
}

func (f *fakeCounter) Count(t string) int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.ids[t]) }
func (f *fakeCounter) Has(id uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.ids {
		if s[id] {
			return true
		}
	}
	return false
}
func (f *fakeCounter) add(t string, ids ...uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ids[t] == nil {
		f.ids[t] = map[uint64]bool{}
	}
	for _, id := range ids {
		f.ids[t][id] = true
	}
}

func TestShare(t *testing.T) {
	for _, tc := range []struct{ limit, rf, n, want int }{
		{0, 3, 3, 0}, {100, 1, 1, 100}, {100, 3, 3, 100}, {100, 1, 4, 25}, {100, 3, 4, 75}, {10, 1, 3, 4}, {100, 3, 0, 100},
		{math.MaxInt, 3, 3, math.MaxInt}, {math.MaxInt, 5, 3, math.MaxInt}, {math.MaxInt, 1, 1, math.MaxInt}, {1, 1, 3, 1},
	} {
		if got := Share(tc.limit, tc.rf, tc.n); got != tc.want {
			t.Errorf("Share(%d,%d,%d) = %d, want %d", tc.limit, tc.rf, tc.n, got, tc.want)
		}
	}
}

func TestAdmitExactlyAtTheLimitWithDuplicates(t *testing.T) {
	c := &fakeCounter{ids: map[string]map[uint64]bool{}}
	c.add("b", 1)
	g := NewGate("series", func(string) int { return 3 }, c, nil)
	// 1 is already active; 2 and 3 are new (3 twice): 1 + 2 == 3 fits.
	err := g.Admit(map[string][]uint64{"b": {1, 2, 3, 3}}, func() error { c.add("b", 2, 3); return nil })
	if err != nil {
		t.Fatalf("admit at the limit: %v", err)
	}
	var le *LimitError
	err = g.Admit(map[string][]uint64{"b": {4}}, func() error { t.Fatal("must not run"); return nil })
	if !errors.As(err, &le) || le.Tenant != "b" || le.Limit != 3 {
		t.Fatalf("err = %v, want a LimitError for b", err)
	}
	// Existing series always fit.
	if err := g.Admit(map[string][]uint64{"b": {1, 2}}, func() error { return nil }); err != nil {
		t.Fatalf("existing series: %v", err)
	}
}

func TestAdmitIsExactUnderConcurrentPushes(t *testing.T) {
	c := &fakeCounter{ids: map[string]map[uint64]bool{}}
	g := NewGate("series", func(string) int { return 10 }, c, nil)
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uint64(i)
			if g.Admit(map[string][]uint64{"b": {id}}, func() error { c.add("b", id); return nil }) == nil {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 10 || c.Count("b") != 10 {
		t.Fatalf("admitted %d, active %d; want exactly 10", admitted.Load(), c.Count("b"))
	}
}

func TestUnlimitedTenantRunsWithoutCounting(t *testing.T) {
	c := &fakeCounter{ids: map[string]map[uint64]bool{}}
	g := NewGate("series", func(string) int { return 0 }, c, nil)
	ran := false
	if err := g.Admit(map[string][]uint64{"b": {1, 2, 3}}, func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("unlimited: ran=%v err=%v", ran, err)
	}
}

func TestRefusalIsReported(t *testing.T) {
	c := &fakeCounter{ids: map[string]map[uint64]bool{}}
	var got string
	g := NewGate("streams", func(string) int { return 1 }, c, func(t string) { got = t })
	_ = g.Admit(map[string][]uint64{"b": {1, 2}}, func() error { return nil })
	if got != "b" {
		t.Fatalf("onReject got %q", got)
	}
}

// bigShare is Share computed with arbitrary-precision integers: the oracle the
// overflow cases are checked against.
func bigShare(limit, rf, n int64) int64 {
	num := new(big.Int).Mul(big.NewInt(limit), big.NewInt(rf))
	num.Add(num, big.NewInt(n-1))
	share := num.Quo(num, big.NewInt(n)).Int64()
	return min(max(share, 1), limit)
}

// No intermediate of Share may overflow, whatever the inputs: a huge limit, or
// an ingester count near the limit (r*rf+n-1 is where a naive split overflows).
func TestShareDoesNotOverflow(t *testing.T) {
	for _, tc := range []struct{ limit, rf, n int64 }{
		{math.MaxInt32, 2, 3},
		{math.MaxInt32, math.MaxInt32 - 1, math.MaxInt32},
		{math.MaxInt64, 2, 3},
		{7999999999, 3999999999, 4000000000},
		{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64},
	} {
		if tc.limit > math.MaxInt || tc.n > math.MaxInt {
			continue // needs a 64-bit int
		}
		want := bigShare(tc.limit, tc.rf, tc.n)
		if got := Share(int(tc.limit), int(tc.rf), int(tc.n)); int64(got) != want {
			t.Errorf("Share(%d,%d,%d) = %d, want %d", tc.limit, tc.rf, tc.n, got, want)
		}
	}
}
