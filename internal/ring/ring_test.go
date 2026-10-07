package ring

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

func members(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("http://ingester-%d:8080", i)
	}
	return out
}

func mustNew(t *testing.T, m []string) *Ring {
	t.Helper()
	r, err := New(m)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The golden table pins placement: a change to the token or key hashing would
// silently move every series between ingesters, so it must fail here first.
func TestGoldenPlacement(t *testing.T) {
	r := mustNew(t, members(3))
	for _, c := range []struct {
		key  uint64
		want string
	}{
		{0x0, "http://ingester-1:8080"},
		{0x1, "http://ingester-2:8080"},
		{0x2a, "http://ingester-1:8080"},
		{0x100000000, "http://ingester-1:8080"},
		{0xdeadbeef, "http://ingester-0:8080"},
		{0x8000000000000007, "http://ingester-0:8080"},
		{math.MaxUint64, "http://ingester-1:8080"},
	} {
		if got := r.Owner(c.key); got != c.want {
			t.Errorf("Owner(%#x) = %s, want %s", c.key, got, c.want)
		}
	}
}

func TestPlacementIgnoresListOrder(t *testing.T) {
	a := mustNew(t, []string{"http://a:1", "http://b:1", "http://c:1"})
	b := mustNew(t, []string{"http://c:1", "http://a:1", "http://b:1"})
	for k := uint64(0); k < 10000; k++ {
		if a.Owner(k) != b.Owner(k) {
			t.Fatalf("Owner(%d) depends on list order", k)
		}
	}
}

func TestRingHashIgnoresOrder(t *testing.T) {
	a := mustNew(t, []string{"http://a:1", "http://b:1"})
	b := mustNew(t, []string{"http://b:1", "http://a:1"})
	c := mustNew(t, []string{"http://a:1", "http://c:1"})
	if a.Hash() != b.Hash() {
		t.Errorf("Hash differs by order: %s vs %s", a.Hash(), b.Hash())
	}
	if a.Hash() == c.Hash() {
		t.Errorf("Hash equal for different sets: %s", a.Hash())
	}
	if len(a.Hash()) != 8 {
		t.Errorf("Hash %q, want 8 hex characters", a.Hash())
	}
}

func TestBalance(t *testing.T) {
	const keys = 100000
	for _, n := range []int{3, 5} {
		r := mustNew(t, members(n))
		count := map[string]int{}
		for k := uint64(0); k < keys; k++ {
			count[r.Owner(k*2654435761)]++
		}
		mean := float64(keys) / float64(n)
		for m, c := range count {
			if d := float64(c) / mean; d < 0.75 || d > 1.25 {
				t.Errorf("n=%d: %s owns %.2f of the mean, want within ±25%%", n, m, d)
			}
		}
		if len(count) != n {
			t.Errorf("n=%d: only %d members own keys", n, len(count))
		}
	}
}

func TestAddingAMemberMovesOnlyItsShare(t *testing.T) {
	const keys = 100000
	for _, n := range []int{3, 5} {
		before, after := mustNew(t, members(n)), mustNew(t, members(n+1))
		added := members(n + 1)[n]
		moved := 0
		for k := uint64(0); k < keys; k++ {
			x, y := before.Owner(k*2654435761), after.Owner(k*2654435761)
			if x == y {
				continue
			}
			moved++
			if y != added {
				t.Fatalf("key moved from %s to %s, not to the added member", x, y)
			}
		}
		share, want := float64(moved)/keys, 1/float64(n+1)
		if share < want-0.05 || share > want+0.05 {
			t.Errorf("n=%d→%d: %.3f of keys moved, want %.3f ± 0.05", n, n+1, share, want)
		}
	}
}

func TestRemovingAMemberMovesOnlyItsKeys(t *testing.T) {
	before, after := mustNew(t, members(4)), mustNew(t, members(3)) // ingester-3 removed
	for k := uint64(0); k < 100000; k++ {
		x, y := before.Owner(k*2654435761), after.Owner(k*2654435761)
		if x != y && x != "http://ingester-3:8080" {
			t.Fatalf("key owned by %s moved to %s although %s stayed", x, y, x)
		}
	}
}

func TestSingleMemberOwnsEverything(t *testing.T) {
	r := mustNew(t, []string{"http://only:1"})
	for _, k := range []uint64{0, 1, 1 << 40, math.MaxUint64} {
		if got := r.Owner(k); got != "http://only:1" {
			t.Errorf("Owner(%d) = %s", k, got)
		}
	}
}

// Two members whose tokens collide: the tie resolves to the member that sorts
// first, so the ring stays a function of the member set.
func TestTokenTieGoesToTheMemberSortingFirst(t *testing.T) {
	r := &Ring{members: []string{"a", "b"}, tokens: []uint64{100, 100}, owners: []int{0, 1}}
	if got := r.lookup(100); got != "a" {
		t.Errorf("tie at exactly the token = %s, want a", got)
	}
	built := mustNew(t, []string{"b", "a"})
	for i := 1; i < len(built.tokens); i++ {
		if built.tokens[i] == built.tokens[i-1] && built.owners[i] < built.owners[i-1] {
			t.Fatalf("equal tokens at %d not ordered by member", i)
		}
	}
}

func TestNewRefusesBadLists(t *testing.T) {
	for _, m := range [][]string{nil, {}, {""}, {"http://a:1", "http://a:1"}, {"http://u:secret@a:1", "http://u:secret@a:1"}} {
		if _, err := New(m); err == nil {
			t.Errorf("New(%q) succeeded, want an error", m)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("error echoes credentials: %v", err)
		}
	}
}

func TestMembersIsASortedCopy(t *testing.T) {
	r := mustNew(t, []string{"http://b:1", "http://a:1"})
	got := r.Members()
	if got[0] != "http://a:1" || got[1] != "http://b:1" {
		t.Errorf("Members() = %v, want sorted", got)
	}
	got[0] = "mutated"
	if r.Members()[0] != "http://a:1" {
		t.Error("Members() returned the ring's own slice")
	}
}

func TestReplicasAreDistinctAndStartAtTheOwner(t *testing.T) {
	r := mustNew(t, []string{"http://a:1", "http://b:1", "http://c:1", "http://d:1", "http://e:1"})
	for key := uint64(0); key < 2000; key++ {
		got, err := r.Replicas(key, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0] != r.Owner(key) {
			t.Fatalf("Replicas(%d, 3) = %v, want 3 starting at owner %s", key, got, r.Owner(key))
		}
		if got[0] == got[1] || got[1] == got[2] || got[0] == got[2] {
			t.Fatalf("Replicas(%d, 3) = %v has a repeat", key, got)
		}
	}
}

func TestReplicasOfOneIsTheOwner(t *testing.T) {
	r := mustNew(t, []string{"http://a:1", "http://b:1", "http://c:1"})
	for key := uint64(0); key < 500; key++ {
		got, _ := r.Replicas(key, 1)
		if len(got) != 1 || got[0] != r.Owner(key) {
			t.Fatalf("Replicas(%d, 1) = %v, want [%s]", key, got, r.Owner(key))
		}
	}
}

func TestReplicasOfEveryMemberIsAllOfThem(t *testing.T) {
	m := []string{"http://a:1", "http://b:1", "http://c:1"}
	r := mustNew(t, m)
	got, _ := r.Replicas(42, 3)
	slices.Sort(got)
	if !slices.Equal(got, m) {
		t.Fatalf("Replicas(42, 3) = %v, want every member", got)
	}
}

func TestReplicasRefusesMoreThanTheMembers(t *testing.T) {
	r := mustNew(t, []string{"http://a:1", "http://b:1"})
	if _, err := r.Replicas(1, 3); err == nil {
		t.Fatal("Replicas(1, 3) on two members returned no error")
	}
	if _, err := r.Replicas(1, 0); err == nil {
		t.Fatal("Replicas(1, 0) returned no error")
	}
}

func TestGoldenReplicas(t *testing.T) {
	r := mustNew(t, members(3))
	for _, c := range []struct {
		key  uint64
		want []string
	}{
		{0, []string{"http://ingester-1:8080", "http://ingester-0:8080", "http://ingester-2:8080"}},
		{1, []string{"http://ingester-2:8080", "http://ingester-0:8080", "http://ingester-1:8080"}},
		{0x2a, []string{"http://ingester-1:8080", "http://ingester-0:8080", "http://ingester-2:8080"}},
		{1 << 40, []string{"http://ingester-1:8080", "http://ingester-0:8080", "http://ingester-2:8080"}},
		{math.MaxUint64, []string{"http://ingester-1:8080", "http://ingester-2:8080", "http://ingester-0:8080"}},
	} {
		got, err := r.Replicas(c.key, 3)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("Replicas(%#x, 3) = %q, want %q", c.key, got, c.want)
		}
	}
}
