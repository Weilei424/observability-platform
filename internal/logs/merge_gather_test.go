package logs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

var errGatherOutage = errors.New("outage")

func gatherHeads(n, tolerate int, seen *[]string) tolerantHeads {
	return tolerantHeads{
		heads: make([]Source, n),
		opts: HeadsOptions{
			Tolerate:  tolerate,
			Skippable: func(err error) bool { return errors.Is(err, errGatherOutage) },
			Observe:   func(h int, err error) { *seen = append(*seen, fmt.Sprint(h, " ", err)) },
		},
	}
}

func queued(rs ...headResult[[]string]) <-chan headResult[[]string] {
	ch := make(chan headResult[[]string], len(rs))
	for _, r := range rs {
		ch <- r
	}
	return ch
}

func wrapStrs(a []string) Source { return fixedSource{strs: a} }

// An outage that exceeds Tolerate is taken first, but a protocol error that is
// already in when the read is decided still wins, and is observed.
func TestGatherPrefersAQueuedProtocolErrorOverAnOutage(t *testing.T) {
	errBad := errors.New("bad answer")
	for _, tolerate := range []int{0, 1} {
		var seen []string
		rs := []headResult[[]string]{{i: 0, err: errGatherOutage}}
		if tolerate == 1 {
			rs = append(rs, headResult[[]string]{i: 1, err: errGatherOutage})
		}
		rs = append(rs, headResult[[]string]{i: 2, err: errBad})
		_, _, _, err := gather(context.Background(), gatherHeads(4, tolerate, &seen), queued(rs...), wrapStrs)
		if !errors.Is(err, errBad) {
			t.Fatalf("tolerate %d: err = %v, want the protocol error", tolerate, err)
		}
		if !slices.Contains(seen, "2 bad answer") {
			t.Errorf("tolerate %d: observed %v, want the protocol error observed", tolerate, seen)
		}
	}
}

// With only outages queued, the outage that exceeds Tolerate is returned
// without waiting for the heads still out.
func TestGatherReturnsTheOutageWhenNoProtocolErrorIsIn(t *testing.T) {
	var seen []string
	_, _, _, err := gather(context.Background(), gatherHeads(3, 0, &seen), queued(headResult[[]string]{i: 1, err: errGatherOutage}), wrapStrs)
	if !errors.Is(err, errGatherOutage) {
		t.Fatalf("err = %v, want the outage", err)
	}
	if !slices.Equal(seen, []string{"1 outage"}) {
		t.Errorf("observed %v, want only the outage", seen)
	}
}

// The first protocol error decides the read; queued answers after it are
// still observed but do not replace it.
func TestGatherKeepsTheFirstProtocolError(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	var seen []string
	_, _, _, err := gather(context.Background(), gatherHeads(4, 1, &seen), queued(
		headResult[[]string]{i: 3, err: first},
		headResult[[]string]{i: 0, a: []string{"x"}},
		headResult[[]string]{i: 1, err: second},
	), wrapStrs)
	if !errors.Is(err, first) {
		t.Fatalf("err = %v, want the first protocol error", err)
	}
	if want := []string{"3 first", "0 <nil>", "1 second"}; !slices.Equal(seen, want) {
		t.Errorf("observed %v, want %v", seen, want)
	}
}
