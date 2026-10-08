package logs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

var errOutage = errors.New("outage")

func outage(err error) bool { return errors.Is(err, errOutage) }

func headWith(t *testing.T, svc, line string, ts int64) Source {
	t.Helper()
	l := mustLabels(t, map[string]string{"service": svc})
	return staticSource{streams: []StreamData{{Labels: l, Entries: []LogEntry{{TimestampNs: ts, Line: line}}}}, names: []string{"service"}}
}

func TestMergeHeadsWithSkipsUpToTolerateOutages(t *testing.T) {
	good := headWith(t, "api", "x", 1)
	down := staticSource{err: errOutage}
	ctx := context.Background()
	var skipped []int
	src := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage, OnSkip: func(s []int) { skipped = s }}, down, good, good)
	got, err := src.SelectStreams(ctx, nil, 0, 10)
	if err != nil || len(got) != 1 || len(got[0].Entries) != 1 {
		t.Fatalf("one outage tolerated: %v %+v", err, got)
	}
	if !slices.Equal(skipped, []int{0}) {
		t.Errorf("OnSkip = %v, want [0]", skipped)
	}
	if _, err := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage}, down, down, good).SelectStreams(ctx, nil, 0, 10); !errors.Is(err, errOutage) {
		t.Fatalf("two outages with tolerance 1: err = %v", err)
	}
	if _, err := MergeHeadsWith(HeadsOptions{Tolerate: 2, Skippable: outage}, staticSource{err: errors.New("bad answer")}, good).SelectStreams(ctx, nil, 0, 10); err == nil {
		t.Fatal("a protocol error was skipped")
	}
	if _, err := MergeHeadsWith(HeadsOptions{}, down, good).SelectLabelNames(ctx); !errors.Is(err, errOutage) {
		t.Fatal("tolerance 0 skipped an outage")
	}
	if _, err := MergeHeadsWith(HeadsOptions{Tolerate: 1}, down, good).SelectLabelNames(ctx); !errors.Is(err, errOutage) {
		t.Fatal("nil Skippable skipped an outage")
	}
}

func TestMergeHeadsWithToleratesLabelReadsToo(t *testing.T) {
	good := staticSource{names: []string{"service"}}
	src := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage}, staticSource{err: errOutage}, good)
	names, err := src.SelectLabelNames(context.Background())
	if err != nil || !slices.Equal(names, []string{"service"}) {
		t.Fatalf("names = %v, %v", names, err)
	}
	vals, err := src.SelectLabelValues(context.Background(), "service")
	if err != nil || !slices.Equal(vals, []string{"service"}) {
		t.Fatalf("values = %v, %v", vals, err)
	}
}

func TestMergeHeadsWithNeverSkipsCancellationOrAllHeadsDown(t *testing.T) {
	good := headWith(t, "api", "x", 1)
	always := func(error) bool { return true }
	ctx := context.Background()
	for _, e := range []error{context.Canceled, fmt.Errorf("wrapped: %w", context.DeadlineExceeded)} {
		_, err := MergeHeadsWith(HeadsOptions{Tolerate: 2, Skippable: always}, staticSource{err: e}, good).SelectStreams(ctx, nil, 0, 10)
		if !errors.Is(err, e) {
			t.Errorf("%v was skipped: err = %v", e, err)
		}
	}
	down := staticSource{err: errOutage}
	if _, err := MergeHeadsWith(HeadsOptions{Tolerate: 2, Skippable: outage}, down, down).SelectStreams(ctx, nil, 0, 10); !errors.Is(err, errOutage) {
		t.Errorf("every head down returned %v, want the outage", err)
	}
}

func TestMergeHeadsWithObservesEveryHeadReadOnce(t *testing.T) {
	good := headWith(t, "api", "x", 1)
	var seen []string
	opts := HeadsOptions{Tolerate: 1, Skippable: outage, Observe: func(h int, err error) { seen = append(seen, fmt.Sprint(h, err)) }}
	if _, err := MergeHeadsWith(opts, good, staticSource{err: errOutage}, good).SelectStreams(context.Background(), nil, 0, 10); err != nil {
		t.Fatal(err)
	}
	if want := []string{"0 <nil>", "1 outage", "2 <nil>"}; !slices.Equal(seen, want) {
		t.Errorf("observed %v, want %v", seen, want)
	}
}

func TestMergeHeadsWithMatchesMergeHeadsWhenNothingFails(t *testing.T) {
	a, b, c := headWith(t, "a", "1", 1), headWith(t, "b", "2", 2), headWith(t, "a", "3", 3)
	ctx := context.Background()
	want, err := MergeHeads(a, b, c).SelectStreams(ctx, nil, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage}, a, b, c).SelectStreams(ctx, nil, 0, 10)
	if err != nil || fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Fatalf("got %+v (%v), want %+v", got, err, want)
	}
}
