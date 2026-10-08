package logs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

var errOutage = errors.New("outage")

// timeoutErr is what a per-request timeout looks like: an outage that also wraps DeadlineExceeded.
var timeoutErr = fmt.Errorf("%w: %w", errOutage, context.DeadlineExceeded)

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
	if _, err := MergeHeadsWith(HeadsOptions{Tolerate: 2, Skippable: always}, staticSource{err: context.Canceled}, good).SelectStreams(ctx, nil, 0, 10); !errors.Is(err, context.Canceled) {
		t.Errorf("Canceled was skipped: err = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancel2 := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel2()
	for _, c := range []context.Context{cancelled, expired} {
		h := MergeHeadsWith(HeadsOptions{Tolerate: 2, Skippable: outage}, staticSource{err: timeoutErr}, good)
		if _, err := h.SelectStreams(c, nil, 0, 10); !errors.Is(err, errOutage) {
			t.Errorf("skipped with a done caller context: err = %v", err)
		}
		if _, err := h.SelectLabelNames(c); !errors.Is(err, errOutage) {
			t.Errorf("label read skipped with a done caller context: err = %v", err)
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

func TestMergeHeadsWithSkipsPerRequestTimeoutWhileCallerIsLive(t *testing.T) {
	good := staticSource{streams: headWith(t, "api", "x", 1).(staticSource).streams, names: []string{"service"}}
	src := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage}, staticSource{err: timeoutErr}, good)
	ctx := context.Background()
	got, err := src.SelectStreams(ctx, nil, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("hung head not skipped: %v %+v", err, got)
	}
	if _, err := src.SelectLabelNames(ctx); err != nil {
		t.Fatalf("label names: %v", err)
	}
	if _, err := src.SelectLabelValues(ctx, "service"); err != nil {
		t.Fatalf("label values: %v", err)
	}
}
