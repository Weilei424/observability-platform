package logs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
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
	src := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage, OnSkip: func(_ context.Context, s []int) { skipped = s }}, down, good, good)
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
	// staticSource ignores ctx, so both heads answer and the outage is deterministic:
	// had skipping happened, the read would succeed.
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
	var mu sync.Mutex
	var seen []string
	opts := HeadsOptions{Tolerate: 1, Skippable: outage, Observe: func(h int, err error) {
		mu.Lock()
		seen = append(seen, fmt.Sprint(h, err))
		mu.Unlock()
	}}
	if _, err := MergeHeadsWith(opts, good, staticSource{err: errOutage}, good).SelectStreams(context.Background(), nil, 0, 10); err != nil {
		t.Fatal(err)
	}
	slices.Sort(seen)
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

// slowSource answers its inner source after d, or ctx's error if ctx ends first.
type slowSource struct {
	inner Source
	d     time.Duration
}

func (s slowSource) wait(ctx context.Context) error {
	select {
	case <-time.After(s.d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s slowSource) SelectStreams(ctx context.Context, m []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.SelectStreams(ctx, m, minTs, maxTs)
}
func (s slowSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.SelectLabelNames(ctx)
}
func (s slowSource) SelectLabelValues(ctx context.Context, n string) ([]string, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.SelectLabelValues(ctx, n)
}

func TestMergeHeadsWithReadsHeadsInParallel(t *testing.T) {
	heads := []Source{
		slowSource{headWith(t, "a", "1", 1), 200 * time.Millisecond},
		slowSource{headWith(t, "b", "2", 2), 200 * time.Millisecond},
		slowSource{headWith(t, "c", "3", 3), 200 * time.Millisecond},
	}
	start := time.Now()
	if _, err := MergeHeadsWith(HeadsOptions{}, heads...).SelectStreams(context.Background(), nil, 0, 10); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 550*time.Millisecond {
		t.Errorf("three 200ms heads took %v; parallel reads take about one", d)
	}
}

// Whatever order heads answer in, the answer is the sequential fold's.
func TestMergeHeadsWithAnswerIgnoresArrivalOrder(t *testing.T) {
	a, b, c := headWith(t, "x", "1", 1), headWith(t, "y", "2", 2), headWith(t, "x", "3", 3)
	want, err := MergeHeads(a, b, c).SelectStreams(context.Background(), nil, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, delays := range [][3]time.Duration{{0, 20, 40}, {40, 20, 0}, {20, 0, 40}} {
		heads := []Source{
			slowSource{a, delays[0] * time.Millisecond},
			slowSource{b, delays[1] * time.Millisecond},
			slowSource{c, delays[2] * time.Millisecond},
		}
		got, err := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage}, heads...).SelectStreams(context.Background(), nil, 0, 10)
		if err != nil || fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
			t.Fatalf("delays %v: got %+v (%v), want %+v", delays, got, err, want)
		}
	}
}

// A protocol error decides the read at once: a hung head is cancelled, not
// waited for, and its cancellation is not observed as an outage.
func TestMergeHeadsWithStopsEarlyAndDoesNotObserveCancelled(t *testing.T) {
	hung := slowSource{headWith(t, "m", "1", 1), time.Hour}
	var mu sync.Mutex
	var seen []string
	opts := HeadsOptions{Tolerate: 1, Skippable: outage, Observe: func(h int, err error) {
		mu.Lock()
		seen = append(seen, fmt.Sprint(h, err))
		mu.Unlock()
	}}
	start := time.Now()
	_, err := MergeHeadsWith(opts, hung, staticSource{err: errors.New("bad answer")}).SelectStreams(context.Background(), nil, 0, 10)
	if err == nil || errors.Is(err, errOutage) {
		t.Fatalf("err = %v, want the protocol error", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v: the hung head was waited for", d)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != "1 bad answer" {
		t.Errorf("observed %v, want only the protocol error", seen)
	}
}

// Tolerate+1 outages decide the read without waiting for a slow head.
func TestMergeHeadsWithFailsAsSoonAsToleranceIsExceeded(t *testing.T) {
	slow := slowSource{headWith(t, "m", "1", 1), time.Hour}
	start := time.Now()
	_, err := MergeHeadsWith(HeadsOptions{Tolerate: 1, Skippable: outage},
		staticSource{err: errOutage}, slow, staticSource{err: errOutage}).SelectStreams(context.Background(), nil, 0, 10)
	if !errors.Is(err, errOutage) || time.Since(start) > time.Second {
		t.Fatalf("err = %v after %v, want the outage at once", err, time.Since(start))
	}
}

// A caller that cancels mid-read gets context.Canceled, and no head is observed as an outage.
func TestMergeHeadsWithCallerCancelIsNotAnOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	var observed atomic.Int32
	opts := HeadsOptions{Tolerate: 1, Skippable: outage, Observe: func(int, error) { observed.Add(1) }}
	_, err := MergeHeadsWith(opts, slowSource{headWith(t, "m", "1", 1), time.Hour}, slowSource{headWith(t, "n", "1", 1), time.Hour}).SelectStreams(ctx, nil, 0, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := observed.Load(); n != 0 {
		t.Errorf("observed %d head reads; a cancelled read is never observed", n)
	}
}

func TestMergeHeadsWithOnSkipIsSorted(t *testing.T) {
	good := headWith(t, "m", "1", 1)
	var skipped []int
	opts := HeadsOptions{Tolerate: 2, Skippable: outage, OnSkip: func(_ context.Context, s []int) { skipped = s }}
	heads := []Source{slowSource{staticSource{err: errOutage}, 30 * time.Millisecond}, good, staticSource{err: errOutage}}
	if _, err := MergeHeadsWith(opts, heads...).SelectStreams(context.Background(), nil, 0, 10); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(skipped, []int{0, 2}) {
		t.Errorf("OnSkip = %v, want [0 2]", skipped)
	}
}
