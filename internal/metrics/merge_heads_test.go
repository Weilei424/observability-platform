package metrics_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type downSource struct{ err error }

func (f downSource) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, f.err
}
func (f downSource) SelectLabelNames(context.Context) ([]string, error) { return nil, f.err }
func (f downSource) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, f.err
}

var errOutage = errors.New("outage")

// timeoutErr is what a per-request timeout looks like: an outage that also wraps DeadlineExceeded.
var timeoutErr = fmt.Errorf("%w: %w", errOutage, context.DeadlineExceeded)

func outage(err error) bool { return errors.Is(err, errOutage) }

func headWith(t *testing.T, name string, ts int64, v float64) metrics.Source {
	t.Helper()
	l, err := metrics.NewLabels(map[string]string{"__name__": name})
	if err != nil {
		t.Fatal(err)
	}
	s := metrics.NewMemoryStore()
	if err := s.Append(l, ts, v); err != nil {
		t.Fatal(err)
	}
	return s
}

var allParams = metrics.SelectParams{MinT: 0, MaxT: 10000}

func TestMergeHeadsWithSkipsUpToTolerateOutages(t *testing.T) {
	good := headWith(t, "m", 1000, 1)
	down := downSource{errOutage}
	var skipped []int
	src := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 1, Skippable: outage, OnSkip: func(_ context.Context, s []int) { skipped = s }}, down, good, good)
	sds, err := src.Select(context.Background(), allParams)
	if err != nil || len(sds) != 1 || len(sds[0].Samples) != 1 {
		t.Fatalf("one outage tolerated: %v %+v", err, sds)
	}
	if !slices.Equal(skipped, []int{0}) {
		t.Errorf("OnSkip = %v, want [0]", skipped)
	}
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 1, Skippable: outage}, down, down, good).Select(context.Background(), allParams); !errors.Is(err, errOutage) {
		t.Fatalf("two outages with tolerance 1: err = %v, want the outage", err)
	}
	protocol := downSource{errors.New("bad answer")}
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: outage}, protocol, good).Select(context.Background(), allParams); err == nil {
		t.Fatal("a protocol error was skipped")
	}
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{}, down, good).SelectLabelNames(context.Background()); !errors.Is(err, errOutage) {
		t.Fatal("tolerance 0 skipped an outage")
	}
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 1}, down, good).SelectLabelNames(context.Background()); !errors.Is(err, errOutage) {
		t.Fatal("nil Skippable skipped an outage")
	}
}

func TestMergeHeadsWithToleratesLabelReadsToo(t *testing.T) {
	good := headWith(t, "m", 1, 1)
	opts := metrics.HeadsOptions{Tolerate: 1, Skippable: outage}
	src := metrics.MergeHeadsWith(opts, downSource{errOutage}, good)
	names, err := src.SelectLabelNames(context.Background())
	if err != nil || !slices.Equal(names, []string{"__name__"}) {
		t.Fatalf("names = %v, %v", names, err)
	}
	vals, err := src.SelectLabelValues(context.Background(), "__name__")
	if err != nil || !slices.Equal(vals, []string{"m"}) {
		t.Fatalf("values = %v, %v", vals, err)
	}
}

func TestMergeHeadsWithNeverSkipsCancellationOrAllHeadsDown(t *testing.T) {
	good := headWith(t, "m", 1, 1)
	always := func(error) bool { return true }
	// context.Canceled is never skipped, even with a live caller context.
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: always}, downSource{context.Canceled}, good).Select(context.Background(), allParams); !errors.Is(err, context.Canceled) {
		t.Errorf("Canceled was skipped: err = %v", err)
	}
	// A done caller context disables skipping.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	for _, ctx := range []context.Context{cancelled, expired} {
		if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: outage}, downSource{timeoutErr}, good).Select(ctx, allParams); !errors.Is(err, errOutage) {
			t.Errorf("skipped with a done caller context: err = %v", err)
		}
		if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: outage}, downSource{timeoutErr}, good).SelectLabelNames(ctx); !errors.Is(err, errOutage) {
			t.Errorf("label read skipped with a done caller context: err = %v", err)
		}
	}
	if _, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: outage}, downSource{errOutage}, downSource{errOutage}).Select(context.Background(), allParams); !errors.Is(err, errOutage) {
		t.Errorf("every head down returned %v, want the outage rather than an empty answer", err)
	}
}

func TestMergeHeadsWithObservesEveryHeadReadOnce(t *testing.T) {
	good := headWith(t, "m", 1, 1)
	var seen []string
	opts := metrics.HeadsOptions{Tolerate: 1, Skippable: outage, Observe: func(h int, err error) { seen = append(seen, fmt.Sprint(h, err)) }}
	if _, err := metrics.MergeHeadsWith(opts, good, downSource{errOutage}, good).Select(context.Background(), allParams); err != nil {
		t.Fatal(err)
	}
	if want := []string{"0 <nil>", "1 outage", "2 <nil>"}; !slices.Equal(seen, want) {
		t.Errorf("observed %v, want %v", seen, want)
	}
}

func TestMergeHeadsWithMatchesMergeHeadsWhenNothingFails(t *testing.T) {
	a, b, c := headWith(t, "x", 1, 1), headWith(t, "y", 2, 2), headWith(t, "x", 3, 3)
	want, err := metrics.MergeHeads(a, b, c).Select(context.Background(), allParams)
	if err != nil {
		t.Fatal(err)
	}
	got, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 1, Skippable: outage}, a, b, c).Select(context.Background(), allParams)
	if err != nil || fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Fatalf("got %+v (%v), want %+v", got, err, want)
	}
}

// Overwrite generations come from each ingester's own wall clock, and the merge
// keeps the highest generation across replicas. When a replica misses an
// overwrite (here B: v2 reached only a quorum, A and C) and its clock runs
// ahead, its stale v1 can outrank v2: an overwrite is only reliable when it
// follows the original by more than the clock skew.
func TestMergeHeadsOverwriteSkewWindow(t *testing.T) {
	const ms = int64(1000) // generation clock unit is microseconds
	run := func(gapMs int64) float64 {
		t.Helper()
		l, err := metrics.NewLabels(map[string]string{"__name__": "m"})
		if err != nil {
			t.Fatal(err)
		}
		now := [3]int64{1_000_000, 1_000_000 + 5*ms, 1_000_000} // A, C on time; B is 5ms ahead
		var stores []*metrics.MemoryStore
		var heads []metrics.Source
		for i := range now {
			s := metrics.NewMemoryStore()
			s.SetGenerationClock(func() int64 { return now[i] })
			stores = append(stores, s)
			heads = append(heads, s)
		}
		write := func(v float64, to ...int) {
			for _, i := range to {
				if err := stores[i].Append(l, 1000, v); err != nil {
					t.Fatal(err)
				}
			}
		}
		write(1, 0, 1, 2)
		for i := range now {
			now[i] += gapMs * ms
		}
		write(2, 0, 2) // B missed the overwrite
		sds, err := metrics.MergeHeads(heads...).Select(context.Background(), allParams)
		if err != nil || len(sds) != 1 || len(sds[0].Samples) != 1 {
			t.Fatalf("select: %v %+v", err, sds)
		}
		return sds[0].Samples[0].Value
	}
	if v := run(1); v != 1 {
		t.Errorf("overwrite 1ms later with 5ms skew = %v, want the stale 1 (documented limitation)", v)
	}
	if v := run(10); v != 2 {
		t.Errorf("overwrite 10ms later with 5ms skew = %v, want 2", v)
	}
}

func TestMergeHeadsWithSkipsPerRequestTimeoutWhileCallerIsLive(t *testing.T) {
	good := headWith(t, "m", 1000, 1)
	opts := metrics.HeadsOptions{Tolerate: 1, Skippable: outage}
	src := metrics.MergeHeadsWith(opts, downSource{timeoutErr}, good)
	sds, err := src.Select(context.Background(), allParams)
	if err != nil || len(sds) != 1 {
		t.Fatalf("hung head not skipped: %v %+v", err, sds)
	}
	if _, err := src.SelectLabelNames(context.Background()); err != nil {
		t.Fatalf("label names: %v", err)
	}
	if _, err := src.SelectLabelValues(context.Background(), "__name__"); err != nil {
		t.Fatalf("label values: %v", err)
	}
}

// Two writes stamped the same generation -- two gateways in one microsecond --
// resolve to the same survivor on every replica, whatever order each replica
// received them in, alone and merged.
func TestEqualGenerationsResolveTheSameEverywhere(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{"__name__": "tie"})
	const gen = 1758600000000000
	replica := func(vals ...float64) *metrics.MemoryStore {
		s := metrics.NewMemoryStore()
		for _, v := range vals {
			if err := s.AppendGen(l, 1000, v, gen); err != nil {
				t.Fatal(err)
			}
		}
		return s
	}
	value := func(src metrics.Source) float64 {
		sds, err := src.Select(context.Background(), allParams)
		if err != nil || len(sds) != 1 || len(sds[0].Samples) != 1 {
			t.Fatalf("select: %v %+v", err, sds)
		}
		return sds[0].Samples[0].Value
	}
	a, b := replica(1, 2), replica(2, 1)
	va, vb := value(a), value(b)
	if va != vb {
		t.Fatalf("replicas that got the tie in opposite orders answer %v and %v", va, vb)
	}
	if m := value(metrics.MergeHeads(a, b)); m != va {
		t.Errorf("merged answer %v, want the replicas' %v", m, va)
	}
	if m := value(metrics.MergeHeads(b, a)); m != va {
		t.Errorf("merged the other way %v, want %v", m, va)
	}
}
