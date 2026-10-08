package metrics_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

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
	src := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 1, Skippable: outage, OnSkip: func(s []int) { skipped = s }}, down, good, good)
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
	for _, e := range []error{context.Canceled, fmt.Errorf("wrapped: %w", context.DeadlineExceeded)} {
		_, err := metrics.MergeHeadsWith(metrics.HeadsOptions{Tolerate: 2, Skippable: always}, downSource{e}, good).Select(context.Background(), allParams)
		if !errors.Is(err, e) {
			t.Errorf("%v was skipped: err = %v", e, err)
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
