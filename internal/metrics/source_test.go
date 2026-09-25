package metrics

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func mustLabelsInternal(t *testing.T, m map[string]string) Labels {
	t.Helper()
	l, err := NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels(%v): %v", m, err)
	}
	return l
}

func TestSelectParamsValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    SelectParams
		ok   bool
	}{
		{"samples", SelectParams{MinT: 0, MaxT: 10}, true},
		{"samples with anchor", SelectParams{MinT: 0, MaxT: 10, Anchor: true}, true},
		{"series only bounded", SelectParams{MinT: 0, MaxT: 10, SeriesOnly: true}, true},
		{"series only any time", SelectParams{SeriesOnly: true, AnyTime: true}, true},
		{"any time needs series only", SelectParams{AnyTime: true}, false},
		{"anchor excludes series only", SelectParams{SeriesOnly: true, Anchor: true}, false},
	} {
		err := tc.p.Validate()
		if tc.ok && err != nil {
			t.Errorf("%s: Validate() = %v, want nil", tc.name, err)
		}
		if !tc.ok && !errors.Is(err, ErrInvalidSelect) {
			t.Errorf("%s: Validate() = %v, want ErrInvalidSelect", tc.name, err)
		}
	}
}

func TestDedupByGenerationKeepsTheLastWritePerTimestamp(t *testing.T) {
	in := []Sample{
		{TimestampMs: 1, Value: 10, Gen: 1},
		{TimestampMs: 1, Value: 30, Gen: 3},
		{TimestampMs: 1, Value: 20, Gen: 2},
		{TimestampMs: 2, Value: 40, Gen: 1},
	}
	got := dedupByGeneration(in)
	want := []Sample{{TimestampMs: 1, Value: 30, Gen: 3}, {TimestampMs: 2, Value: 40, Gen: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dedupByGeneration = %+v, want %+v", got, want)
	}
}

func TestLaterSample(t *testing.T) {
	a := &Sample{TimestampMs: 5, Gen: 1}
	b := &Sample{TimestampMs: 5, Gen: 2}
	c := &Sample{TimestampMs: 6, Gen: 0}
	if laterSample(nil, nil) != nil {
		t.Error("laterSample(nil, nil) != nil")
	}
	if laterSample(a, nil) != a || laterSample(nil, a) != a {
		t.Error("a nil side must yield the other side")
	}
	if laterSample(a, b) != b || laterSample(b, a) != b {
		t.Error("an equal timestamp must be decided by the higher generation")
	}
	if laterSample(b, c) != c {
		t.Error("the later timestamp must win regardless of generation")
	}
}

// The per-series adapter is the reference definition of Select. These cases pin
// that definition; Task 3's native implementations are checked against it.
func TestPerSeriesSourceSelect(t *testing.T) {
	ms := NewMemoryStore()
	a := mustLabelsInternal(t, map[string]string{"__name__": "m", "k": "a"})
	b := mustLabelsInternal(t, map[string]string{"__name__": "m", "k": "b"})
	c := mustLabelsInternal(t, map[string]string{"__name__": "m", "k": "c"})
	for _, s := range []struct {
		l  Labels
		ts int64
		v  float64
	}{
		{a, 100, 1}, {a, 200, 2}, {a, 200, 22}, {a, 300, 3}, // a: overwrite at 200
		{b, 50, 5},  // b: only before any range below starts
		{c, 900, 9}, // c: only after it
	} {
		if err := ms.Append(s.l, s.ts, s.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	src := perSeriesSource{s: ms}
	ctx := context.Background()
	sel := Selector{MetricName: "m"}

	byKey := func(sds []SeriesData) map[string]SeriesData {
		out := map[string]SeriesData{}
		for _, sd := range sds {
			k, _ := sd.Labels.Get("k")
			out[k] = sd
		}
		return out
	}

	got, err := src.Select(ctx, SelectParams{Selector: sel, MinT: 150, MaxT: 300, Anchor: true})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	m := byKey(got)
	if len(m) != 2 {
		t.Fatalf("anchored select returned series %v, want a and b (c has nothing at or before 300)", m)
	}
	if s := m["a"].Samples; len(s) != 2 || s[0].Value != 22 || s[1].Value != 3 {
		t.Errorf("a samples = %+v, want the overwrite 22 at 200, then 3 at 300", s)
	}
	if m["a"].Anchor == nil || m["a"].Anchor.Value != 1 {
		t.Errorf("a anchor = %+v, want the sample at 100", m["a"].Anchor)
	}
	if m["b"].Anchor == nil || m["b"].Anchor.Value != 5 || len(m["b"].Samples) != 0 {
		t.Errorf("b = %+v, want only its anchor at 50", m["b"])
	}

	got, _ = src.Select(ctx, SelectParams{Selector: sel, MinT: 150, MaxT: 300})
	if m := byKey(got); len(m) != 1 || m["a"].Anchor != nil {
		t.Errorf("unanchored select = %v, want only a, without an anchor", m)
	}

	got, _ = src.Select(ctx, SelectParams{Selector: sel, MinT: 150, MaxT: 300, SeriesOnly: true})
	if m := byKey(got); len(m) != 1 || m["a"].Samples != nil {
		t.Errorf("series-only bounded = %v, want a alone with no samples", m)
	}

	got, _ = src.Select(ctx, SelectParams{Selector: sel, SeriesOnly: true, AnyTime: true})
	if m := byKey(got); len(m) != 3 {
		t.Errorf("series-only any time = %v, want a, b, c", m)
	}

	got, _ = src.Select(ctx, SelectParams{Selector: sel, MinT: 300, MaxT: 299, Anchor: true})
	if len(got) != 0 {
		t.Errorf("empty range selected %v, want nothing", got)
	}

	got, _ = src.Select(ctx, SelectParams{Selector: sel, MinT: math.MinInt64, MaxT: 100, Anchor: true})
	if m := byKey(got); len(m) != 2 || m["a"].Anchor != nil {
		t.Errorf("MinInt64 range = %v, want a and b with no anchor (nothing precedes MinInt64)", m)
	}

	if _, err := src.Select(ctx, SelectParams{Selector: sel, AnyTime: true}); !errors.Is(err, ErrInvalidSelect) {
		t.Errorf("invalid params: err = %v, want ErrInvalidSelect", err)
	}

	names, _ := src.SelectLabelNames(ctx)
	if !reflect.DeepEqual(names, []string{"__name__", "k"}) {
		t.Errorf("SelectLabelNames = %v", names)
	}
	vals, _ := src.SelectLabelValues(ctx, "k")
	if !reflect.DeepEqual(vals, []string{"a", "b", "c"}) {
		t.Errorf("SelectLabelValues = %v", vals)
	}
}

func TestPerSeriesSourceSelectHonoursCancellation(t *testing.T) {
	ms := NewMemoryStore()
	if err := ms.Append(mustLabelsInternal(t, map[string]string{"__name__": "m"}), 1, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := perSeriesSource{s: ms}.Select(ctx, SelectParams{Selector: Selector{MetricName: "m"}, MinT: 0, MaxT: 10})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Select on a cancelled context = %v, want context.Canceled", err)
	}
}
