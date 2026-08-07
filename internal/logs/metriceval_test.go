package logs

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// metricFake is a Reader with hand-placed streams, so every timestamp in these
// tests is exact rather than relative to a wall clock.
type metricFake struct {
	labels  map[StreamID]StreamLabels
	entries map[StreamID][]LogEntry
}

func newMetricFake(t *testing.T, streams map[string][]LogEntry) *metricFake {
	t.Helper()
	f := &metricFake{labels: map[StreamID]StreamLabels{}, entries: map[StreamID][]LogEntry{}}
	for spec, es := range streams {
		lbls, err := NewStreamLabels(parseLabelSpec(t, spec))
		if err != nil {
			t.Fatalf("labels %q: %v", spec, err)
		}
		id := StreamIDOf(lbls)
		f.labels[id] = lbls
		for _, e := range es {
			e.StreamID = id
			f.entries[id] = append(f.entries[id], e)
		}
	}
	return f
}

// parseLabelSpec turns `service=api,level=info` into a label map.
func parseLabelSpec(t *testing.T, spec string) map[string]string {
	t.Helper()
	m := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("bad label spec %q", spec)
		}
		m[k] = v
	}
	return m
}

func (f *metricFake) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	var out []StreamID
	for id, l := range f.labels {
		if streamMatches(l, matchers) {
			out = append(out, id)
		}
	}
	return out
}

func (f *metricFake) StreamEntries(_ context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	var out []LogEntry
	for _, e := range f.entries[id] {
		if e.TimestampNs >= minTs && e.TimestampNs <= maxTs {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *metricFake) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	l, ok := f.labels[id]
	return l, ok
}
func (f *metricFake) LabelNames() []string        { return nil }
func (f *metricFake) LabelValues(string) []string { return nil }

func entry(ts int64, line string) LogEntry { return LogEntry{TimestampNs: ts, Line: line} }

func mustParse(t *testing.T, q string) MetricQuery {
	t.Helper()
	mq, err := ParseMetricQuery(q)
	if err != nil {
		t.Fatalf("ParseMetricQuery(%q): %v", q, err)
	}
	return mq
}

const sec = int64(time.Second)

// TestEvalMetricRange_WindowBoundaries pins the boundary rule the design takes
// from upstream: every tick's window is (t-range, t] — start-exclusive,
// end-inclusive — including the final tick, so an entry landing exactly on `end`
// is counted there.
//
// This test previously asserted the opposite, on the mistaken reasoning that
// upstream's sample reads are half-open on the right. They are, but upstream then
// adds a leap nanosecond to the selected end for exactly this reason: "add leap
// nanosecond to endTs to include lines exactly at endTs. range iterators work on
// start exclusive, end inclusive ranges" (pkg/logql/evaluator.go). Dropping the
// entry at `end` made the last tick silently narrower than every other tick.
func TestEvalMetricRange_WindowBoundaries(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api": {
			entry(100*sec, "at t-range"), // == t-range for the tick at 160s
			entry(160*sec, "at t"),
			entry(200*sec, "at end"),
		},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"}[60s])`)

	series, err := e.EvalMetricRange(context.Background(), q, 160*sec, 200*sec, 40*sec)
	if err != nil {
		t.Fatalf("EvalMetricRange: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("got %d series, want 1: %+v", len(series), series)
	}
	// Tick 160s: window (100s, 160s] holds only "at t" — the entry at exactly
	// 100s is excluded by the open lower bound.
	// Tick 200s: window (140s, 200s] holds "at t" and "at end" — the entry at
	// exactly 200s counts, because the window closes on its tick.
	want := []MetricPoint{{160 * sec, 1}, {200 * sec, 2}}
	assertPoints(t, series[0].Points, want)
}

// TestEvalMetricRange_TicksAndGaps pins the tick sequence for an end that is not
// step-aligned, and that an empty window produces no point at all.
func TestEvalMetricRange_TicksAndGaps(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api": {entry(10*sec, "a"), entry(11*sec, "b")},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"}[5s])`)

	series, err := e.EvalMetricRange(context.Background(), q, 0, 25*sec, 10*sec)
	if err != nil {
		t.Fatalf("EvalMetricRange: %v", err)
	}
	// Ticks are 0, 10s, 20s — 25s is not a tick, and 30s is past end.
	// Tick 0:   window (-5s, 0]   → empty, no point.
	// Tick 10s: window (5s, 10s]  → "a".
	// Tick 20s: window (15s, 20s] → empty, no point.
	if len(series) != 1 {
		t.Fatalf("got %d series, want 1", len(series))
	}
	assertPoints(t, series[0].Points, []MetricPoint{{10 * sec, 1}})
}

// TestEvalMetricRange_EmptyResult proves a stream with no matching entries yields
// no series rather than a series of zeros.
func TestEvalMetricRange_EmptyResult(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{"service=api": {entry(10*sec, "a")}})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"} |= "nothing matches" [5s])`)

	series, err := e.EvalMetricRange(context.Background(), q, 0, 30*sec, 10*sec)
	if err != nil {
		t.Fatalf("EvalMetricRange: %v", err)
	}
	if len(series) != 0 {
		t.Fatalf("got %d series, want 0: %+v", len(series), series)
	}
}

// TestEvalMetricRange_Ops covers the value of every supported op over the same
// data, including the zero that bytes_over_time must emit for empty lines — a
// real zero, not the gap an empty window produces.
func TestEvalMetricRange_Ops(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api": {entry(10*sec, "abc"), entry(10*sec, "dé")}, // "dé" is 3 bytes
	})
	empty := newMetricFake(t, map[string][]LogEntry{"service=api": {entry(10*sec, "")}})

	cases := []struct {
		q    string
		fake *metricFake
		want float64
	}{
		{`count_over_time({service="api"}[10s])`, f, 2},
		{`bytes_over_time({service="api"}[10s])`, f, 6},     // 3 + 3 bytes
		{`rate({service="api"}[10s])`, f, 0.2},              // 2 entries / 10s
		{`bytes_rate({service="api"}[10s])`, f, 0.6},        // 6 bytes / 10s
		{`bytes_over_time({service="api"}[10s])`, empty, 0}, // non-empty window, zero bytes
	}
	for _, tc := range cases {
		eng := NewQueryEngine(tc.fake)
		series, err := eng.EvalMetricRange(context.Background(), mustParse(t, tc.q), 10*sec, 10*sec, sec)
		if err != nil {
			t.Fatalf("%s: %v", tc.q, err)
		}
		if len(series) != 1 || len(series[0].Points) != 1 {
			t.Fatalf("%s: got %+v, want one series with one point", tc.q, series)
		}
		if got := series[0].Points[0].Value; math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: value = %v, want %v", tc.q, got, tc.want)
		}
	}
}

// TestEvalMetricRange_Grouping covers every output-label rule at once, including
// the one that is forced rather than stylistic: a stream carrying level="" groups
// with a stream that has no level, because both render to the same label set.
func TestEvalMetricRange_Grouping(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info":  {entry(10*sec, "a")},
		"service=api,level=error": {entry(10*sec, "b")},
		"service=api,level=":      {entry(10*sec, "c")},
		"service=api,region=eu":   {entry(10*sec, "d")},
	})
	e := NewQueryEngine(f)

	// sum by (level): info=1, error=1, and one label-less group holding the
	// empty-level stream plus the stream with no level at all.
	series, err := e.EvalMetricRange(context.Background(), mustParse(t,
		`sum by (level) (count_over_time({service="api"}[10s]))`), 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("sum by: %v", err)
	}
	got := map[string]float64{}
	for _, s := range series {
		got[renderLabels(s.Labels)] += s.Points[0].Value
	}
	want := map[string]float64{`{level="error"}`: 1, `{level="info"}`: 1, `{}`: 2}
	assertValues(t, got, want)

	// Bare sum: one label-less series holding everything.
	series, err = e.EvalMetricRange(context.Background(), mustParse(t,
		`sum(count_over_time({service="api"}[10s]))`), 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if len(series) != 1 || len(series[0].Labels) != 0 || series[0].Points[0].Value != 4 {
		t.Fatalf("sum = %+v, want one label-less series with value 4", series)
	}

	// sum without (level): everything keeps service, one group loses its level,
	// and the region stream stays separate.
	series, err = e.EvalMetricRange(context.Background(), mustParse(t,
		`sum without (level) (count_over_time({service="api"}[10s]))`), 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("sum without: %v", err)
	}
	got = map[string]float64{}
	for _, s := range series {
		got[renderLabels(s.Labels)] += s.Points[0].Value
	}
	want = map[string]float64{`{service="api"}`: 3, `{region="eu",service="api"}`: 1}
	assertValues(t, got, want)

	// No aggregation: one series per stream, labels verbatim (the empty level is
	// kept here, since nothing is being grouped).
	series, err = e.EvalMetricRange(context.Background(), mustParse(t,
		`count_over_time({service="api"}[10s])`), 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("bare: %v", err)
	}
	if len(series) != 4 {
		t.Fatalf("bare aggregation = %d series, want 4", len(series))
	}
}

// TestEvalMetricRange_DropLabel_Grouping proves a dropped label is invisible to
// grouping: `by (level)` on a query that dropped `level` sees it as absent, the
// same as a stream that never had it — not the value the stream still carries.
func TestEvalMetricRange_DropLabel_Grouping(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info": {entry(10*sec, "a")},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `sum by (level) (count_over_time({service="api"} | drop level [10s]))`)

	series, err := e.EvalMetricRange(context.Background(), q, 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("EvalMetricRange: %v", err)
	}
	if len(series) != 1 || len(series[0].Labels) != 0 {
		t.Fatalf("series = %+v, want exactly one label-less series", series)
	}
	if series[0].Points[0].Value != 1 {
		t.Errorf("value = %v, want 1", series[0].Points[0].Value)
	}
}

// TestEvalMetricRange_DropLabel_Bare proves a bare range aggregation's output
// labels lose the dropped names too: "verbatim" means "after drop", not before.
func TestEvalMetricRange_DropLabel_Bare(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info": {entry(10*sec, "a")},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"} | drop level [10s])`)

	series, err := e.EvalMetricRange(context.Background(), q, 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("EvalMetricRange: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("got %d series, want 1: %+v", len(series), series)
	}
	if _, ok := series[0].Labels["level"]; ok {
		t.Errorf("labels = %v, want no level", series[0].Labels)
	}
	if series[0].Labels["service"] != "api" {
		t.Errorf("labels = %v, want service=api kept", series[0].Labels)
	}
}

// TestEvalMetricRange_DropErrorLabel_NoOp proves dropping __error__ — the only
// form Grafana emits — changes nothing: no parser stage in this subset ever sets
// __error__, so there is nothing for the drop to remove.
func TestEvalMetricRange_DropErrorLabel_NoOp(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info": {entry(10*sec, "a")},
	})
	e := NewQueryEngine(f)
	withDrop := mustParse(t, `count_over_time({service="api"} | drop __error__ [10s])`)
	without := mustParse(t, `count_over_time({service="api"}[10s])`)

	a, err := e.EvalMetricRange(context.Background(), withDrop, 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("with drop: %v", err)
	}
	b, err := e.EvalMetricRange(context.Background(), without, 10*sec, 10*sec, sec)
	if err != nil {
		t.Fatalf("without: %v", err)
	}
	if len(a) != 1 || len(b) != 1 || renderLabels(a[0].Labels) != renderLabels(b[0].Labels) {
		t.Fatalf("dropping __error__ changed labels: with=%+v without=%+v", a, b)
	}
}

// TestEvalMetricRange_Deterministic proves repeated evaluation returns series and
// points in the same order, so responses do not flap with Go's map iteration.
func TestEvalMetricRange_Deterministic(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info":  {entry(10*sec, "a"), entry(20*sec, "b")},
		"service=api,level=error": {entry(10*sec, "c")},
		"service=api,level=warn":  {entry(20*sec, "d")},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `sum by (level) (count_over_time({service="api"}[10s]))`)

	var first []string
	for run := 0; run < 5; run++ {
		series, err := e.EvalMetricRange(context.Background(), q, 10*sec, 30*sec, 10*sec)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		var order []string
		for _, s := range series {
			order = append(order, renderLabels(s.Labels))
			for i := 1; i < len(s.Points); i++ {
				if s.Points[i-1].TimestampNs >= s.Points[i].TimestampNs {
					t.Fatalf("points not ascending: %+v", s.Points)
				}
			}
		}
		if run == 0 {
			first = order
			continue
		}
		if len(order) != len(first) {
			t.Fatalf("run %d order = %v, want %v", run, order, first)
		}
		for i := range order {
			if order[i] != first[i] {
				t.Fatalf("run %d order = %v, want %v", run, order, first)
			}
		}
	}
}

// TestEvalMetricRange_Int64Extremes proves the window lower bound clamps instead
// of wrapping and the tick advance does not overflow.
func TestEvalMetricRange_Int64Extremes(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{"service=api": {entry(math.MinInt64+1, "x")}})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"}[1h])`)

	if _, err := e.EvalMetricRange(context.Background(), q, math.MinInt64+1, math.MinInt64+2, 1); err != nil {
		t.Fatalf("near MinInt64: %v", err)
	}
	if _, err := e.EvalMetricRange(context.Background(), q, math.MaxInt64-1, math.MaxInt64, 1); err != nil {
		t.Fatalf("near MaxInt64: %v", err)
	}
	// A step wider than the whole span must terminate after the first tick.
	if _, err := e.EvalMetricRange(context.Background(), q, math.MinInt64+1, math.MaxInt64, math.MaxInt64); err != nil {
		t.Fatalf("full span: %v", err)
	}
}

func TestEvalMetricRange_InvalidArguments(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{"service=api": {entry(10*sec, "a")}})
	e := NewQueryEngine(f)
	q := mustParse(t, `count_over_time({service="api"}[10s])`)

	if _, err := e.EvalMetricRange(context.Background(), q, 0, 10*sec, 0); err == nil {
		t.Error("step 0 = nil error, want error")
	}
	if _, err := e.EvalMetricRange(context.Background(), q, 10*sec, 0, sec); err == nil {
		t.Error("end < start = nil error, want error")
	}
	bad := q
	bad.RangeNs = 0
	if _, err := e.EvalMetricRange(context.Background(), bad, 0, 10*sec, sec); err == nil {
		t.Error("range 0 = nil error, want error")
	}
}

// TestEvalMetricInstant_InclusiveEnd pins that an entry at exactly `time` counts.
// This is not a special case: every tick's window is (t-range, t], so the single
// tick of an instant query closes on its own timestamp like any other. It matches
// upstream, and it matches QueryInstant's treatment of the same boundary in 4.4.
func TestEvalMetricInstant_InclusiveEnd(t *testing.T) {
	f := newMetricFake(t, map[string][]LogEntry{
		"service=api,level=info": {entry(90*sec, "before"), entry(100*sec, "at time")},
	})
	e := NewQueryEngine(f)
	q := mustParse(t, `sum by (level) (count_over_time({service="api"}[60s]))`)

	samples, err := e.EvalMetricInstant(context.Background(), q, 100*sec)
	if err != nil {
		t.Fatalf("EvalMetricInstant: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1: %+v", len(samples), samples)
	}
	if samples[0].Value != 2 {
		t.Errorf("value = %v, want 2 (the entry at exactly `time` is included)", samples[0].Value)
	}
	if samples[0].TimestampNs != 100*sec {
		t.Errorf("timestamp = %d, want %d", samples[0].TimestampNs, 100*sec)
	}
	if samples[0].Labels["level"] != "info" {
		t.Errorf("labels = %v, want level=info", samples[0].Labels)
	}
}

func assertPoints(t *testing.T, got, want []MetricPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d points %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i].TimestampNs != want[i].TimestampNs || math.Abs(got[i].Value-want[i].Value) > 1e-9 {
			t.Fatalf("point %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func assertValues(t *testing.T, got, want map[string]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	for k, v := range want {
		if math.Abs(got[k]-v) > 1e-9 {
			t.Fatalf("group %s = %v, want %v (all groups: %v)", k, got[k], v, got)
		}
	}
}
