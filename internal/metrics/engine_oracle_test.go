package metrics

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// oracleEngine is the pre-bulk query engine, preserved verbatim as a test
// oracle: per-series SelectSeries, then QueryInstant at every tick, QueryRange
// for every rate window. It is the definition the bulk engine must agree with.
type oracleEngine struct{ store queryStore }

func (e oracleEngine) instantQuery(sel Selector, tMs int64) ([]InstantSample, error) {
	matched, err := e.store.SelectSeries(sel)
	if err != nil {
		return nil, err
	}
	result := make([]InstantSample, 0, len(matched))
	for _, ms := range matched {
		sample, ok, err := e.store.QueryInstant(SeriesID(ms.Labels.Hash()), tMs)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		result = append(result, InstantSample{Labels: ms.Labels, TimestampMs: sample.TimestampMs, Value: sample.Value})
	}
	return result, nil
}

func (e oracleEngine) rangeQuery(sel Selector, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}
	matched, err := e.store.SelectSeries(sel)
	if err != nil {
		return nil, err
	}
	result := make([]RangeSeries, 0, len(matched))
	for _, ms := range matched {
		var points []SamplePoint
		id := SeriesID(ms.Labels.Hash())
		for t := startMs; t <= endMs; t += stepMs {
			sample, ok, err := e.store.QueryInstant(id, t)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			points = append(points, SamplePoint{TimestampMs: t, Value: sample.Value})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: ms.Labels, Points: points})
	}
	return result, nil
}

func (e oracleEngine) rateInstant(x RateExpr, tMs int64) ([]InstantSample, error) {
	matched, err := e.store.SelectSeries(x.Selector)
	if err != nil {
		return nil, err
	}
	result := make([]InstantSample, 0, len(matched))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, ms := range matched {
		samples, err := e.store.QueryRange(SeriesID(ms.Labels.Hash()), tMs-x.WindowMs, tMs)
		if err != nil {
			return nil, err
		}
		if len(samples) < 2 {
			continue
		}
		first, last := samples[0], samples[len(samples)-1]
		result = append(result, InstantSample{Labels: ms.Labels, TimestampMs: tMs, Value: (last.Value - first.Value) / windowSec})
	}
	return result, nil
}

func (e oracleEngine) rateRange(x RateExpr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	matched, err := e.store.SelectSeries(x.Selector)
	if err != nil {
		return nil, err
	}
	result := make([]RangeSeries, 0, len(matched))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, ms := range matched {
		id := SeriesID(ms.Labels.Hash())
		var points []SamplePoint
		for t := startMs; t <= endMs; t += stepMs {
			samples, err := e.store.QueryRange(id, t-x.WindowMs, t)
			if err != nil {
				return nil, err
			}
			if len(samples) < 2 {
				continue
			}
			first, last := samples[0], samples[len(samples)-1]
			points = append(points, SamplePoint{TimestampMs: t, Value: (last.Value - first.Value) / windowSec})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: ms.Labels, Points: points})
	}
	return result, nil
}

func (e oracleEngine) evalInstant(expr Expr, tMs int64) ([]InstantSample, error) {
	switch x := expr.(type) {
	case SelectorExpr:
		return e.instantQuery(x.Selector, tMs)
	case RateExpr:
		return e.rateInstant(x, tMs)
	case SumExpr:
		inner, err := e.evalInstant(x.Inner, tMs)
		if err != nil {
			return nil, err
		}
		return aggregateInstant(inner, x.By, tMs), nil
	case ScalarExpr:
		return []InstantSample{{Labels: newOutputLabels(nil), TimestampMs: tMs, Value: x.Value}}, nil
	default:
		return nil, fmt.Errorf("unknown expression type %T", expr)
	}
}

func (e oracleEngine) evalRange(expr Expr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}
	switch x := expr.(type) {
	case SelectorExpr:
		return e.rangeQuery(x.Selector, startMs, endMs, stepMs)
	case RateExpr:
		return e.rateRange(x, startMs, endMs, stepMs)
	case SumExpr:
		inner, err := e.evalRange(x.Inner, startMs, endMs, stepMs)
		if err != nil {
			return nil, err
		}
		return aggregateRange(inner, x.By), nil
	case ScalarExpr:
		points := scalarPoints(x.Value, startMs, endMs, stepMs)
		sortPoints(points)
		return []RangeSeries{{Labels: newOutputLabels(nil), Points: points}}, nil
	default:
		return nil, fmt.Errorf("unknown expression type %T", expr)
	}
}

// matchingSeries is the pre-bulk metadata filter.
func (e oracleEngine) matchingSeries(f MetadataFilter) ([]MatchedSeries, error) {
	sels := f.Selectors
	if len(sels) == 0 {
		sels = []Selector{{}}
	}
	seen := make(map[SeriesID]struct{})
	var out []MatchedSeries
	for _, sel := range sels {
		matched, err := e.store.SelectSeries(sel)
		if err != nil {
			return nil, err
		}
		for _, ms := range matched {
			id := SeriesID(ms.Labels.Hash())
			if _, ok := seen[id]; ok {
				continue
			}
			if f.HasTime {
				samples, err := e.store.QueryRange(id, f.StartMs, f.EndMs)
				if err != nil {
					return nil, err
				}
				if len(samples) == 0 {
					continue
				}
			}
			seen[id] = struct{}{}
			out = append(out, ms)
		}
	}
	return out, nil
}

// oracleFixture builds a BlockStore holding random samples for seven series:
// out-of-order timestamps, overwrites, NaN and ±Inf, and a flush part-way so
// the second round overwrites data that now lives in a block. Values are
// integers or special floats, so sums are exact in any order.
func oracleFixture(t *testing.T, seed uint64) *BlockStore {
	t.Helper()
	bs, err := NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	var series []Labels
	for _, job := range []string{"a", "b"} {
		for inst := 1; inst <= 3; inst++ {
			series = append(series, mustLabelsInternal(t, map[string]string{
				"__name__": "oracle_metric", "job": job, "instance": strconv.Itoa(inst),
			}))
		}
	}
	series = append(series, mustLabelsInternal(t, map[string]string{"__name__": "oracle_other", "job": "a"}))

	value := func() float64 {
		switch r.IntN(50) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1)
		case 2:
			return math.Inf(-1)
		default:
			return float64(r.IntN(1000))
		}
	}
	round := func(n int) {
		for range n {
			l := series[r.IntN(len(series))]
			ts := int64(r.IntN(4*3600)) * 1000
			if err := bs.Append(l, ts, value()); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	}
	round(2000)
	if wrote, err := bs.FlushBlock(); err != nil || !wrote {
		t.Fatalf("FlushBlock = %v, %v; the fixture needs a persisted block", wrote, err)
	}
	round(1000)
	return bs
}

func labelKey(l Labels) string {
	m := l.Map()
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "%s=%q,", n, m[n])
	}
	return b.String()
}

func sameFloat(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

func canonicalInstant(in []InstantSample) []InstantSample {
	out := append([]InstantSample(nil), in...)
	sort.Slice(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return out
}

func canonicalRange(in []RangeSeries) []RangeSeries {
	out := append([]RangeSeries(nil), in...)
	sort.Slice(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return out
}

func assertInstantEqual(t *testing.T, what string, got, want []InstantSample) {
	t.Helper()
	got, want = canonicalInstant(got), canonicalInstant(want)
	if len(got) != len(want) {
		t.Fatalf("%s: %d series, oracle %d", what, len(got), len(want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if labelKey(g.Labels) != labelKey(w.Labels) || g.TimestampMs != w.TimestampMs || !sameFloat(g.Value, w.Value) {
			t.Fatalf("%s: series %d = %s@%d=%v, oracle %s@%d=%v",
				what, i, labelKey(g.Labels), g.TimestampMs, g.Value, labelKey(w.Labels), w.TimestampMs, w.Value)
		}
	}
}

func assertRangeEqual(t *testing.T, what string, got, want []RangeSeries) {
	t.Helper()
	got, want = canonicalRange(got), canonicalRange(want)
	if len(got) != len(want) {
		t.Fatalf("%s: %d series, oracle %d", what, len(got), len(want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if labelKey(g.Labels) != labelKey(w.Labels) || len(g.Points) != len(w.Points) {
			t.Fatalf("%s: series %d = %s with %d points, oracle %s with %d",
				what, i, labelKey(g.Labels), len(g.Points), labelKey(w.Labels), len(w.Points))
		}
		for j := range g.Points {
			if g.Points[j].TimestampMs != w.Points[j].TimestampMs || !sameFloat(g.Points[j].Value, w.Points[j].Value) {
				t.Fatalf("%s: %s point %d = %+v, oracle %+v", what, labelKey(g.Labels), j, g.Points[j], w.Points[j])
			}
		}
	}
}

func TestEngineAgreesWithThePerTickOracle(t *testing.T) {
	selAll := Selector{MetricName: "oracle_metric"}
	selA := Selector{MetricName: "oracle_metric", Matchers: []Matcher{{Name: "job", Value: "a"}}}
	exprs := []Expr{
		SelectorExpr{Selector: selAll},
		SelectorExpr{Selector: selA},
		SelectorExpr{Selector: Selector{Matchers: []Matcher{{Name: "job", Value: "a"}}}},
		RateExpr{Selector: selAll, WindowMs: 60_000},
		RateExpr{Selector: selA, WindowMs: 900_000},
		SumExpr{Inner: SelectorExpr{Selector: selAll}},
		SumExpr{Inner: RateExpr{Selector: selAll, WindowMs: 300_000}, By: []string{"job"}},
	}
	// Sizes are bounded on purpose: the oracle re-reads every chunk at every
	// tick, so a step of at least a minute and a range of at most two hours keep
	// each range query to ~120 ticks and the whole test to a couple of seconds.
	for seed := uint64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			bs := oracleFixture(t, seed)
			eng := NewQueryEngine(bs)
			oracle := oracleEngine{store: bs}
			r := rand.New(rand.NewPCG(seed, 7))
			for i, expr := range exprs {
				for q := range 10 {
					tMs := int64(r.IntN(5*3600)-1800) * 1000
					got, err := eng.EvalInstant(expr, tMs)
					if err != nil {
						t.Fatalf("EvalInstant: %v", err)
					}
					want, err := oracle.evalInstant(expr, tMs)
					if err != nil {
						t.Fatalf("oracle evalInstant: %v", err)
					}
					assertInstantEqual(t, fmt.Sprintf("expr %d instant %d at %d", i, q, tMs), got, want)

					start := int64(r.IntN(5*3600)-1800) * 1000
					end := start + int64(r.IntN(2*3600))*1000
					step := int64(60+r.IntN(600)) * 1000
					gotR, err := eng.EvalRange(expr, start, end, step)
					if err != nil {
						t.Fatalf("EvalRange: %v", err)
					}
					wantR, err := oracle.evalRange(expr, start, end, step)
					if err != nil {
						t.Fatalf("oracle evalRange: %v", err)
					}
					assertRangeEqual(t, fmt.Sprintf("expr %d range %d [%d,%d]/%d", i, q, start, end, step), gotR, wantR)
				}
			}

			for q := range 25 {
				f := MetadataFilter{}
				if r.IntN(2) == 0 {
					f.Selectors = []Selector{selA}
				}
				if r.IntN(2) == 0 {
					f.StartMs = int64(r.IntN(4*3600)) * 1000
					f.EndMs = f.StartMs + int64(r.IntN(1800))*1000
					f.HasTime = true
				}
				got, err := eng.Series(f)
				if err != nil {
					t.Fatalf("Series: %v", err)
				}
				want, err := oracle.matchingSeries(f)
				if err != nil {
					t.Fatalf("oracle matchingSeries: %v", err)
				}
				gotKeys := make([]string, len(got))
				for i, l := range got {
					gotKeys[i] = labelKey(l)
				}
				wantKeys := make([]string, len(want))
				for i, ms := range want {
					wantKeys[i] = labelKey(ms.Labels)
				}
				sort.Strings(gotKeys)
				sort.Strings(wantKeys)
				if strings.Join(gotKeys, "|") != strings.Join(wantKeys, "|") {
					t.Fatalf("filter %d %+v: Series = %v, oracle %v", q, f, gotKeys, wantKeys)
				}
			}
		})
	}
}
