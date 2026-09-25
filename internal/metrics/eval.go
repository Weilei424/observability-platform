package metrics

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
)

// EvalInstant evaluates expr at time tMs and returns one InstantSample per output series.
func (e *QueryEngine) EvalInstant(expr Expr, tMs int64) ([]InstantSample, error) {
	return e.EvalInstantContext(context.Background(), expr, tMs)
}

// EvalInstantContext is EvalInstant bound to ctx.
func (e *QueryEngine) EvalInstantContext(ctx context.Context, expr Expr, tMs int64) ([]InstantSample, error) {
	switch x := expr.(type) {
	case SelectorExpr:
		return e.InstantQueryContext(ctx, x.Selector, tMs)
	case RateExpr:
		return e.rateInstant(ctx, x, tMs)
	case SumExpr:
		inner, err := e.EvalInstantContext(ctx, x.Inner, tMs)
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

// EvalRange evaluates expr over [startMs, endMs] at stepMs-aligned ticks.
func (e *QueryEngine) EvalRange(expr Expr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	return e.EvalRangeContext(context.Background(), expr, startMs, endMs, stepMs)
}

// EvalRangeContext is EvalRange bound to ctx.
func (e *QueryEngine) EvalRangeContext(ctx context.Context, expr Expr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}
	switch x := expr.(type) {
	case SelectorExpr:
		return e.RangeQueryContext(ctx, x.Selector, startMs, endMs, stepMs)
	case RateExpr:
		return e.rateRange(ctx, x, startMs, endMs, stepMs)
	case SumExpr:
		inner, err := e.EvalRangeContext(ctx, x.Inner, startMs, endMs, stepMs)
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

// scalarPoints generates step-aligned points for a constant scalar value.
func scalarPoints(v float64, startMs, endMs, stepMs int64) []SamplePoint {
	var points []SamplePoint
	for t := startMs; t <= endMs; t += stepMs {
		points = append(points, SamplePoint{TimestampMs: t, Value: v})
	}
	return points
}

func (e *QueryEngine) rateInstant(ctx context.Context, x RateExpr, tMs int64) ([]InstantSample, error) {
	series, err := e.src.Select(ctx, SelectParams{Selector: x.Selector, MinT: tMs - x.WindowMs, MaxT: tMs})
	if err != nil {
		return nil, err
	}
	result := make([]InstantSample, 0, len(series))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, sd := range series {
		if len(sd.Samples) < 2 {
			continue
		}
		first, last := sd.Samples[0], sd.Samples[len(sd.Samples)-1]
		result = append(result, InstantSample{
			Labels:      sd.Labels,
			TimestampMs: tMs,
			Value:       (last.Value - first.Value) / windowSec,
		})
	}
	return result, nil
}

// rateRange reads each series once over [startMs-WindowMs, endMs] and slides a
// [t-WindowMs, t] window across the ticks with two pointers, where the
// pre-bulk engine re-read storage for every tick.
func (e *QueryEngine) rateRange(ctx context.Context, x RateExpr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
	series, err := e.src.Select(ctx, SelectParams{Selector: x.Selector, MinT: saturatingSub(startMs, x.WindowMs), MaxT: endMs})
	if err != nil {
		return nil, err
	}
	result := make([]RangeSeries, 0, len(series))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, sd := range series {
		var points []SamplePoint
		lo, hi := 0, 0 // the tick's window is Samples[lo:hi]
		for t := startMs; t <= endMs; t += stepMs {
			for hi < len(sd.Samples) && sd.Samples[hi].TimestampMs <= t {
				hi++
			}
			for lo < hi && sd.Samples[lo].TimestampMs < t-x.WindowMs {
				lo++
			}
			if hi-lo < 2 {
				continue
			}
			first, last := sd.Samples[lo], sd.Samples[hi-1]
			points = append(points, SamplePoint{TimestampMs: t, Value: (last.Value - first.Value) / windowSec})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: sd.Labels, Points: points})
	}
	return result, nil
}

// saturatingSub returns a-b clamped to the int64 range, for a window start that
// would otherwise wrap below math.MinInt64.
func saturatingSub(a, b int64) int64 {
	if b > 0 && a < math.MinInt64+b {
		return math.MinInt64
	}
	return a - b
}

func aggregateInstant(samples []InstantSample, by []string, tMs int64) []InstantSample {
	if len(samples) == 0 {
		return []InstantSample{}
	}
	if len(by) == 0 {
		var total float64
		for _, s := range samples {
			total += s.Value
		}
		return []InstantSample{{Labels: newOutputLabels(nil), TimestampMs: tMs, Value: total}}
	}

	groupValues := make(map[string]float64)
	groupLabels := make(map[string]map[string]string)
	for _, s := range samples {
		key := groupKey(s.Labels, by)
		if _, ok := groupLabels[key]; !ok {
			lm := make(map[string]string, len(by))
			for _, name := range by {
				lm[name], _ = s.Labels.Get(name) // absent labels → ""
			}
			groupLabels[key] = lm
		}
		groupValues[key] += s.Value
	}

	result := make([]InstantSample, 0, len(groupValues))
	for key, val := range groupValues {
		result = append(result, InstantSample{
			Labels:      newOutputLabels(groupLabels[key]),
			TimestampMs: tMs,
			Value:       val,
		})
	}
	return result
}

func aggregateRange(series []RangeSeries, by []string) []RangeSeries {
	if len(series) == 0 {
		return []RangeSeries{}
	}

	groupTicks := make(map[string]map[int64]float64)
	groupLabels := make(map[string]map[string]string)

	for _, rs := range series {
		key := groupKey(rs.Labels, by)
		if _, ok := groupTicks[key]; !ok {
			groupTicks[key] = make(map[int64]float64)
			lm := make(map[string]string, len(by))
			for _, name := range by {
				lm[name], _ = rs.Labels.Get(name) // absent labels → ""
			}
			groupLabels[key] = lm
		}
		for _, pt := range rs.Points {
			groupTicks[key][pt.TimestampMs] += pt.Value
		}
	}

	result := make([]RangeSeries, 0, len(groupTicks))
	for key, ticks := range groupTicks {
		points := make([]SamplePoint, 0, len(ticks))
		for t, v := range ticks {
			points = append(points, SamplePoint{TimestampMs: t, Value: v})
		}
		sortPoints(points)
		result = append(result, RangeSeries{
			Labels: newOutputLabels(groupLabels[key]),
			Points: points,
		})
	}
	return result
}

// groupKey returns an unambiguous key for the values of labels named by `by`.
// Each value is encoded as a 4-byte big-endian length followed by the value bytes.
// Absent labels contribute an empty string (length 0, zero bytes).
// Length-prefix encoding ensures distinct tuples always produce distinct keys,
// regardless of what bytes appear in label values.
func groupKey(labels Labels, by []string) string {
	if len(by) == 0 {
		return ""
	}
	var b strings.Builder
	var buf [4]byte
	for _, name := range by {
		val, _ := labels.Get(name)
		binary.BigEndian.PutUint32(buf[:], uint32(len(val)))
		b.Write(buf[:])
		b.WriteString(val)
	}
	return b.String()
}

// sortPoints sorts SamplePoints by TimestampMs ascending.
func sortPoints(pts []SamplePoint) {
	sort.Slice(pts, func(i, j int) bool { return pts[i].TimestampMs < pts[j].TimestampMs })
}
