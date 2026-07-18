package metrics

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// EvalInstant evaluates expr at time tMs and returns one InstantSample per output series.
func (e *QueryEngine) EvalInstant(expr Expr, tMs int64) ([]InstantSample, error) {
	switch x := expr.(type) {
	case SelectorExpr:
		return e.InstantQuery(x.Selector, tMs)
	case RateExpr:
		return e.rateInstant(x, tMs)
	case SumExpr:
		inner, err := e.EvalInstant(x.Inner, tMs)
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
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end time must be >= start time")
	}
	switch x := expr.(type) {
	case SelectorExpr:
		return e.RangeQuery(x.Selector, startMs, endMs, stepMs)
	case RateExpr:
		return e.rateRange(x, startMs, endMs, stepMs)
	case SumExpr:
		inner, err := e.EvalRange(x.Inner, startMs, endMs, stepMs)
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

func (e *QueryEngine) rateInstant(x RateExpr, tMs int64) ([]InstantSample, error) {
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
		rate := (last.Value - first.Value) / windowSec
		result = append(result, InstantSample{
			Labels:      ms.Labels,
			TimestampMs: tMs,
			Value:       rate,
		})
	}
	return result, nil
}

func (e *QueryEngine) rateRange(x RateExpr, startMs, endMs, stepMs int64) ([]RangeSeries, error) {
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
			rate := (last.Value - first.Value) / windowSec
			points = append(points, SamplePoint{TimestampMs: t, Value: rate})
		}
		if len(points) == 0 {
			continue
		}
		result = append(result, RangeSeries{Labels: ms.Labels, Points: points})
	}
	return result, nil
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
