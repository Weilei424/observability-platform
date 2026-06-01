package metrics

import (
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
	default:
		return nil, fmt.Errorf("unknown expression type %T", expr)
	}
}

func (e *QueryEngine) rateInstant(x RateExpr, tMs int64) ([]InstantSample, error) {
	matched := e.store.SelectSeries(x.Selector)
	result := make([]InstantSample, 0, len(matched))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, ms := range matched {
		samples := e.store.QueryRange(ms.Labels.Fingerprint(), tMs-x.WindowMs, tMs)
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
	matched := e.store.SelectSeries(x.Selector)
	result := make([]RangeSeries, 0, len(matched))
	windowSec := float64(x.WindowMs) / 1000.0
	for _, ms := range matched {
		id := ms.Labels.Fingerprint()
		var points []SamplePoint
		for t := startMs; t <= endMs; t += stepMs {
			samples := e.store.QueryRange(id, t-x.WindowMs, t)
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
				if val, ok2 := s.Labels.Get(name); ok2 {
					lm[name] = val
				}
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
				if val, ok2 := rs.Labels.Get(name); ok2 {
					lm[name] = val
				}
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

// groupKey returns a null-byte-separated key for the values of labels named by `by`.
// Absent labels contribute an empty string. Null bytes cannot appear in label values
// (UTF-8 validation on ingestion), so there are no collisions between groups.
func groupKey(labels Labels, by []string) string {
	if len(by) == 0 {
		return ""
	}
	parts := make([]string, len(by))
	for i, name := range by {
		parts[i], _ = labels.Get(name)
	}
	return strings.Join(parts, "\x00")
}

// sortPoints sorts SamplePoints by TimestampMs ascending.
func sortPoints(pts []SamplePoint) {
	sort.Slice(pts, func(i, j int) bool { return pts[i].TimestampMs < pts[j].TimestampMs })
}
