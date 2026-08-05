package logs

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MetricPoint is one evaluated step of a metric query.
type MetricPoint struct {
	TimestampNs int64
	Value       float64
}

// MetricSeries is one output series of a range metric query. Points are ascending
// by timestamp and never empty — a series with no data is omitted entirely.
type MetricSeries struct {
	Labels map[string]string
	Points []MetricPoint
}

// MetricSample is one output series of an instant metric query.
type MetricSample struct {
	Labels      map[string]string
	TimestampNs int64
	Value       float64
}

// EvalMetricRange evaluates q at every step tick in [startNs, endNs].
//
// Ticks run start, start+step, ... up to and including endNs when aligned. Each
// tick's window is (t-range, t], and entries are read over [start-range, end) —
// half-open on the right, as upstream's sample reads are, so an entry at exactly
// endNs is never counted. A window with no entries produces no point, matching
// Prometheus and Loki, which emit a gap rather than a zero.
//
// The caller bounds the tick count: the HTTP layer enforces Loki's 11,000-points
// -per-timeseries limit before calling. Nothing here allocates in proportion to
// the tick count except the points actually emitted.
func (e *QueryEngine) EvalMetricRange(ctx context.Context, q MetricQuery, startNs, endNs, stepNs int64) ([]MetricSeries, error) {
	return e.evalMetric(ctx, q, startNs, endNs, stepNs, false)
}

// EvalMetricInstant evaluates q at a single instant, returning one sample per
// output series. start == end yields exactly one tick regardless of step.
//
// The end is inclusive here, so an entry at exactly tsNs is counted. That mirrors
// QueryInstant: `time` is an evaluation instant, not a range bound. It is a
// superset of upstream, whose reads are half-open everywhere — but leaving it
// exclusive would mean a log query at T returns an entry that a metric query at T
// does not count.
func (e *QueryEngine) EvalMetricInstant(ctx context.Context, q MetricQuery, tsNs int64) ([]MetricSample, error) {
	series, err := e.evalMetric(ctx, q, tsNs, tsNs, 1, true)
	if err != nil {
		return nil, err
	}
	out := make([]MetricSample, 0, len(series))
	for _, s := range series {
		for _, p := range s.Points {
			out = append(out, MetricSample{Labels: s.Labels, TimestampNs: p.TimestampNs, Value: p.Value})
		}
	}
	return out, nil
}

// metricGroup accumulates one output series. values is keyed by tick timestamp,
// so a group fed by several streams sums them tick by tick.
type metricGroup struct {
	labels map[string]string
	values map[int64]float64
}

func (e *QueryEngine) evalMetric(ctx context.Context, q MetricQuery, startNs, endNs, stepNs int64, endInclusive bool) ([]MetricSeries, error) {
	if stepNs <= 0 {
		return nil, fmt.Errorf("step must be greater than 0")
	}
	if endNs < startNs {
		return nil, fmt.Errorf("end must be greater than or equal to start")
	}
	if q.RangeNs <= 0 {
		return nil, fmt.Errorf("range must be greater than 0")
	}

	readStart := windowStart(startNs, q.RangeNs)
	groups := make(map[string]*metricGroup)

	for _, id := range e.r.MatchingStreamIDs(q.Selector.Matchers) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		labels, ok := e.r.StreamLabelSet(id)
		if !ok {
			continue
		}
		entries, err := e.r.StreamEntries(ctx, id, readStart, endNs)
		if err != nil {
			return nil, err
		}
		// Filter in place rather than allocating a second slice: StreamEntries
		// returns a fresh slice on every call (both Store's disk-backed
		// implementation and the in-memory one build `out` from nil), never one
		// shared with a cache or another caller, so overwriting its head as we
		// go is safe. That freshness is the assumption a future StreamEntries
		// change (e.g. a cached or pooled buffer) could break.
		kept := entries[:0]
		for _, en := range entries {
			if !endInclusive && en.TimestampNs >= endNs {
				continue // the read is half-open on the right
			}
			if !passesFilters(en.Line, q.Selector.LineFilters) {
				continue
			}
			kept = append(kept, en)
		}
		if len(kept) == 0 {
			continue
		}
		key, outLabels := groupOf(labels, q)
		g := groups[key]
		if g == nil {
			g = &metricGroup{labels: outLabels, values: make(map[int64]float64)}
			groups[key] = g
		}
		accumulate(g, kept, q, startNs, endNs, stepNs)
	}

	series := make([]MetricSeries, 0, len(groups))
	for _, g := range groups {
		if len(g.values) == 0 {
			continue
		}
		points := make([]MetricPoint, 0, len(g.values))
		for ts, v := range g.values {
			points = append(points, MetricPoint{TimestampNs: ts, Value: v})
		}
		sort.Slice(points, func(i, j int) bool { return points[i].TimestampNs < points[j].TimestampNs })
		series = append(series, MetricSeries{Labels: g.labels, Points: points})
	}

	// Sort by the rendered label set, per §5 of the design spec ("series are
	// sorted by their rendered label set") — not the encoded group key, which
	// exists only to disambiguate groups internally and carries no ordering
	// promise of its own. renderLabels is deterministic (sorted names, quoted
	// values), so the response order is stable across identical requests even
	// though the loop above walks `groups` in Go's randomized map order.
	sort.Slice(series, func(i, j int) bool {
		return renderLabels(series[i].Labels) < renderLabels(series[j].Labels)
	})
	return series, nil
}

// accumulate adds one stream's per-tick values to its group with a two-pointer
// sliding window. entries must be sorted ascending by timestamp, which
// StreamEntries guarantees, and ticks are monotonic — so both window edges move
// in one direction and the whole range costs O(entries + ticks).
func accumulate(g *metricGroup, entries []LogEntry, q MetricQuery, startNs, endNs, stepNs int64) {
	scale := 1.0
	if q.Op == OpRate || q.Op == OpBytesRate {
		scale = float64(time.Second) / float64(q.RangeNs)
	}
	head, tail := 0, 0 // [tail, head) is the current window
	var acc float64
	for t := startNs; ; {
		for head < len(entries) && entries[head].TimestampNs <= t {
			acc += entryWeight(entries[head], q.Op)
			head++
		}
		lo := windowStart(t, q.RangeNs)
		for tail < head && entries[tail].TimestampNs <= lo {
			acc -= entryWeight(entries[tail], q.Op)
			tail++
		}
		// The condition is "window non-empty", not "value non-zero":
		// bytes_over_time over empty lines is a real zero and must emit a point,
		// while an empty window must leave a gap.
		if head > tail {
			g.values[t] += scale * acc
		}
		// Advance without overflowing. The loop invariant t <= endNs makes the
		// unsigned difference exact, which a signed endNs-t would not be when the
		// span itself exceeds int64.
		if uint64(stepNs) > uint64(endNs)-uint64(t) {
			return
		}
		t += stepNs
	}
}

func entryWeight(e LogEntry, op RangeOp) float64 {
	if op == OpBytesOverTime || op == OpBytesRate {
		return float64(len(e.Line)) // bytes, as Loki counts them — not runes
	}
	return 1
}

// windowStart returns t - rangeNs, clamped at MinInt64 rather than wrapping. A
// wrapped bound is worse than a clamped one: it lands somewhere plausible in the
// far future instead of at the beginning of time.
func windowStart(t, rangeNs int64) int64 {
	if t < math.MinInt64+rangeNs {
		return math.MinInt64
	}
	return t - rangeNs
}

// groupOf returns a stream's group key and its output label set.
//
// Selector.DropLabels is removed from the stream's label map first, before any
// of the rules below run — so a `by (level)` grouping on a query that dropped
// `level` sees it as absent, exactly as if the stream never carried it, and a
// bare range aggregation's "verbatim" label set is verbatim-after-drop.
//
// An empty label value is treated as absent, because both render to the same
// output label set — splitting them would put two series with identical labels in
// one response. Grouping is therefore a function of the output labels: one group,
// one label set. A bare range aggregation keeps the stream's (post-drop) labels
// and cannot collide either, since stream label sets are unique by fingerprint
// and dropping the same names from two already-distinct sets is the one
// documented exception (see the log-path known limitation in task-1-report.md).
func groupOf(l StreamLabels, q MetricQuery) (string, map[string]string) {
	m := l.Map()
	for _, n := range q.Selector.DropLabels {
		delete(m, n)
	}
	if q.Agg == AggNone {
		return encodeLabelSet(m), m
	}
	out := make(map[string]string)
	switch {
	case len(q.Grouping.Labels) == 0: // bare sum()
	case q.Grouping.Without:
		drop := make(map[string]bool, len(q.Grouping.Labels))
		for _, n := range q.Grouping.Labels {
			drop[n] = true
		}
		for n, v := range m {
			if !drop[n] && v != "" {
				out[n] = v
			}
		}
	default:
		for _, n := range q.Grouping.Labels {
			if v, ok := m[n]; ok && v != "" {
				out[n] = v
			}
		}
	}
	return encodeLabelSet(out), out
}

// encodeLabelSet builds an unambiguous key for a label set: names sorted, each
// name and value length-prefixed, so distinct sets cannot collide regardless of
// the bytes inside a value.
func encodeLabelSet(m map[string]string) string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	var buf [4]byte
	for _, n := range names {
		binary.BigEndian.PutUint32(buf[:], uint32(len(n)))
		b.Write(buf[:])
		b.WriteString(n)
		binary.BigEndian.PutUint32(buf[:], uint32(len(m[n])))
		b.Write(buf[:])
		b.WriteString(m[n])
	}
	return b.String()
}

// renderLabels formats a label set the way LogQL writes one: names sorted,
// values quoted. evalMetric uses it as the output series' sort key (§5 of the
// design spec); tests use it the same way to assert series order and identity.
func renderLabels(m map[string]string) string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(m[n]))
	}
	b.WriteByte('}')
	return b.String()
}
