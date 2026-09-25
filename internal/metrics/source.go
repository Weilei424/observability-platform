package metrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
)

// Source is the read surface the query engine needs: one call per selector per
// query, coarse enough to cross a process boundary. The local stores implement
// it directly, internal/rpc implements it over HTTP, and Merge combines two.
//
// The label methods carry a Select prefix because the local stores already have
// context-free LabelNames and LabelValues with other signatures.
//
// Slices in a result belong to the caller, which may modify them.
type Source interface {
	Select(ctx context.Context, p SelectParams) ([]SeriesData, error)
	SelectLabelNames(ctx context.Context) ([]string, error)
	SelectLabelValues(ctx context.Context, name string) ([]string, error)
}

// SelectParams describes one bulk read.
//
// A samples select (SeriesOnly false) returns, for every series matching
// Selector, its samples with MinT <= ts <= MaxT, and with Anchor also its latest
// sample with ts < MinT — which is what keeps "latest sample at or before t"
// exact without a lookback bound. A series with neither is omitted. An empty
// range (MaxT < MinT) selects nothing.
//
// A series-only select returns labels only: every matching series with at least
// one sample in [MinT, MaxT], or, with AnyTime, every matching series the index
// knows regardless of its samples.
type SelectParams struct {
	Selector   Selector
	MinT, MaxT int64
	Anchor     bool
	SeriesOnly bool
	AnyTime    bool
}

// ErrInvalidSelect reports a SelectParams combination no caller should build.
var ErrInvalidSelect = errors.New("metrics: invalid select parameters")

// Validate reports whether p is a combination Select accepts: AnyTime only with
// SeriesOnly, and Anchor only without it.
func (p SelectParams) Validate() error {
	if p.AnyTime && !p.SeriesOnly {
		return fmt.Errorf("%w: AnyTime requires SeriesOnly", ErrInvalidSelect)
	}
	if p.Anchor && p.SeriesOnly {
		return fmt.Errorf("%w: Anchor cannot be combined with SeriesOnly", ErrInvalidSelect)
	}
	return nil
}

// SeriesData is one series' answer to a Select. Samples ascend by timestamp with
// one sample per timestamp: the highest write generation, i.e. the last write.
// Anchor, when set, precedes every sample in Samples.
type SeriesData struct {
	Labels  Labels
	Anchor  *Sample
	Samples []Sample
}

// dedupByGeneration collapses a timestamp-sorted slice to one sample per
// timestamp, keeping the highest write generation (last-write-wins). It reuses
// the input's backing array: the write index never passes the read index. This
// is the one merge rule every read path applies — head against blocks, and
// later ingester against store.
func dedupByGeneration(sorted []Sample) []Sample {
	if len(sorted) <= 1 {
		return sorted
	}
	out := sorted[:1]
	for i := 1; i < len(sorted); i++ {
		last := &out[len(out)-1]
		if sorted[i].TimestampMs == last.TimestampMs {
			if sorted[i].Gen > last.Gen {
				*last = sorted[i]
			}
			continue
		}
		out = append(out, sorted[i])
	}
	return out
}

// sortAndDedup sorts samples by timestamp and dedups them by generation.
func sortAndDedup(samples []Sample) []Sample {
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].TimestampMs < samples[j].TimestampMs })
	return dedupByGeneration(samples)
}

// laterSample returns whichever of a and b is later: the larger timestamp, or
// for an equal timestamp the larger generation. Either may be nil.
func laterSample(a, b *Sample) *Sample {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case b.TimestampMs > a.TimestampMs, b.TimestampMs == a.TimestampMs && b.Gen > a.Gen:
		return b
	default:
		return a
	}
}

// perSeriesSource adapts a queryStore — the one-series-at-a-time contract the
// engine was built on — to Source. It is the reference definition of Select:
// native implementations are checked against it, and it keeps working every
// test double that implements only queryStore.
type perSeriesSource struct{ s queryStore }

func (p perSeriesSource) Select(ctx context.Context, sp SelectParams) ([]SeriesData, error) {
	if err := sp.Validate(); err != nil {
		return nil, err
	}
	matched, err := p.s.SelectSeries(sp.Selector)
	if err != nil {
		return nil, err
	}
	out := make([]SeriesData, 0, len(matched))
	for _, ms := range matched {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if sp.SeriesOnly && sp.AnyTime {
			out = append(out, SeriesData{Labels: ms.Labels})
			continue
		}
		if sp.MaxT < sp.MinT {
			continue
		}
		id := SeriesID(ms.Labels.Hash())
		samples, err := p.s.QueryRange(id, sp.MinT, sp.MaxT)
		if err != nil {
			return nil, err
		}
		if sp.SeriesOnly {
			if len(samples) > 0 {
				out = append(out, SeriesData{Labels: ms.Labels})
			}
			continue
		}
		var anchor *Sample
		if sp.Anchor && sp.MinT > math.MinInt64 {
			a, ok, err := p.s.QueryInstant(id, sp.MinT-1)
			if err != nil {
				return nil, err
			}
			if ok {
				anchor = &a
			}
		}
		if len(samples) == 0 && anchor == nil {
			continue
		}
		out = append(out, SeriesData{Labels: ms.Labels, Anchor: anchor, Samples: samples})
	}
	return out, nil
}

func (p perSeriesSource) SelectLabelNames(context.Context) ([]string, error) {
	return p.s.LabelNames(), nil
}

func (p perSeriesSource) SelectLabelValues(_ context.Context, name string) ([]string, error) {
	return p.s.LabelValues(name), nil
}
