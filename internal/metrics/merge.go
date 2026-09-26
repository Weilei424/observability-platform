package metrics

import (
	"context"
	"slices"
)

// Merge returns a Source that reads first to completion, then second, and
// merges what they return. The querier uses Merge(ingester, store).
//
// The order is the correctness argument, not a detail. The store registers a
// flushed block before it acknowledges the flush, and the ingester drops those
// chunks only after the acknowledgement. So a sample missing from first's
// answer was dropped before that read began — by which time the store had it —
// and it is in second's answer, which is read after. Reading the two
// concurrently, or the store first, could miss a sample in flight.
//
// A series both answer is merged by the generation rule: one sample per
// timestamp, the higher generation winning, and the later of the two anchors.
// Series come back in first's order, then the series only second knows. Either
// side failing fails the whole select: a partial answer is wrong, not late.
func Merge(first, second Source) Source { return merged{first: first, second: second} }

type merged struct{ first, second Source }

func (m merged) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	a, err := m.first.Select(ctx, p)
	if err != nil {
		return nil, err
	}
	b, err := m.second.Select(ctx, p)
	if err != nil {
		return nil, err
	}
	pos := make(map[SeriesID]int, len(a))
	out := make([]SeriesData, 0, len(a)+len(b))
	for _, sd := range a {
		pos[SeriesID(sd.Labels.Hash())] = len(out)
		out = append(out, sd)
	}
	for _, sd := range b {
		i, ok := pos[SeriesID(sd.Labels.Hash())]
		if !ok {
			out = append(out, sd)
			continue
		}
		cur := &out[i]
		cur.Anchor = laterSample(cur.Anchor, sd.Anchor)
		// sortAndDedup is the one merge rule every read path applies (dedupByGeneration's
		// doc, source.go): head against blocks, and here, ingester against store. Using it
		// instead of a second copy of the generation comparison also means peer input that
		// does not conform to a single Source's own invariants — two samples at one
		// timestamp, or out of order, both reachable once samples cross the network — still
		// comes out sorted and deduped: slices.Concat allocates a fresh slice, so this never
		// appends into a peer's backing array.
		cur.Samples = sortAndDedup(slices.Concat(cur.Samples, sd.Samples))
	}
	return out, nil
}

func (m merged) SelectLabelNames(ctx context.Context) ([]string, error) {
	a, err := m.first.SelectLabelNames(ctx)
	if err != nil {
		return nil, err
	}
	b, err := m.second.SelectLabelNames(ctx)
	if err != nil {
		return nil, err
	}
	return unionSorted(a, b), nil
}

func (m merged) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	a, err := m.first.SelectLabelValues(ctx, name)
	if err != nil {
		return nil, err
	}
	b, err := m.second.SelectLabelValues(ctx, name)
	if err != nil {
		return nil, err
	}
	return unionSorted(a, b), nil
}

// unionSorted builds the set of a and b and sorts it via sortedStringSet
// (query.go), rather than a second copy of that normalize-and-sort logic.
func unionSorted(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	return sortedStringSet(set)
}
