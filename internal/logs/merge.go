package logs

import (
	"context"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Merge returns a Source that reads first to completion, then second, and
// merges per stream. The querier uses Merge(ingester, store), for the reason
// metrics.Merge gives: the store makes flushed chunks readable before it
// acknowledges, and the ingester drops entries only afterwards, so reading the
// ingester first cannot miss an entry in flight.
//
// A stream both answer keeps second's (persisted) entries ahead of first's
// (head) entries at an equal timestamp — the order a single Store produces —
// and drops exact (ts, line) duplicates. Streams come back ascending by ID.
// Either side failing fails the whole call.
func Merge(first, second Source) Source { return merged{first: first, second: second} }

type merged struct{ first, second Source }

func (m merged) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	heads, err := m.first.SelectStreams(ctx, matchers, minTs, maxTs)
	if err != nil {
		return nil, err
	}
	persisted, err := m.second.SelectStreams(ctx, matchers, minTs, maxTs)
	if err != nil {
		return nil, err
	}
	byID := make(map[StreamID]*StreamData, len(heads)+len(persisted))
	for i := range persisted {
		byID[StreamIDOf(persisted[i].Labels)] = &persisted[i]
	}
	for _, sd := range heads {
		id := StreamIDOf(sd.Labels)
		if p, ok := byID[id]; ok {
			p.Entries = mergeEntries(p.Entries, sd.Entries, minTs, maxTs)
			continue
		}
		cp := sd
		byID[id] = &cp
	}
	ids := make([]StreamID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]StreamData, 0, len(ids))
	for _, id := range ids {
		out = append(out, *byID[id])
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
	return unionStrings(a, b), nil
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
	return unionStrings(a, b), nil
}

func unionStrings(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	return sortedKeys(set)
}
