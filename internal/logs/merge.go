package logs

import (
	"context"
	"errors"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Merge returns a Source that reads first to completion, then second, and
// merges per stream. The querier uses Merge(MergeHeads(ingesters...), store), for the reason
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

// MergeHeads folds every ingester into one Source that reads them in order,
// each to completion before the next, with no outage tolerated: any head
// failing fails the read. See MergeHeadsWith for the merge rules and for
// reads that survive replicated ingesters being down.
func MergeHeads(heads ...Source) Source {
	if len(heads) == 0 {
		panic("logs: MergeHeads needs at least one head")
	}
	if len(heads) == 1 {
		return heads[0]
	}
	return MergeHeadsWith(HeadsOptions{}, heads...)
}

// HeadsOptions let a read skip failed heads that replication covers.
type HeadsOptions struct {
	// Tolerate is how many heads may fail with a Skippable error per read.
	Tolerate int
	// Skippable says which errors are an outage (the querier passes
	// errors.Is(err, rpc.ErrUnavailable)). Nil means nothing is skippable.
	// Whatever Skippable says, nothing is skipped once the read's own context
	// is done, nor is a context.Canceled error: a cancelled read is not an
	// outage. A per-request timeout with a live caller context is an outage
	// and is skippable even though it wraps context.DeadlineExceeded.
	Skippable func(error) bool
	// Observe is called once per head read with the head's index; err is nil
	// on success.
	Observe func(head int, err error)
	// OnSkip is called, once, with the skipped head indexes when a read
	// succeeded only by skipping.
	OnSkip func(skipped []int)
}

// MergeHeadsWith is MergeHeads that may skip up to opts.Tolerate failed heads.
//
// Heads are read sequentially, each to completion, in order, which Merge's
// no-gap argument needs; they are never read in parallel. Skipping is complete
// when Tolerate is at most W-1 for replication factor W: every write was
// acknowledged by W replicas on distinct ingesters, so any W-1 of them
// being unreadable leaves at least one replica of every sample among the
// heads read. A protocol error (anything not Skippable) is never skipped,
// and if every head is skipped the last outage is returned instead of an
// empty answer. With no failure the answer is identical to MergeHeads: the
// collected answers are folded through Merge, nested as Merge(h0, Merge(h1,
// ...)), so series/stream order and the dedup rules are unchanged. Label
// names and values read with the same tolerance. Like MergeHeads it panics
// on zero heads.
func MergeHeadsWith(opts HeadsOptions, heads ...Source) Source {
	if len(heads) == 0 {
		panic("logs: MergeHeadsWith needs at least one head")
	}
	return tolerantHeads{opts: opts, heads: heads}
}

type tolerantHeads struct {
	opts  HeadsOptions
	heads []Source
}

type fixedSource struct {
	streams []StreamData
	strs    []string
}

func (f fixedSource) SelectStreams(context.Context, []index.Pair, int64, int64) ([]StreamData, error) {
	return f.streams, nil
}
func (f fixedSource) SelectLabelNames(context.Context) ([]string, error) { return f.strs, nil }
func (f fixedSource) SelectLabelValues(context.Context, string) ([]string, error) {
	return f.strs, nil
}

// collect reads every head in order, skipping tolerated outages, and folds the
// answers with Merge. wrap turns one answer into a Source; read reads a Source
// the same way (used on the folded result).
func collect[T any](ctx context.Context, t tolerantHeads, readHead func(Source) (T, error), wrap func(T) Source, read func(Source) (T, error)) (T, error) {
	var zero T
	var answers []Source
	var skipped []int
	var lastErr error
	for i, h := range t.heads {
		a, err := readHead(h)
		if t.opts.Observe != nil {
			t.opts.Observe(i, err)
		}
		if err != nil {
			if !t.skip(ctx, err) || len(skipped) >= t.opts.Tolerate {
				return zero, err
			}
			skipped = append(skipped, i)
			lastErr = err
			continue
		}
		answers = append(answers, wrap(a))
	}
	if len(answers) == 0 {
		return zero, lastErr
	}
	acc := answers[len(answers)-1]
	for i := len(answers) - 2; i >= 0; i-- {
		acc = Merge(answers[i], acc)
	}
	out, err := read(acc)
	if err != nil {
		return zero, err
	}
	if len(skipped) > 0 && t.opts.OnSkip != nil {
		t.opts.OnSkip(skipped)
	}
	return out, nil
}

func (t tolerantHeads) skip(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	return t.opts.Skippable != nil && t.opts.Skippable(err)
}

func (t tolerantHeads) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	return collect(ctx, t, func(h Source) ([]StreamData, error) { return h.SelectStreams(ctx, matchers, minTs, maxTs) }, func(a []StreamData) Source { return fixedSource{streams: a} }, func(s Source) ([]StreamData, error) { return s.SelectStreams(ctx, matchers, minTs, maxTs) })
}

func (t tolerantHeads) SelectLabelNames(ctx context.Context) ([]string, error) {
	return collect(ctx, t, func(h Source) ([]string, error) { return h.SelectLabelNames(ctx) }, func(a []string) Source { return fixedSource{strs: a} }, func(s Source) ([]string, error) { return s.SelectLabelNames(ctx) })
}

func (t tolerantHeads) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	return collect(ctx, t, func(h Source) ([]string, error) { return h.SelectLabelValues(ctx, name) }, func(a []string) Source { return fixedSource{strs: a} }, func(s Source) ([]string, error) { return s.SelectLabelValues(ctx, name) })
}
