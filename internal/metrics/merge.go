package metrics

import (
	"context"
	"errors"
	"slices"
)

// Merge returns a Source that reads first to completion, then second, and
// merges what they return. The querier uses Merge(MergeHeads(ingesters...), store).
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

// MergeHeads folds every ingester into one Source that reads them in order,
// each to completion before the next, with no outage tolerated: any head
// failing fails the read. See MergeHeadsWith for the merge rules and for
// reads that survive replicated ingesters being down.
func MergeHeads(heads ...Source) Source {
	if len(heads) == 0 {
		panic("metrics: MergeHeads needs at least one head")
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
	// OnSkip is called, once, with the read's context and the skipped head
	// indexes when the heads were read only by skipping. It runs when the
	// heads are done, before an outer Merge reads the store, so it records
	// the skip; whoever sees the whole read succeed reports it.
	OnSkip func(ctx context.Context, skipped []int)
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
		panic("metrics: MergeHeadsWith needs at least one head")
	}
	return tolerantHeads{opts: opts, heads: heads}
}

type tolerantHeads struct {
	opts  HeadsOptions
	heads []Source
}

type fixedSource struct {
	series []SeriesData
	strs   []string
}

func (f fixedSource) Select(context.Context, SelectParams) ([]SeriesData, error) {
	return f.series, nil
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
		t.opts.OnSkip(ctx, skipped)
	}
	return out, nil
}

func (t tolerantHeads) skip(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	return t.opts.Skippable != nil && t.opts.Skippable(err)
}

func (t tolerantHeads) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return collect(ctx, t, func(h Source) ([]SeriesData, error) { return h.Select(ctx, p) }, func(a []SeriesData) Source { return fixedSource{series: a} }, func(s Source) ([]SeriesData, error) { return s.Select(ctx, p) })
}

func (t tolerantHeads) SelectLabelNames(ctx context.Context) ([]string, error) {
	return collect(ctx, t, func(h Source) ([]string, error) { return h.SelectLabelNames(ctx) }, func(a []string) Source { return fixedSource{strs: a} }, func(s Source) ([]string, error) { return s.SelectLabelNames(ctx) })
}

func (t tolerantHeads) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	return collect(ctx, t, func(h Source) ([]string, error) { return h.SelectLabelValues(ctx, name) }, func(a []string) Source { return fixedSource{strs: a} }, func(s Source) ([]string, error) { return s.SelectLabelValues(ctx, name) })
}
