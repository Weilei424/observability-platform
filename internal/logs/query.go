package logs

import (
	"context"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Reader is the read surface a QueryEngine needs. *Store satisfies it.
type Reader interface {
	MatchingStreamIDs(matchers []index.Pair) []StreamID
	StreamEntries(ctx context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error)
	StreamLabelSet(id StreamID) (StreamLabels, bool)
	LabelNames() []string
	LabelValues(name string) []string
}

// Direction is the result ordering. Backward (Loki default) is newest-first.
type Direction int

const (
	Backward Direction = iota
	Forward
)

// StreamResult is one stream's labels and its ordered entries.
type StreamResult struct {
	Labels  map[string]string
	Entries []LogEntry
}

// QueryEngine evaluates parsed LogQL over a Reader.
type QueryEngine struct {
	r Reader
}

// NewQueryEngine returns an engine reading from r.
func NewQueryEngine(r Reader) *QueryEngine { return &QueryEngine{r: r} }

// QueryRange evaluates sel over the half-open range [startNs, endNs), matching
// Loki: results have a timestamp >= start and < end. Entries are line-filtered,
// ordered by dir, capped at limit (limit <= 0 means no cap), then regrouped by
// stream preserving global order. The stream containing the first ordered entry
// leads. Empty streams are omitted.
func (e *QueryEngine) QueryRange(ctx context.Context, sel LogSelector, startNs, endNs int64, limit int, dir Direction) ([]StreamResult, error) {
	return e.query(ctx, sel, startNs, endNs, false, limit, dir)
}

// QueryInstant returns up to limit entries with ts <= timeNs, ordered by dir.
// The end is inclusive here because timeNs is an evaluation instant, not a range
// bound.
func (e *QueryEngine) QueryInstant(ctx context.Context, sel LogSelector, timeNs int64, limit int, dir Direction) ([]StreamResult, error) {
	return e.query(ctx, sel, 0, timeNs, true, limit, dir)
}

// taggedEntry carries the stream an entry came from plus its insertion order, so
// the global sort can break ties deterministically.
type taggedEntry struct {
	id    StreamID
	entry LogEntry
	seq   int
}

func (e *QueryEngine) query(ctx context.Context, sel LogSelector, startNs, endNs int64, endInclusive bool, limit int, dir Direction) ([]StreamResult, error) {
	ids := e.r.MatchingStreamIDs(sel.Matchers)

	var all []taggedEntry
	labelsByID := make(map[StreamID]StreamLabels)
	seq := 0
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		labels, ok := e.r.StreamLabelSet(id)
		if !ok {
			continue
		}
		labelsByID[id] = labels
		// The store's range is inclusive on both ends, so it returns a superset
		// when the caller wants a half-open range; the boundary drop below is what
		// makes query_range's [start, end) exact.
		entries, err := e.r.StreamEntries(ctx, id, startNs, endNs)
		if err != nil {
			return nil, err
		}
		kept := make([]LogEntry, 0, len(entries))
		for _, en := range entries {
			if !endInclusive && en.TimestampNs == endNs {
				continue
			}
			if !passesFilters(en.Line, sel.LineFilters) {
				continue
			}
			kept = append(kept, en)
		}

		// Cap each stream's contribution before the global merge. A global top-N
		// can never draw more than N entries from any single stream, so ordering
		// each stream by dir and keeping its first `limit` is lossless.
		//
		// With limit > 0 — every request through the HTTP handlers, since
		// parseLokiLimit defaults to 100 and rejects <= 0 — this bounds what is
		// *carried across* streams at O(streams x limit) rather than O(all
		// matches), and peak is O(largest matching stream + streams x limit).
		// With limit <= 0 the cap below is skipped entirely and every matching
		// entry is retained, so peak is O(all matching entries); that path is
		// reachable only by calling the engine directly.
		//
		// Neither case bounds the transient per-stream working set: StreamEntries
		// still materializes every in-range entry for the stream (plus its dedup
		// map), and `kept` above is sized to that. Bounding the transient set
		// needs selection pushed down into the read — a lazy per-stream cursor
		// feeding a k-way merge — which is the deferred streaming work noted in
		// the design's known limitations.
		sortEntries(kept, dir)
		if limit > 0 && len(kept) > limit {
			kept = kept[:limit]
		}
		for _, en := range kept {
			all = append(all, taggedEntry{id: id, entry: en, seq: seq})
			seq++
		}
	}

	sort.SliceStable(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if a.entry.TimestampNs != b.entry.TimestampNs {
			if dir == Forward {
				return a.entry.TimestampNs < b.entry.TimestampNs
			}
			return a.entry.TimestampNs > b.entry.TimestampNs
		}
		if a.id != b.id {
			return a.id < b.id
		}
		return a.seq < b.seq
	})

	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}

	var order []StreamID
	grouped := make(map[StreamID][]LogEntry)
	for _, t := range all {
		if _, seen := grouped[t.id]; !seen {
			order = append(order, t.id)
		}
		grouped[t.id] = append(grouped[t.id], t.entry)
	}
	results := make([]StreamResult, 0, len(order))
	for _, id := range order {
		labels := labelsByID[id].Map()
		for _, n := range sel.DropLabels {
			delete(labels, n)
		}
		results = append(results, StreamResult{
			Labels:  labels,
			Entries: grouped[id],
		})
	}
	return results, nil
}

// sortEntries orders entries by timestamp per dir. The sort is stable so entries
// sharing a timestamp keep their insertion order, which is the same tie-break the
// global sort applies.
func sortEntries(entries []LogEntry, dir Direction) {
	sort.SliceStable(entries, func(i, j int) bool {
		if dir == Forward {
			return entries[i].TimestampNs < entries[j].TimestampNs
		}
		return entries[i].TimestampNs > entries[j].TimestampNs
	})
}

// LabelNames returns all stream label names.
func (e *QueryEngine) LabelNames() []string { return e.r.LabelNames() }

// LabelValues returns all values for a stream label name.
func (e *QueryEngine) LabelValues(name string) []string { return e.r.LabelValues(name) }

func passesFilters(line string, filters []LineFilter) bool {
	for _, f := range filters {
		if !f.Keep(line) {
			return false
		}
	}
	return true
}
