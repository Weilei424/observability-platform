package logs

import (
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Reader is the read surface a QueryEngine needs. *Store satisfies it.
type Reader interface {
	MatchingStreamIDs(matchers []index.Pair) []StreamID
	StreamEntries(id StreamID, minTs, maxTs int64) ([]LogEntry, error)
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

// QueryRange evaluates sel over [startNs, endNs]: match streams by label, read
// entries, apply line filters, order all surviving entries by dir, cap to limit
// (limit <= 0 means no cap), then regroup by stream preserving global order. The
// stream containing the first ordered entry leads. Empty streams are omitted.
func (e *QueryEngine) QueryRange(sel LogSelector, startNs, endNs int64, limit int, dir Direction) ([]StreamResult, error) {
	ids := e.r.MatchingStreamIDs(sel.Matchers)

	type tagged struct {
		id    StreamID
		entry LogEntry
		seq   int
	}
	var all []tagged
	labelsByID := make(map[StreamID]StreamLabels)
	seq := 0
	for _, id := range ids {
		labels, ok := e.r.StreamLabelSet(id)
		if !ok {
			continue
		}
		labelsByID[id] = labels
		entries, err := e.r.StreamEntries(id, startNs, endNs)
		if err != nil {
			return nil, err
		}
		for _, en := range entries {
			if !passesFilters(en.Line, sel.LineFilters) {
				continue
			}
			all = append(all, tagged{id: id, entry: en, seq: seq})
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
		results = append(results, StreamResult{
			Labels:  labelsByID[id].Map(),
			Entries: grouped[id],
		})
	}
	return results, nil
}

// QueryInstant returns up to limit entries with ts <= timeNs, ordered by dir.
func (e *QueryEngine) QueryInstant(sel LogSelector, timeNs int64, limit int, dir Direction) ([]StreamResult, error) {
	return e.QueryRange(sel, 0, timeNs, limit, dir)
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
