package logs

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// fakeReader is a deterministic in-memory Reader for ordering/limit tests.
type fakeReader struct {
	ids     []StreamID
	labels  map[StreamID]StreamLabels
	entries map[StreamID][]LogEntry
}

func (f *fakeReader) MatchingStreamIDs(_ []index.Pair) []StreamID { return f.ids }
func (f *fakeReader) StreamEntries(id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	var out []LogEntry
	for _, e := range f.entries[id] {
		if e.TimestampNs >= minTs && e.TimestampNs <= maxTs {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeReader) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	l, ok := f.labels[id]
	return l, ok
}
func (f *fakeReader) LabelNames() []string        { return nil }
func (f *fakeReader) LabelValues(string) []string { return nil }

func TestQueryRange_GlobalOrderAndLimit(t *testing.T) {
	a := mustLabels(t, map[string]string{"stream": "a"})
	b := mustLabels(t, map[string]string{"stream": "b"})
	fr := &fakeReader{
		ids:    []StreamID{StreamIDOf(a), StreamIDOf(b)},
		labels: map[StreamID]StreamLabels{StreamIDOf(a): a, StreamIDOf(b): b},
		entries: map[StreamID][]LogEntry{
			StreamIDOf(a): {{TimestampNs: 10, Line: "a10"}, {TimestampNs: 30, Line: "a30"}},
			StreamIDOf(b): {{TimestampNs: 20, Line: "b20"}, {TimestampNs: 40, Line: "b40"}},
		},
	}
	eng := NewQueryEngine(fr)

	// Backward + limit 3 → newest three: b40, a30, b20 ; regrouped, b leads.
	res, err := eng.QueryRange(LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}, 0, 100, 3, Backward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 2 || res[0].Labels["stream"] != "b" {
		t.Fatalf("expected stream b first, got %+v", res)
	}
	var lines []string
	for _, s := range res {
		for _, e := range s.Entries {
			lines = append(lines, e.Line)
		}
	}
	// b: b40,b20 ; a: a30  → total 3 entries kept.
	if len(lines) != 3 {
		t.Fatalf("expected 3 entries after limit, got %v", lines)
	}

	// Forward → oldest first: a10, b20, a30, b40.
	res, err = eng.QueryRange(LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}, 0, 100, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange fwd: %v", err)
	}
	if res[0].Labels["stream"] != "a" || res[0].Entries[0].Line != "a10" {
		t.Fatalf("forward order wrong: %+v", res)
	}
}
