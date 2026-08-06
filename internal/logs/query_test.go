package logs

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
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
func (f *fakeReader) StreamEntries(_ context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
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
	res, err := eng.QueryRange(context.Background(), LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}, 0, 100, 3, Backward)
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
	res, err = eng.QueryRange(context.Background(), LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}, 0, 100, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange fwd: %v", err)
	}
	if res[0].Labels["stream"] != "a" || res[0].Entries[0].Line != "a10" {
		t.Fatalf("forward order wrong: %+v", res)
	}
}

func TestQueryRange_RealStore_FilterAndTimeRange(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(api, 100, "GET /a error"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(api, 200, "GET /b ok"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(api, 300, "GET /c error"); err != nil {
		t.Fatal(err)
	}
	eng := NewQueryEngine(s)

	sel, err := ParseLogQL(`{service="api"} |= "error"`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.QueryRange(context.Background(), sel, 0, 1000, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 1 || len(res[0].Entries) != 2 {
		t.Fatalf("expected 2 error lines, got %+v", res)
	}

	// Time range excludes ts=300.
	res, err = eng.QueryRange(context.Background(), sel, 0, 250, 100, Forward)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || len(res[0].Entries) != 1 || res[0].Entries[0].TimestampNs != 100 {
		t.Fatalf("time-range narrowing wrong: %+v", res)
	}
}

// TestQueryRange_DropLabel proves a `| drop` stage removes the named label from
// the returned StreamResult.Labels, while leaving the entries themselves
// unchanged — drop is an output-label transform, not a filter.
func TestQueryRange_DropLabel(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api", "level": "info"})
	if err := s.Append(api, 100, "line one"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(api, 200, "line two"); err != nil {
		t.Fatal(err)
	}
	eng := NewQueryEngine(s)

	sel, err := ParseLogQL(`{service="api"} | drop level`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.QueryRange(context.Background(), sel, 0, 1000, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("got %d streams, want 1: %+v", len(res), res)
	}
	if _, ok := res[0].Labels["level"]; ok {
		t.Errorf("labels = %v, want no level", res[0].Labels)
	}
	if res[0].Labels["service"] != "api" {
		t.Errorf("labels = %v, want service=api kept", res[0].Labels)
	}
	if len(res[0].Entries) != 2 || res[0].Entries[0].Line != "line one" || res[0].Entries[1].Line != "line two" {
		t.Fatalf("entries = %+v, want both lines unchanged", res[0].Entries)
	}
}

// TestQueryRange_DropLabelMergesStreams covers the case the single-stream test
// above cannot see: two streams that differ ONLY by the dropped label are the
// same stream once it is gone, so they must come back merged. A stream is its
// label set — returning two results with identical labels would be a response no
// real Loki can produce, and a client keying on the label set would silently
// lose one of them.
//
// The merged entries must also stay in global timestamp order, interleaved
// across the original streams rather than concatenated stream by stream.
func TestQueryRange_DropLabelMergesStreams(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	info := mustLabels(t, map[string]string{"service": "api", "level": "info"})
	errs := mustLabels(t, map[string]string{"service": "api", "level": "error"})
	if err := s.Append(info, 100, "info first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(errs, 200, "error second"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(info, 300, "info third"); err != nil {
		t.Fatal(err)
	}
	eng := NewQueryEngine(s)

	sel, err := ParseLogQL(`{service="api"} | drop level`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.QueryRange(context.Background(), sel, 0, 1000, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("got %d streams, want them merged into 1: %+v", len(res), res)
	}
	if len(res[0].Labels) != 1 || res[0].Labels["service"] != "api" {
		t.Errorf("labels = %v, want exactly {service:api}", res[0].Labels)
	}
	var lines []string
	for _, e := range res[0].Entries {
		lines = append(lines, e.Line)
	}
	want := []string{"info first", "error second", "info third"}
	if len(lines) != len(want) {
		t.Fatalf("entries = %v, want %v", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("entries = %v, want %v (merged entries must stay in global time order)", lines, want)
		}
	}

	// Without the drop stage the same two streams stay separate — proof the merge
	// is driven by the dropped label, not by collapsing everything.
	plain, err := ParseLogQL(`{service="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	res, err = eng.QueryRange(context.Background(), plain, 0, 1000, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("got %d streams without drop, want 2: %+v", len(res), res)
	}
}

func TestQueryInstant_UpToTime(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api"})
	_ = s.Append(api, 100, "old")
	_ = s.Append(api, 400, "new")
	eng := NewQueryEngine(s)
	sel, _ := ParseLogQL(`{service="api"}`)

	res, err := eng.QueryInstant(context.Background(), sel, 250, 100, Backward)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || len(res[0].Entries) != 1 || res[0].Entries[0].Line != "old" {
		t.Fatalf("instant should return only ts<=250, got %+v", res)
	}
}

// TestQueryRange_EndIsExclusive pins Loki's documented range semantics: results
// have a timestamp >= start and < end. Without this, an entry sitting exactly on
// the boundary is returned twice across adjacent or paginated requests.
func TestQueryRange_EndIsExclusive(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api"})
	for _, e := range []struct {
		ts   int64
		line string
	}{{100, "a"}, {200, "b"}, {300, "c"}} {
		if err := s.Append(api, e.ts, e.line); err != nil {
			t.Fatal(err)
		}
	}
	eng := NewQueryEngine(s)
	sel, err := ParseLogQL(`{service="api"}`)
	if err != nil {
		t.Fatal(err)
	}

	res, err := eng.QueryRange(context.Background(), sel, 100, 300, 100, Forward)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("res = %+v", res)
	}
	var lines []string
	for _, e := range res[0].Entries {
		lines = append(lines, e.Line)
	}
	// start=100 inclusive, end=300 exclusive.
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("entries = %v, want [a b] — start inclusive, end exclusive", lines)
	}

	// Adjacent windows must partition the entries, never duplicate the boundary.
	next, err := eng.QueryRange(context.Background(), sel, 300, 400, 100, Forward)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || len(next[0].Entries) != 1 || next[0].Entries[0].Line != "c" {
		t.Fatalf("adjacent window = %+v, want exactly [c]", next)
	}

	// The instant query's end stays inclusive: `time` is an evaluation point.
	inst, err := eng.QueryInstant(context.Background(), sel, 300, 100, Backward)
	if err != nil {
		t.Fatal(err)
	}
	if len(inst) != 1 || len(inst[0].Entries) != 3 {
		t.Fatalf("instant at ts=300 = %+v, want all 3 entries (end inclusive)", inst)
	}
}

// flattenResults renders a result set as "stream:line:ts" in the order it would
// be read, so tests can compare against an expected sequence.
func flattenResults(res []StreamResult) []string {
	var out []string
	for _, s := range res {
		for _, e := range s.Entries {
			out = append(out, s.Labels["stream"]+":"+e.Line+":"+strconv.FormatInt(e.TimestampNs, 10))
		}
	}
	return out
}

// TestQueryRange_PerStreamCapIsLossless proves the per-stream cap does not change
// results. Timestamps are unique here so the expected top-N is a plain global
// sort — an oracle independent of the engine's own merge, rather than the engine
// compared against itself.
func TestQueryRange_PerStreamCapIsLossless(t *testing.T) {
	a := mustLabels(t, map[string]string{"stream": "a"})
	b := mustLabels(t, map[string]string{"stream": "b"})
	idA, idB := StreamIDOf(a), StreamIDOf(b)

	type ref struct {
		stream, line string
		ts           int64
	}
	var all []ref
	var entriesA, entriesB []LogEntry
	for i := 0; i < 50; i++ {
		entriesA = append(entriesA, LogEntry{TimestampNs: int64(2 * i), Line: "a"})
		entriesB = append(entriesB, LogEntry{TimestampNs: int64(2*i + 1), Line: "b"})
		all = append(all,
			ref{"a", "a", int64(2 * i)},
			ref{"b", "b", int64(2*i + 1)})
	}

	fr := &fakeReader{
		ids:     []StreamID{idA, idB},
		labels:  map[StreamID]StreamLabels{idA: a, idB: b},
		entries: map[StreamID][]LogEntry{idA: entriesA, idB: entriesB},
	}
	eng := NewQueryEngine(fr)
	sel := LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}

	// want computes the expected output independently: order globally by
	// timestamp, truncate to limit, then regroup by stream in first-seen order.
	want := func(limit int, dir Direction) []string {
		ordered := append([]ref(nil), all...)
		sort.Slice(ordered, func(i, j int) bool {
			if dir == Forward {
				return ordered[i].ts < ordered[j].ts
			}
			return ordered[i].ts > ordered[j].ts
		})
		if limit > 0 && limit < len(ordered) {
			ordered = ordered[:limit]
		}
		var order []string
		groups := map[string][]string{}
		for _, r := range ordered {
			if _, seen := groups[r.stream]; !seen {
				order = append(order, r.stream)
			}
			groups[r.stream] = append(groups[r.stream], r.stream+":"+r.line+":"+strconv.FormatInt(r.ts, 10))
		}
		var out []string
		for _, s := range order {
			out = append(out, groups[s]...)
		}
		return out
	}

	for _, dir := range []Direction{Backward, Forward} {
		for _, limit := range []int{1, 2, 7, 50, 99, 100, 200, 0} {
			got, err := eng.QueryRange(context.Background(), sel, 0, 1000, limit, dir)
			if err != nil {
				t.Fatal(err)
			}
			gotFlat, wantFlat := flattenResults(got), want(limit, dir)
			if len(gotFlat) != len(wantFlat) {
				t.Fatalf("dir=%d limit=%d: got %d entries, want %d", dir, limit, len(gotFlat), len(wantFlat))
			}
			for i := range wantFlat {
				if gotFlat[i] != wantFlat[i] {
					t.Fatalf("dir=%d limit=%d: entry %d = %q, want %q", dir, limit, i, gotFlat[i], wantFlat[i])
				}
			}
		}
	}
}

// TestQueryRange_PerStreamCapKeepsTiedTimestamps is the case the per-stream cap
// could plausibly break: two streams share the newest timestamp, and a limit of 2
// must return one entry from each rather than two from whichever stream is read
// first.
func TestQueryRange_PerStreamCapKeepsTiedTimestamps(t *testing.T) {
	a := mustLabels(t, map[string]string{"stream": "a"})
	b := mustLabels(t, map[string]string{"stream": "b"})
	idA, idB := StreamIDOf(a), StreamIDOf(b)

	fr := &fakeReader{
		ids:    []StreamID{idA, idB},
		labels: map[StreamID]StreamLabels{idA: a, idB: b},
		entries: map[StreamID][]LogEntry{
			idA: {{TimestampNs: 10, Line: "old-a"}, {TimestampNs: 500, Line: "tie-a"}},
			idB: {{TimestampNs: 20, Line: "old-b"}, {TimestampNs: 500, Line: "tie-b"}},
		},
	}
	eng := NewQueryEngine(fr)
	sel := LogSelector{Matchers: []index.Pair{{Name: "x", Value: "y"}}}

	res, err := eng.QueryRange(context.Background(), sel, 0, 1000, 2, Backward)
	if err != nil {
		t.Fatal(err)
	}
	got := flattenResults(res)
	if len(got) != 2 {
		t.Fatalf("got %v, want 2 entries", got)
	}
	seen := map[string]bool{}
	for _, g := range got {
		if !strings.HasSuffix(g, ":500") {
			t.Fatalf("got %v, want both entries at the tied newest timestamp 500", got)
		}
		seen[strings.SplitN(g, ":", 2)[0]] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("got %v, want one entry from each stream", got)
	}
}

// TestQueryRange_ContextCancelled proves a client disconnect stops the query
// instead of running it to completion.
func TestQueryRange_ContextCancelled(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(api, 100, "line"); err != nil {
		t.Fatal(err)
	}
	eng := NewQueryEngine(s)
	sel, err := ParseLogQL(`{service="api"}`)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := eng.QueryRange(ctx, sel, 0, 1000, 100, Forward); !errors.Is(err, context.Canceled) {
		t.Fatalf("QueryRange with a cancelled context: err = %v, want context.Canceled", err)
	}
}
