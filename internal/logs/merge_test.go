package logs

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

type staticSource struct {
	streams []StreamData
	err     error
	names   []string
}

func (s staticSource) SelectStreams(context.Context, []index.Pair, int64, int64) ([]StreamData, error) {
	return s.streams, s.err
}
func (s staticSource) SelectLabelNames(context.Context) ([]string, error) { return s.names, s.err }
func (s staticSource) SelectLabelValues(context.Context, string) ([]string, error) {
	return s.names, s.err
}

// TestLogsMergeKeepsStoreEntriesAheadAndSortsStreams lists both sides in
// descending StreamID order, computed at runtime rather than assumed from the
// label values chosen: a test built from two labels ("a" before "b") would
// pass by coincidence if StreamIDOf happened to agree with string order. It
// also checks every stream's presence and entries, rather than skipping past
// whichever ones a loop does not recognize.
func TestLogsMergeKeepsStoreEntriesAheadAndSortsStreams(t *testing.T) {
	type stream struct {
		labels StreamLabels
		id     StreamID
	}
	streams := make([]stream, 3)
	for i, v := range []string{"stream-0", "stream-1", "stream-2"} {
		l := mustLabels(t, map[string]string{"s": v})
		streams[i] = stream{labels: l, id: StreamIDOf(l)}
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].id > streams[j].id }) // descending
	// streams[0] now holds the highest ID and streams[2] the lowest — both
	// sides below list them in that same descending order.

	ingester := staticSource{streams: []StreamData{
		{Labels: streams[0].labels, Entries: []LogEntry{{TimestampNs: 10, Line: "head10"}, {TimestampNs: 20, Line: "dup"}}},
		{Labels: streams[1].labels, Entries: []LogEntry{{TimestampNs: 1, Line: "head-only"}}},
	}}
	store := staticSource{streams: []StreamData{
		{Labels: streams[0].labels, Entries: []LogEntry{{TimestampNs: 10, Line: "store10"}, {TimestampNs: 20, Line: "dup"}}},
		{Labels: streams[1].labels, Entries: []LogEntry{{TimestampNs: 2, Line: "store-only"}}},
		{Labels: streams[2].labels, Entries: []LogEntry{{TimestampNs: 3, Line: "store-exclusive"}}},
	}}

	got, err := Merge(ingester, store).SelectStreams(context.Background(), nil, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d streams, want 3: %+v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if StreamIDOf(got[i-1].Labels) > StreamIDOf(got[i].Labels) {
			t.Fatalf("streams = %+v, want ascending by ID; the input was listed descending", got)
		}
	}

	byID := make(map[StreamID]StreamData, len(got))
	for _, sd := range got {
		byID[StreamIDOf(sd.Labels)] = sd
	}

	sd0, ok := byID[streams[0].id]
	if !ok {
		t.Fatalf("stream 0 (id %d) missing from merged output: %+v", streams[0].id, got)
	}
	var lines0 []string
	for _, e := range sd0.Entries {
		lines0 = append(lines0, e.Line)
	}
	want0 := []string{"store10", "head10", "dup"}
	if len(lines0) != len(want0) || lines0[0] != want0[0] || lines0[1] != want0[1] || lines0[2] != want0[2] {
		t.Fatalf("stream 0 lines = %v, want %v: persisted ahead of head at ts 10, duplicate once", lines0, want0)
	}

	sd1, ok := byID[streams[1].id]
	if !ok {
		t.Fatalf("stream 1 (id %d) missing from merged output: %+v", streams[1].id, got)
	}
	if len(sd1.Entries) != 2 {
		t.Fatalf("stream 1 entries = %+v, want 2 (one head-only, one store-only: a union, not an overwrite)", sd1.Entries)
	}

	sd2, ok := byID[streams[2].id]
	if !ok {
		t.Fatalf("stream 2 (id %d) missing from merged output: %+v", streams[2].id, got)
	}
	if len(sd2.Entries) != 1 || sd2.Entries[0].Line != "store-exclusive" {
		t.Fatalf("stream 2 entries = %+v, want just store-exclusive (store-only stream)", sd2.Entries)
	}
}

func TestLogsMergePropagatesEitherFailure(t *testing.T) {
	bad := staticSource{err: errors.New("simulated peer failure")}
	good := staticSource{}
	for _, src := range []Source{Merge(bad, good), Merge(good, bad)} {
		if _, err := src.SelectStreams(context.Background(), nil, 0, 1); err == nil {
			t.Fatal("a failing side must fail the merged select")
		}
		if _, err := src.SelectLabelNames(context.Background()); err == nil {
			t.Fatal("a failing side must fail the merged label names")
		}
		if _, err := src.SelectLabelValues(context.Background(), "x"); err == nil {
			t.Fatal("a failing side must fail the merged label values")
		}
	}
}

// orderSource records call start/end for whichever method is invoked, wrapping
// a staticSource — the logs analogue of the metrics merge test's orderSource.
type orderSource struct {
	staticSource
	name   string
	events *[]string
}

func (o orderSource) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	*o.events = append(*o.events, o.name+" SelectStreams start")
	out, err := o.staticSource.SelectStreams(ctx, matchers, minTs, maxTs)
	*o.events = append(*o.events, o.name+" SelectStreams end")
	return out, err
}

func (o orderSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	*o.events = append(*o.events, o.name+" SelectLabelNames start")
	out, err := o.staticSource.SelectLabelNames(ctx)
	*o.events = append(*o.events, o.name+" SelectLabelNames end")
	return out, err
}

func (o orderSource) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	*o.events = append(*o.events, o.name+" SelectLabelValues start")
	out, err := o.staticSource.SelectLabelValues(ctx, name)
	*o.events = append(*o.events, o.name+" SelectLabelValues end")
	return out, err
}

// TestLogsMergeReadsFirstToCompletionBeforeSecond covers all three Source
// methods: metrics.Merge's ordering test (Task 14's original coverage) only
// exercised Select.
func TestLogsMergeReadsFirstToCompletionBeforeSecond(t *testing.T) {
	var events []string
	a := orderSource{staticSource: staticSource{names: []string{"a"}}, name: "first", events: &events}
	b := orderSource{staticSource: staticSource{names: []string{"b"}}, name: "second", events: &events}
	m := Merge(a, b)

	if _, err := m.SelectStreams(context.Background(), nil, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SelectLabelNames(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SelectLabelValues(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"first SelectStreams start", "first SelectStreams end", "second SelectStreams start", "second SelectStreams end",
		"first SelectLabelNames start", "first SelectLabelNames end", "second SelectLabelNames start", "second SelectLabelNames end",
		"first SelectLabelValues start", "first SelectLabelValues end", "second SelectLabelValues start", "second SelectLabelValues end",
	}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

// SelectLabelNames/SelectLabelValues must return the sorted union of both
// sides with no duplicate entries.
func TestLogsMergeUnionsLabelNamesAndValues(t *testing.T) {
	first := staticSource{names: []string{"b", "a"}}
	second := staticSource{names: []string{"a", "c"}}
	m := Merge(first, second)

	names, err := m.SelectLabelNames(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c"}
	if len(names) != len(want) {
		t.Fatalf("label names = %v, want %v (sorted union, no duplicates)", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("label names = %v, want %v (sorted union, no duplicates)", names, want)
		}
	}

	values, err := m.SelectLabelValues(context.Background(), "whatever")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != len(want) {
		t.Fatalf("label values = %v, want %v (sorted union, no duplicates)", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("label values = %v, want %v (sorted union, no duplicates)", values, want)
		}
	}
}

func TestAsSourceAdaptsAReaderAndPassesASourceThrough(t *testing.T) {
	if _, ok := AsSource(readerOnly{&fakeReader{}}).(readerSource); !ok {
		t.Fatal("a plain Reader must be adapted")
	}
	both := readerAndSource{fakeReader: &fakeReader{}}
	if _, ok := AsSource(both).(readerAndSource); !ok {
		t.Fatal("a Reader that is already a Source must be used as-is")
	}
}

type readerOnly struct{ *fakeReader }

type readerAndSource struct {
	*fakeReader
	staticSource
}
