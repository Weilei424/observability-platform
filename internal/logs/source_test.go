package logs

import (
	"context"
	"errors"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

type errReader struct{ *fakeReader }

func (errReader) StreamEntries(context.Context, StreamID, int64, int64) ([]LogEntry, error) {
	return nil, errors.New("simulated chunk read failure")
}

func TestReaderSourceSelectStreams(t *testing.T) {
	a := mustLabels(t, map[string]string{"stream": "a"})
	b := mustLabels(t, map[string]string{"stream": "b"})
	c := mustLabels(t, map[string]string{"stream": "c"})
	gone := StreamID(12345) // matched by the index but with no label set
	fr := &fakeReader{
		ids:    []StreamID{StreamIDOf(b), gone, StreamIDOf(a), StreamIDOf(c)},
		labels: map[StreamID]StreamLabels{StreamIDOf(a): a, StreamIDOf(b): b, StreamIDOf(c): c},
		entries: map[StreamID][]LogEntry{
			StreamIDOf(a): {{TimestampNs: 10, Line: "a10"}, {TimestampNs: 20, Line: "a20"}},
			StreamIDOf(b): {{TimestampNs: 15, Line: "b15"}},
			StreamIDOf(c): {{TimestampNs: 99, Line: "c99"}},
		},
	}
	got, err := readerSource{r: fr}.SelectStreams(context.Background(), []index.Pair{{Name: "x", Value: "y"}}, 0, 50)
	if err != nil {
		t.Fatalf("SelectStreams: %v", err)
	}
	// Reader order is kept, a stream without labels is skipped, and a stream
	// with no entries in range is omitted.
	if len(got) != 2 {
		t.Fatalf("got %d streams, want 2 (b, a): %+v", len(got), got)
	}
	if v, _ := got[0].Labels.Get("stream"); v != "b" {
		t.Errorf("first stream = %s, want b", v)
	}
	if v, _ := got[1].Labels.Get("stream"); v != "a" || len(got[1].Entries) != 2 {
		t.Errorf("second stream = %s with %d entries, want a with 2", v, len(got[1].Entries))
	}

	if _, err := (readerSource{r: errReader{fr}}).SelectStreams(context.Background(), nil, 0, 50); err == nil {
		t.Error("a failing StreamEntries must fail SelectStreams")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (readerSource{r: fr}).SelectStreams(ctx, nil, 0, 50); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled SelectStreams = %v, want context.Canceled", err)
	}
}

func TestNewQueryEngineAdaptsAReader(t *testing.T) {
	a := mustLabels(t, map[string]string{"stream": "a"})
	fr := &fakeReader{
		ids:     []StreamID{StreamIDOf(a)},
		labels:  map[StreamID]StreamLabels{StreamIDOf(a): a},
		entries: map[StreamID][]LogEntry{StreamIDOf(a): {{TimestampNs: 10, Line: "x"}}},
	}
	names, err := NewQueryEngine(fr).LabelNames(context.Background())
	if err != nil || names != nil {
		t.Fatalf("LabelNames = %v, %v; the fake reports nil names", names, err)
	}
}
