package logs

import (
	"context"
	"errors"
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

func TestLogsMergeKeepsStoreEntriesAheadAndSortsStreams(t *testing.T) {
	a := mustLabels(t, map[string]string{"s": "a"})
	b := mustLabels(t, map[string]string{"s": "b"})
	ingester := staticSource{streams: []StreamData{
		{Labels: a, Entries: []LogEntry{{TimestampNs: 10, Line: "head10"}, {TimestampNs: 20, Line: "dup"}}},
	}}
	store := staticSource{streams: []StreamData{
		{Labels: b, Entries: []LogEntry{{TimestampNs: 5, Line: "b5"}}},
		{Labels: a, Entries: []LogEntry{{TimestampNs: 10, Line: "store10"}, {TimestampNs: 20, Line: "dup"}}},
	}}
	got, err := Merge(ingester, store).SelectStreams(context.Background(), nil, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || StreamIDOf(got[0].Labels) > StreamIDOf(got[1].Labels) {
		t.Fatalf("streams = %+v, want both, ascending by ID", got)
	}
	for _, sd := range got {
		if v, _ := sd.Labels.Get("s"); v != "a" {
			continue
		}
		var lines []string
		for _, e := range sd.Entries {
			lines = append(lines, e.Line)
		}
		want := []string{"store10", "head10", "dup"}
		if len(lines) != 3 || lines[0] != want[0] || lines[1] != want[1] || lines[2] != want[2] {
			t.Fatalf("stream a = %v, want %v: persisted ahead of head at ts 10, duplicate once", lines, want)
		}
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
