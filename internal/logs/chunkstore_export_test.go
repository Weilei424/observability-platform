package logs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestExportStreamsYieldsEachStreamOnceWithAllEntries(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	api := mustLabels(t, map[string]string{"service": "api"})
	web := mustLabels(t, map[string]string{"service": "web"})
	db := mustLabels(t, map[string]string{"service": "db"})
	ctx := context.Background()
	if err := cs.IngestStreams(ctx, []StreamData{
		{Labels: api, Entries: []LogEntry{{TimestampNs: 1, Line: "a1"}, {TimestampNs: 2, Line: "a2"}}},
		{Labels: web, Entries: []LogEntry{{TimestampNs: 1, Line: "w1"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cs.IngestStreams(ctx, []StreamData{
		{Labels: api, Entries: []LogEntry{{TimestampNs: 3, Line: "a3"}}},
		{Labels: db, Entries: []LogEntry{{TimestampNs: 5, Line: "d1"}}},
	}); err != nil {
		t.Fatal(err)
	}
	want := map[StreamID]int{StreamIDOf(api): 3, StreamIDOf(web): 1, StreamIDOf(db): 1}
	for _, limit := range []int{1 << 20, 1} {
		got := map[StreamID]int{}
		n, err := cs.ExportStreams(ctx, limit, func(batch []StreamData) error {
			for _, sd := range batch {
				got[StreamIDOf(sd.Labels)] += len(sd.Entries)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if n != 3 {
			t.Errorf("limit %d: streams = %d, want 3", limit, n)
		}
		for id, w := range want {
			if got[id] != w {
				t.Errorf("limit %d: stream %d entries = %d, want %d", limit, id, got[id], w)
			}
		}
	}
	boom := errors.New("boom")
	if _, err := cs.ExportStreams(ctx, 1<<20, func([]StreamData) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}
