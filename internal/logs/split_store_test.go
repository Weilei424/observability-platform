package logs

import (
	"context"
	"path/filepath"
	"testing"
)

func openSplit(t *testing.T, threshold int64) (*Head, *ChunkStore) {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatalf("OpenChunkStore: %v", err)
	}
	h, err := OpenHead(filepath.Join(dir, "wal"), 1<<20, 1, threshold, cs)
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, cs
}

// A head flush moves every entry into the chunk store and empties the head, and
// the two halves each answer only for what they hold.
func TestHeadFlushMovesEntriesToTheChunkStore(t *testing.T) {
	h, cs := openSplit(t, 1<<30)
	l := mustLabels(t, map[string]string{"service": "api"})
	for i := range 5 {
		if err := h.Append(l, int64(i+1), "line"); err != nil {
			t.Fatal(err)
		}
	}
	id := StreamIDOf(l)
	if got, _ := h.StreamEntries(context.Background(), id, 0, 100); len(got) != 5 {
		t.Fatalf("head before flush: %d entries, want 5", len(got))
	}
	if err := h.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := h.StreamCount(); n != 0 {
		t.Fatalf("head holds %d streams after flush, want 0", n)
	}
	got, err := cs.StreamEntries(context.Background(), id, 0, 100)
	if err != nil || len(got) != 5 {
		t.Fatalf("chunk store after flush: %d entries, %v; want 5", len(got), err)
	}
	if ids := cs.MatchingStreamIDs(nil); len(ids) != 1 || ids[0] != id {
		t.Fatalf("chunk store streams = %v, want [%d]", ids, id)
	}
}

// Equal timestamps must keep persisted entries ahead of head entries, the order
// Store.StreamEntries has always produced and the engine's ties depend on.
func TestMergeEntriesKeepsPersistedAheadOfHeadAtEqualTimestamps(t *testing.T) {
	persisted := []LogEntry{{TimestampNs: 10, Line: "p10"}, {TimestampNs: 20, Line: "p20"}}
	head := []LogEntry{{TimestampNs: 20, Line: "h20"}, {TimestampNs: 10, Line: "p10"}, {TimestampNs: 5, Line: "h5"}, {TimestampNs: 99, Line: "late"}}
	got := mergeEntries(persisted, head, 0, 50)
	var lines []string
	for _, e := range got {
		lines = append(lines, e.Line)
	}
	want := []string{"h5", "p10", "p20", "h20"}
	if len(lines) != len(want) {
		t.Fatalf("merged = %v, want %v", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("merged = %v, want %v", lines, want)
		}
	}
}
