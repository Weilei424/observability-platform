package logs

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

var retentionNow = time.Unix(10_000_000, 0)

func retentionPolicy() tenant.RetentionPolicy {
	return tenant.RetentionPolicy{
		Default:   24 * time.Hour,
		Overrides: map[string]time.Duration{"team-b": 72 * time.Hour, "keep": 0},
	}
}

func openRetentionStore(t *testing.T, dir string) *ChunkStore {
	t.Helper()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func chunkFiles(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		out[e.Name()] = true
	}
	return out
}

func allEntries(t *testing.T, cs *ChunkStore, id StreamID) []LogEntry {
	t.Helper()
	got, err := cs.StreamEntries(context.Background(), id, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("StreamEntries(%d): %v", id, err)
	}
	return got
}

type retentionFixture struct {
	dir                      string
	cs                       *ChunkStore
	oldDefault, fresh, teamB StreamLabels
	keep                     StreamLabels
	oldName                  string
	oldTs, freshTs           int64
}

// newRetentionFixture writes chunks older than 48h for the default tenant,
// team-b and keep, plus a fresh default-tenant chunk in another stream.
func newRetentionFixture(t *testing.T) retentionFixture {
	t.Helper()
	dir := t.TempDir()
	cs := openRetentionStore(t, dir)
	f := retentionFixture{
		dir:        dir,
		cs:         cs,
		oldDefault: mustLabels(t, map[string]string{"service": "old"}),
		fresh:      mustLabels(t, map[string]string{"service": "fresh"}),
		teamB:      labels.WithTenant(mustLabels(t, map[string]string{"service": "old"}), "team-b"),
		keep:       labels.WithTenant(mustLabels(t, map[string]string{"service": "old"}), "keep"),
		oldTs:      retentionNow.Add(-49 * time.Hour).UnixNano(),
		freshTs:    retentionNow.Add(-time.Hour).UnixNano(),
	}
	if err := cs.IngestStreams(context.Background(), []StreamData{
		{Labels: f.oldDefault, Entries: []LogEntry{{TimestampNs: f.oldTs, Line: "old"}}},
		{Labels: f.teamB, Entries: []LogEntry{{TimestampNs: f.oldTs, Line: "b"}}},
		{Labels: f.keep, Entries: []LogEntry{{TimestampNs: f.oldTs, Line: "k"}}},
		{Labels: f.fresh, Entries: []LogEntry{{TimestampNs: f.freshTs, Line: "new"}}},
	}); err != nil {
		t.Fatal(err)
	}
	refs := cs.index.refs[StreamIDOf(f.oldDefault)]
	if len(refs) != 1 {
		t.Fatalf("old default stream has %d refs, want 1", len(refs))
	}
	f.oldName = refs[0].Name
	return f
}

func TestChunkStoreRetentionDeletesOnlyExpiredTenantChunks(t *testing.T) {
	f := newRetentionFixture(t)
	n, err := f.cs.ApplyRetention(retentionNow, retentionPolicy())
	if err != nil || n != 1 {
		t.Fatalf("ApplyRetention = %d, %v; want 1, nil", n, err)
	}
	files := chunkFiles(t, f.dir)
	if files[f.oldName] || len(files) != 3 {
		t.Fatalf("chunk files after retention = %v, want 3 without %s", files, f.oldName)
	}
	check := func(cs *ChunkStore) {
		t.Helper()
		if got := allEntries(t, cs, StreamIDOf(f.oldDefault)); len(got) != 0 {
			t.Errorf("expired stream still reads %v", got)
		}
		for _, l := range []StreamLabels{f.teamB, f.keep, f.fresh} {
			if got := allEntries(t, cs, StreamIDOf(l)); len(got) != 1 {
				t.Errorf("stream %v reads %d entries, want 1", l, len(got))
			}
		}
		ids := cs.MatchingStreamIDs([]index.Pair{{Name: "service", Value: "old"}})
		for _, id := range ids {
			if id == StreamIDOf(f.oldDefault) {
				t.Errorf("MatchingStreamIDs still lists the emptied stream")
			}
		}
		if _, ok := cs.StreamLabelSet(StreamIDOf(f.oldDefault)); ok {
			t.Errorf("emptied stream still has labels")
		}
	}
	check(f.cs)
	// The manifest reloads the same after a restart.
	check(openRetentionStore(t, f.dir))

	// A second pass finds nothing to delete.
	if n, err := f.cs.ApplyRetention(retentionNow, retentionPolicy()); n != 0 || err != nil {
		t.Fatalf("second pass = %d, %v; want 0, nil", n, err)
	}
}

func TestChunkStoreReadRacingRetentionSkipsDeletedChunk(t *testing.T) {
	f := newRetentionFixture(t)
	// A second chunk in the expired stream that survives retention.
	if err := f.cs.IngestStreams(context.Background(), []StreamData{
		{Labels: f.oldDefault, Entries: []LogEntry{{TimestampNs: f.freshTs, Line: "survivor"}}},
	}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.cs.testBeforeChunkRead = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	type result struct {
		entries []LogEntry
		err     error
	}
	done := make(chan result, 1)
	go func() {
		got, err := f.cs.StreamEntries(context.Background(), StreamIDOf(f.oldDefault), math.MinInt64, math.MaxInt64)
		done <- result{got, err}
	}()
	<-entered
	if n, err := f.cs.ApplyRetention(retentionNow, retentionPolicy()); n != 1 || err != nil {
		t.Fatalf("ApplyRetention = %d, %v; want 1, nil", n, err)
	}
	close(release)
	r := <-done
	if r.err != nil {
		t.Fatalf("StreamEntries racing a deletion: %v", r.err)
	}
	if len(r.entries) != 1 || r.entries[0].Line != "survivor" {
		t.Fatalf("entries = %+v, want only the survivor", r.entries)
	}
}

func TestChunkStoreRestartAfterPartialDeletion(t *testing.T) {
	f := newRetentionFixture(t)
	tmp := filepath.Join(f.dir, "chunks-tmp")
	if err := os.Rename(filepath.Join(f.dir, "chunks", f.oldName), filepath.Join(tmp, f.oldName)); err != nil {
		t.Fatal(err)
	}
	cs := openRetentionStore(t, f.dir)
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Fatalf("tmp after open = %v, %v; want empty", left, err)
	}
	if got := allEntries(t, cs, StreamIDOf(f.oldDefault)); len(got) != 0 {
		t.Fatalf("missing chunk read %v, want nothing", got)
	}
	if len(cs.index.refs[StreamIDOf(f.oldDefault)]) != 1 {
		t.Fatal("the stale ref should survive until the next retention pass")
	}
	if _, err := cs.ApplyRetention(retentionNow, retentionPolicy()); err != nil {
		t.Fatalf("ApplyRetention over a stale ref: %v", err)
	}
	if _, ok := cs.index.refs[StreamIDOf(f.oldDefault)]; ok {
		t.Fatal("ApplyRetention kept the stale ref")
	}
	if _, ok := openRetentionStore(t, f.dir).StreamLabelSet(StreamIDOf(f.oldDefault)); ok {
		t.Fatal("manifest still lists the stale stream after retention")
	}
}

// A clock beyond int64 nanoseconds (year 2262) must saturate, not wrap: a
// wrapped cutoff could keep everything or delete everything.
func TestChunkStoreRetentionSaturatesAFarFutureClock(t *testing.T) {
	f := newRetentionFixture(t)
	n, err := f.cs.ApplyRetention(time.UnixMilli(10_000_000_000_000), tenant.RetentionPolicy{Default: time.Millisecond})
	if err != nil || n != 4 {
		t.Fatalf("ApplyRetention far future = %d, %v; want all 4 chunks, nil", n, err)
	}
}
