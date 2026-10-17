package logs

import (
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
)

var (
	_ TenantCounter = (*Head)(nil)
	_ TenantCounter = (*Store)(nil)
)

func tenantStream(t *testing.T, tenantID, svc string) StreamLabels {
	t.Helper()
	return labels.WithTenant(mustLabels(t, map[string]string{"service": svc}), tenantID)
}

func TestTenantCount_HeadCountsFlushesAndReplays(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	walDir := filepath.Join(dir, "wal")
	h, err := OpenHead(walDir, 1<<20, 1, 1<<30, cs, HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b1, b2, d := tenantStream(t, "team-b", "a"), tenantStream(t, "team-b", "b"), tenantStream(t, "anonymous", "c")
	for i, l := range []StreamLabels{b1, b1, b2, d} {
		if err := h.Append(l, int64(i+1), "x"); err != nil {
			t.Fatal(err)
		}
	}
	check := func(h *Head, b, a int) {
		t.Helper()
		if h.TenantStreams("team-b") != b || h.TenantStreams("anonymous") != a {
			t.Fatalf("team-b=%d anonymous=%d, want %d/%d", h.TenantStreams("team-b"), h.TenantStreams("anonymous"), b, a)
		}
	}
	check(h, 2, 1)
	if !h.HasStream(StreamIDOf(b1)) || h.HasStream(StreamID(12345)) {
		t.Fatal("HasStream wrong")
	}
	m := h.TenantStreamCounts()
	if len(m) != 2 || m["team-b"] != 2 {
		t.Fatalf("counts = %v", m)
	}
	m["team-b"] = 99
	check(h, 2, 1)

	// Reopen without flushing: WAL replay rebuilds the counts.
	if err := h.CloseWithoutFlush(); err != nil {
		t.Fatal(err)
	}
	h2, err := OpenHead(walDir, 1<<20, 1, 1<<30, cs, HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	check(h2, 2, 1)

	if err := h2.Flush(); err != nil {
		t.Fatal(err)
	}
	check(h2, 0, 0)
	if c := h2.TenantStreamCounts(); len(c) != 0 {
		t.Fatalf("counts after flush = %v, want empty", c)
	}
	if h2.HasStream(StreamIDOf(b1)) {
		t.Fatal("HasStream true after flush")
	}
	_ = h2.Close()
}

func TestTenantCount_StorePassesThrough(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(filepath.Join(dir, "wal"), filepath.Join(dir, "chunks"), filepath.Join(dir, "index"), 1<<20, 1, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	l := tenantStream(t, "team-b", "a")
	if err := s.Append(l, 1, "x"); err != nil {
		t.Fatal(err)
	}
	if s.TenantStreams("team-b") != 1 || !s.HasStream(StreamIDOf(l)) || s.TenantStreamCounts()["team-b"] != 1 {
		t.Fatal("Store pass-through wrong")
	}
}
