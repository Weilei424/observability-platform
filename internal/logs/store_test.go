package logs

import "testing"

func mustLabels(t *testing.T, m map[string]string) StreamLabels {
	t.Helper()
	l, err := NewStreamLabels(m)
	if err != nil {
		t.Fatalf("NewStreamLabels(%v): %v", m, err)
	}
	return l
}

func TestMemoryStore_AppendAndRead(t *testing.T) {
	s := NewMemoryStore()
	sl := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(sl, 100, "first"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(sl, 200, "second"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	entries := s.StreamEntries(StreamIDOf(sl))
	if len(entries) != 2 {
		t.Fatalf("StreamEntries len = %d, want 2", len(entries))
	}
	if entries[0].Line != "first" || entries[1].Line != "second" {
		t.Errorf("entries out of order: %+v", entries)
	}
	if entries[0].TimestampNs != 100 {
		t.Errorf("entries[0].TimestampNs = %d, want 100", entries[0].TimestampNs)
	}
}

func TestMemoryStore_StreamIdentityOrderIndependent(t *testing.T) {
	s := NewMemoryStore()
	a := mustLabels(t, map[string]string{"service": "api", "level": "info"})
	b := mustLabels(t, map[string]string{"level": "info", "service": "api"})
	_ = s.Append(a, 1, "x")
	_ = s.Append(b, 2, "y")
	if s.StreamCount() != 1 {
		t.Errorf("StreamCount = %d, want 1 (same labels different order)", s.StreamCount())
	}
	if got := len(s.StreamEntries(StreamIDOf(a))); got != 2 {
		t.Errorf("stream has %d entries, want 2", got)
	}
}

func TestMemoryStore_DistinctStreams(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Append(mustLabels(t, map[string]string{"service": "api"}), 1, "x")
	_ = s.Append(mustLabels(t, map[string]string{"service": "web"}), 1, "y")
	if s.StreamCount() != 2 {
		t.Errorf("StreamCount = %d, want 2", s.StreamCount())
	}
}

func TestMemoryStore_StreamEntriesCopy(t *testing.T) {
	s := NewMemoryStore()
	sl := mustLabels(t, map[string]string{"service": "api"})
	_ = s.Append(sl, 1, "x")
	entries := s.StreamEntries(StreamIDOf(sl))
	entries[0].Line = "mutated"
	fresh := s.StreamEntries(StreamIDOf(sl))
	if fresh[0].Line != "x" {
		t.Errorf("StreamEntries returned a live slice; got mutated %q", fresh[0].Line)
	}
}
