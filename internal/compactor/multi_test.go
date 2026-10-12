package compactor

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

type fakeMgr struct {
	compact, deleted int
	err              error
	compactCalls     int
	retentionCalls   int
}

func (f *fakeMgr) CompactOnce(func([]block.BlockInfo) [][]string) (int, error) {
	f.compactCalls++
	return f.compact, f.err
}

func (f *fakeMgr) ApplyRetention(time.Time, time.Duration) (int, error) {
	f.retentionCalls++
	return f.deleted, f.err
}

func newMulti(t *testing.T, ms []*fakeMgr, seen *[]string) *MultiBlockManager {
	t.Helper()
	names := []string{"a", "b", "c"}[:len(ms)]
	bms := make([]BlockManager, len(ms))
	for i, m := range ms {
		bms[i] = m
	}
	outcome := func(err error) string {
		if err == nil {
			return "ok"
		}
		return "error"
	}
	observe := func(s, o string) { *seen = append(*seen, s+"="+o) }
	return NewMultiBlockManager(names, bms, outcome, observe, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestMultiCompactOnceRunsEveryStoreAndReportsFirstError(t *testing.T) {
	boom := errors.New("boom")
	ms := []*fakeMgr{{compact: 2}, {compact: 5, err: boom}, {compact: 3}}
	var seen []string
	m := newMulti(t, ms, &seen)
	n, err := m.CompactOnce(nil)
	if n != 10 || !errors.Is(err, boom) {
		t.Fatalf("n=%d err=%v, want 10 and boom", n, err)
	}
	for i, f := range ms {
		if f.compactCalls != 1 {
			t.Fatalf("store %d called %d times", i, f.compactCalls)
		}
	}
	want := []string{"a=ok", "b=error", "c=ok"}
	if len(seen) != 3 || seen[0] != want[0] || seen[1] != want[1] || seen[2] != want[2] {
		t.Fatalf("observed %v, want %v", seen, want)
	}
}

func TestMultiApplyRetentionRunsEveryStore(t *testing.T) {
	boom := errors.New("boom")
	ms := []*fakeMgr{{deleted: 1, err: boom}, {deleted: 4}}
	var seen []string
	m := newMulti(t, ms, &seen)
	n, err := m.ApplyRetention(time.Now(), time.Hour)
	if n != 5 || !errors.Is(err, boom) || ms[1].retentionCalls != 1 {
		t.Fatalf("n=%d err=%v calls=%d", n, err, ms[1].retentionCalls)
	}
	if len(seen) != 2 || seen[0] != "a=error" || seen[1] != "b=ok" {
		t.Fatalf("observed %v", seen)
	}
}

func TestMultiFailingStoreStillLetsOthersCountDriveTheLoop(t *testing.T) {
	ms := []*fakeMgr{{err: errors.New("down")}, {compact: 1}}
	var seen []string
	m := newMulti(t, ms, &seen)
	n, _ := m.CompactOnce(nil)
	if n != 1 {
		t.Fatalf("n=%d, want 1", n)
	}
}
