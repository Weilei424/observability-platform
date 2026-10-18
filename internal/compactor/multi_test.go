package compactor

import (
	"context"
	"errors"
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeMgr struct {
	compact, deleted int
	err              error
	compactCalls     int
	retentionCalls   int
}

func (f *fakeMgr) CompactOnce(func([]block.BlockInfo) [][]string) (int, error) {
	f.compactCalls++
	if f.err != nil {
		return f.compact, f.err
	}
	if f.compactCalls > 1 { // stable after the first round
		return 0, nil
	}
	return f.compact, nil
}

func (f *fakeMgr) ApplyRetention(time.Time, tenant.RetentionPolicy) (int, error) {
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
		want := 2 // one round of work, then a round that finds it stable
		if f.err != nil {
			want = 1
		}
		if f.compactCalls != want {
			t.Fatalf("store %d called %d times, want %d", i, f.compactCalls, want)
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
	n, err := m.ApplyRetention(time.Now(), tenant.RetentionPolicy{Default: time.Hour})
	if n != 5 || !errors.Is(err, boom) || ms[1].retentionCalls != 1 {
		t.Fatalf("n=%d err=%v calls=%d", n, err, ms[1].retentionCalls)
	}
	if len(seen) != 2 || seen[0] != "a=error" || seen[1] != "b=ok" {
		t.Fatalf("observed %v", seen)
	}
}

// roundsMgr reports rounds[i] on its i-th call and 0 afterwards.
type roundsMgr struct {
	rounds []int
	calls  int
}

func (r *roundsMgr) CompactOnce(func([]block.BlockInfo) [][]string) (int, error) {
	r.calls++
	if r.calls <= len(r.rounds) {
		return r.rounds[r.calls-1], nil
	}
	return 0, nil
}

func (r *roundsMgr) ApplyRetention(time.Time, tenant.RetentionPolicy) (int, error) { return 0, nil }

func TestRunOnceCompactsHealthyStoreToStableWhileAnotherFails(t *testing.T) {
	down := &fakeMgr{err: errors.New("down")}
	healthy := &roundsMgr{rounds: []int{2, 1, 3}}
	multi := NewMultiBlockManager([]string{"a", "b"}, []BlockManager{down, healthy},
		func(err error) string {
			if err != nil {
				return "error"
			}
			return "ok"
		}, func(string, string) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg, inst := observability.NewRegistry(observability.RegistryOptions{})
	c := New(nil, multi, nil, time.Now, Config{Ranges: []int64{1000}}, inst.Maintenance,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.RunOnce(context.Background())

	if healthy.calls <= len(healthy.rounds) {
		t.Fatalf("healthy store called %d times, never reached its final 0", healthy.calls)
	}
	if got := counter(t, reg, "obs_compactions_total"); got != 6 {
		t.Errorf("CompactionsTotal = %v, want 6", got)
	}
	if got := counter(t, reg, "obs_compaction_failures_total"); got < 1 {
		t.Errorf("CompactionFailuresTotal = %v, want >= 1", got)
	}
}

func counter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name && len(mf.GetMetric()) > 0 {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}
