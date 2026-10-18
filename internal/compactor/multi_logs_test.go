package compactor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

type fakeLogs struct {
	deleted int
	err     error
	calls   int
	policy  tenant.RetentionPolicy
}

func (f *fakeLogs) ApplyLogsRetention(_ time.Time, p tenant.RetentionPolicy) (int, error) {
	f.calls++
	f.policy = p
	return f.deleted, f.err
}

func okOrError(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func TestMultiLogManagerAppliesEveryStoreAndCountsTheFailure(t *testing.T) {
	boom := errors.New("boom")
	ms := []*fakeLogs{{deleted: 2}, {deleted: 1, err: boom}, {deleted: 3}}
	lms := make([]LogManager, len(ms))
	for i, m := range ms {
		lms[i] = m
	}
	var seen []string
	m := NewMultiLogManager([]string{"a", "b", "c"}, lms, okOrError,
		func(s, o string) { seen = append(seen, s+"="+o) }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := m.ApplyLogsRetention(time.Now(), tenant.RetentionPolicy{Default: time.Hour})
	if n != 6 || !errors.Is(err, boom) {
		t.Fatalf("n=%d err=%v, want 6 and boom", n, err)
	}
	for i, f := range ms {
		if f.calls != 1 {
			t.Fatalf("store %d called %d times, want 1", i, f.calls)
		}
	}
	if len(seen) != 3 || seen[0] != "a=ok" || seen[1] != "b=error" || seen[2] != "c=ok" {
		t.Fatalf("observed %v", seen)
	}
}

func TestRunOnceAppliesLogsRetentionAndCountsIt(t *testing.T) {
	lm := &fakeLogs{deleted: 4, err: errors.New("partial")}
	policy := tenant.RetentionPolicy{Default: time.Hour, Overrides: map[string]time.Duration{"keep": 0}}
	reg, inst := observability.NewRegistry(observability.RegistryOptions{})
	c := New(nil, nil, nil, time.Now, Config{RetentionPolicy: policy}, inst.Maintenance,
		slog.New(slog.NewTextHandler(io.Discard, nil)), WithLogs(lm))
	c.RunOnce(context.Background())
	if lm.calls != 1 || lm.policy.Default != time.Hour || len(lm.policy.Overrides) != 1 {
		t.Fatalf("logs retention calls=%d policy=%+v", lm.calls, lm.policy)
	}
	if got := counter(t, reg, "obs_logs_retention_deleted_chunks_total"); got != 4 {
		t.Errorf("LogsRetentionDeletedTotal = %v, want 4", got)
	}
}
