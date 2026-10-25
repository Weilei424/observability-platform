package logs

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A flush that gives up waiting for the head lock never reaches the flush
// hook, so its error is ErrLockWait: the caller must report it itself.
func TestFlushContextLockWaitIsReportedByTheCaller(t *testing.T) {
	h, _ := openPolicyHead(t, nil, HeadOptions{}, 1<<30)
	t.Cleanup(func() { _ = h.CloseWithoutFlush() })
	hooked := false
	h.SetFlushHook(func(error) { hooked = true })

	h.mu.Lock() // an append's threshold flush holding the head
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := h.FlushContext(ctx)
	h.mu.Unlock()

	if !errors.Is(err, ErrLockWait) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrLockWait wrapping the deadline", err)
	}
	if hooked {
		t.Fatal("the flush hook ran for a flush that never started")
	}
}

// A bounded close gives up on a held head lock at its deadline: it syncs the
// WAL under the WAL's own lock and leaves it open (whatever holds the head may
// still checkpoint it), reporting ErrLockWait.
func TestCloseWithoutFlushContextGivesUpOnAHeldLock(t *testing.T) {
	h, _ := openPolicyHead(t, nil, HeadOptions{}, 1<<30)
	t.Cleanup(func() { _ = h.CloseWithoutFlush() })

	h.mu.Lock() // a drain's flush holding the head
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := h.CloseWithoutFlushContext(ctx)
	waited := time.Since(start)
	h.mu.Unlock()

	if !errors.Is(err, ErrLockWait) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrLockWait wrapping the deadline", err)
	}
	if waited > 2*time.Second {
		t.Fatalf("waited %v past a 20ms deadline", waited)
	}
	if err := h.Append(mustLabels(t, map[string]string{"service": "api"}), 1, "after"); err != nil {
		t.Fatalf("the WAL was closed under a head it never locked: %v", err)
	}
}

// A free head lock is taken even when the shutdown budget is already spent:
// the WAL is then closed, synced, as usual.
func TestCloseWithoutFlushContextClosesWhenTheLockIsFree(t *testing.T) {
	h, _ := openPolicyHead(t, nil, HeadOptions{}, 1<<30)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.CloseWithoutFlushContext(ctx); err != nil {
		t.Fatalf("close with a free lock and a spent budget: %v", err)
	}
	if err := h.Append(mustLabels(t, map[string]string{"service": "api"}), 1, "after"); err == nil {
		t.Fatal("the WAL is still open after the close")
	}
}
