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
