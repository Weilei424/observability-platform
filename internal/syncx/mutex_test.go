package syncx

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMutexLockContextGivesUpAtTheDeadline(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := m.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LockContext on a held mutex: err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("LockContext waited %v past a 50ms deadline", d)
	}
	m.Unlock() // the failed LockContext did not take the lock
	if err := m.LockContext(context.Background()); err != nil {
		t.Fatalf("LockContext on a free mutex: %v", err)
	}
	m.Unlock()
}

func TestMutexLockContextRefusesADoneContext(t *testing.T) {
	var m Mutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.LockContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("LockContext with a canceled ctx: err = %v, want Canceled", err)
	}
	if err := m.LockContext(context.Background()); err != nil { // still free
		t.Fatalf("LockContext after a refused one: %v", err)
	}
	m.Unlock()
}

func TestMutexExcludes(t *testing.T) {
	var m Mutex
	n := 0
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				m.Lock()
				n++
				m.Unlock()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if n != 8000 {
		t.Fatalf("n = %d, want 8000", n)
	}
}

func TestMutexUnlockOfUnlockedPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Unlock of an unlocked Mutex did not panic")
		}
	}()
	var m Mutex
	m.Unlock()
}
