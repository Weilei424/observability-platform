package rpc

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ t atomic.Int64 }

func (c *fakeClock) now() time.Time      { return time.Unix(0, c.t.Load()) }
func (c *fakeClock) add(d time.Duration) { c.t.Add(int64(d)) }

func (f *fakeStore) received() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bodies...)
}

func queueOver(t *testing.T, stores []*fakeStore, opts StragglerOptions) *StragglerQueue {
	t.Helper()
	clients := map[string]*Client{}
	for _, s := range stores {
		c, err := NewClient("store", s.url)
		if err != nil {
			t.Fatal(err)
		}
		clients[s.url] = c
	}
	return NewStragglerQueue(clients, opts)
}

func TestStragglerQueueDeliversOnRetry(t *testing.T) {
	stores := startStores(t, 1, nil)
	clk := &fakeClock{}
	var outcomes []string
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 1 << 20, MaxAge: time.Minute, Clock: clk.now, Observe: func(o string) { outcomes = append(outcomes, o) }})
	q.Add(stores[0].url, "logs/flush", []byte(`{"streams":[]}`))
	q.Retry(context.Background())
	if n := len(stores[0].received()); n != 1 || q.PendingBytes() != 0 || len(outcomes) != 1 || outcomes[0] != "ok" {
		t.Fatalf("bodies %d, pending %d, outcomes %v", n, q.PendingBytes(), outcomes)
	}
}

func TestStragglerQueueBacksOff(t *testing.T) {
	stores := startStores(t, 1, map[int]int{0: http.StatusServiceUnavailable})
	clk := &fakeClock{}
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 1 << 20, MaxAge: time.Hour, Clock: clk.now})
	q.Add(stores[0].url, "logs/flush", []byte(`{}`))
	q.Retry(context.Background()) // attempt 1 fails; next due in 1s
	q.Retry(context.Background()) // not due
	if n := len(stores[0].received()); n != 1 {
		t.Fatalf("attempts = %d, want 1 before the backoff passes", n)
	}
	clk.add(1100 * time.Millisecond)
	q.Retry(context.Background()) // attempt 2; next due in 2s
	clk.add(1100 * time.Millisecond)
	q.Retry(context.Background()) // not due
	if n := len(stores[0].received()); n != 2 {
		t.Fatalf("attempts = %d, want 2 (backoff doubles)", n)
	}
	for range 10 {
		clk.add(2 * time.Minute)
		q.Retry(context.Background())
	}
	if q.PendingBytes() == 0 {
		t.Fatal("the item was dropped before its age bound")
	}
}

func TestStragglerQueueDropsByAge(t *testing.T) {
	stores := startStores(t, 1, map[int]int{0: http.StatusServiceUnavailable})
	clk := &fakeClock{}
	var outcomes []string
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 1 << 20, MaxAge: time.Minute, Clock: clk.now, Observe: func(o string) { outcomes = append(outcomes, o) }})
	q.Add(stores[0].url, "logs/flush", []byte(`{}`))
	clk.add(2 * time.Minute)
	q.Retry(context.Background())
	if q.PendingBytes() != 0 || outcomes[len(outcomes)-1] != "dropped" {
		t.Fatalf("pending %d, outcomes %v", q.PendingBytes(), outcomes)
	}
}

func TestStragglerQueueDropsOldestOverTheByteBound(t *testing.T) {
	stores := startStores(t, 1, nil)
	clk := &fakeClock{}
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 10, MaxAge: time.Hour, Clock: clk.now})
	q.Add(stores[0].url, "logs/flush", []byte("aaaaaa")) // 6
	q.Add(stores[0].url, "logs/flush", []byte("bbbbbb")) // 12 > 10: drop the oldest
	if q.PendingBytes() != 6 {
		t.Fatalf("pending = %d, want 6", q.PendingBytes())
	}
	q.Retry(context.Background())
	if got := stores[0].received(); len(got) != 1 || got[0] != "bbbbbb" {
		t.Fatalf("delivered %v, want only the newer item", got)
	}
}

// Review Focus 5: one body larger than the bound is dropped at once and leaves the queue intact.
func TestStragglerQueueDropsAnOversizedItem(t *testing.T) {
	stores := startStores(t, 1, nil)
	clk := &fakeClock{}
	var outcomes []string
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 10, MaxAge: time.Hour, Clock: clk.now, Observe: func(o string) { outcomes = append(outcomes, o) }})
	q.Add(stores[0].url, "logs/flush", []byte("aaaa"))
	q.Add(stores[0].url, "logs/flush", []byte("bbbbbbbbbbbbbbbbbbbb"))
	if q.PendingBytes() != 4 || len(outcomes) != 1 || outcomes[0] != "dropped" {
		t.Fatalf("pending %d, outcomes %v", q.PendingBytes(), outcomes)
	}
}

func TestStragglerQueueDisabledAtZeroBytes(t *testing.T) {
	stores := startStores(t, 1, nil)
	q := queueOver(t, stores, StragglerOptions{MaxBytes: 0, MaxAge: time.Hour})
	q.Add(stores[0].url, "logs/flush", []byte("a"))
	if q.PendingBytes() != 0 {
		t.Fatal("a disabled queue kept an item")
	}
}
