package rpc

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var errNoStragglerClient = errors.New("rpc: no client for straggler store")

const (
	stragglerBackoffStart = time.Second
	stragglerBackoffCap   = time.Minute
)

// StragglerOptions bound and instrument a StragglerQueue.
type StragglerOptions struct {
	MaxBytes int64         // total queued body bytes; 0 disables the queue
	MaxAge   time.Duration // an item older than this is dropped
	Clock    func() time.Time
	Observe  func(outcome string) // "ok", "failed", "dropped"
	Pending  func(bytes int64)    // the queued bytes after every change
	Log      *slog.Logger
}

type stragglerItem struct {
	store, path string
	body        []byte
	added, due  time.Time
	backoff     time.Duration
}

// StragglerQueue re-sends flush bodies a quorum flush failed to deliver to
// one of the stores that owns them, so that store regains its copy. It is in
// memory only: a restart loses it, and the data stays on a quorum of stores.
type StragglerQueue struct {
	clients map[string]*Client
	opts    StragglerOptions
	mu      sync.Mutex
	items   []*stragglerItem
	bytes   int64
}

func NewStragglerQueue(clients map[string]*Client, opts StragglerOptions) *StragglerQueue {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &StragglerQueue{clients: clients, opts: opts}
}

func (q *StragglerQueue) observe(o string) {
	if q.opts.Observe != nil {
		q.opts.Observe(o)
	}
}

// pending reports the queued bytes; the caller holds q.mu.
func (q *StragglerQueue) pending() {
	if q.opts.Pending != nil {
		q.opts.Pending(q.bytes)
	}
}

func (q *StragglerQueue) warn(msg, store string) {
	if q.opts.Log != nil {
		q.opts.Log.Warn(msg, slog.String("store", MemberLabel(store)))
	}
}

// Add queues body for store, dropping the oldest items while the queue is
// over its byte bound. A body larger than the bound alone is dropped at once.
func (q *StragglerQueue) Add(store, path string, body []byte) {
	if q.opts.MaxBytes <= 0 {
		q.observe("dropped")
		q.warn("store copy not queued: the straggler queue is disabled", store)
		return
	}
	n := int64(len(body))
	if n > q.opts.MaxBytes {
		q.observe("dropped")
		q.warn("store copy dropped: larger than the straggler queue", store)
		return
	}
	now := q.opts.Clock()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, &stragglerItem{store: store, path: path, body: body, added: now, due: now, backoff: stragglerBackoffStart})
	q.bytes += n
	for q.bytes > q.opts.MaxBytes {
		old := q.items[0]
		q.items = q.items[1:]
		q.bytes -= int64(len(old.body))
		q.observe("dropped")
		q.warn("store copy dropped: the straggler queue is full", old.store)
	}
	q.pending()
}

// Retry drops expired items, then sends every due item once, oldest first.
// A failed item is due again after its backoff, which doubles up to a minute.
func (q *StragglerQueue) Retry(ctx context.Context) {
	now := q.opts.Clock()
	q.mu.Lock()
	var due []*stragglerItem
	kept := q.items[:0]
	for _, it := range q.items {
		if now.Sub(it.added) > q.opts.MaxAge {
			q.bytes -= int64(len(it.body))
			q.observe("dropped")
			q.warn("store copy dropped: older than the straggler queue keeps", it.store)
			continue
		}
		kept = append(kept, it)
		if !now.Before(it.due) {
			due = append(due, it)
		}
	}
	q.items = kept
	q.pending()
	q.mu.Unlock()

	for _, it := range due {
		var err error
		if c := q.clients[it.store]; c != nil {
			err = c.postRaw(ctx, it.path, it.body)
		} else {
			err = errNoStragglerClient
		}
		q.mu.Lock()
		if err == nil {
			// Remove only if still queued: Add may have evicted it mid-send,
			// and its bytes were already subtracted then.
			for i, x := range q.items {
				if x == it {
					q.items = append(q.items[:i], q.items[i+1:]...)
					q.bytes -= int64(len(it.body))
					break
				}
			}
			q.observe("ok")
		} else {
			it.due = q.opts.Clock().Add(it.backoff)
			it.backoff = min(it.backoff*2, stragglerBackoffCap)
			q.observe("failed")
		}
		q.pending()
		q.mu.Unlock()
	}
}

func (q *StragglerQueue) PendingBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}
