// Package drain holds the ingester's write barrier: once a drain starts, the
// ingester takes no new writes, so a drain that answers 200 has flushed every
// write it ever acknowledged (Phase 6.2).
package drain

import (
	"context"
	"errors"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// ErrDraining refuses a write to an ingester that has started draining. It is
// an outage for the writer, answered 503: the ring no longer routes writes to
// an ingester being removed, and one shutting down is about to stop answering.
var ErrDraining = errors.New("ingester is draining: it takes no new writes")

// Gate admits appends until it is closed, then refuses them for good. Close
// waits for appends already admitted to finish, so once it returns nothing
// can reach the heads any more.
type Gate struct {
	mu       sync.Mutex
	closed   bool
	inflight int
	idle     chan struct{} // closed when inflight reaches 0 after Close
}

// NewGate returns an open gate.
func NewGate() *Gate { return &Gate{idle: make(chan struct{})} }

// enter admits one append, or refuses it with ErrDraining once the gate is
// closed. The caller calls leave when the append is done.
func (g *Gate) enter() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrDraining
	}
	g.inflight++
	return nil
}

func (g *Gate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
	if g.closed && g.inflight == 0 {
		close(g.idle)
	}
}

// Close refuses every append from now on, then waits within ctx for the ones
// already admitted to finish. The gate stays closed even when ctx expires
// first; a later Close waits again. Closing never reopens: an ingester that
// started draining takes writes again only after a restart.
func (g *Gate) Close(ctx context.Context) error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		if g.inflight == 0 {
			close(g.idle)
		}
	}
	g.mu.Unlock()
	select {
	case <-g.idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns ing behind the gate.
func (g *Gate) Metrics(ing metrics.Ingester) metrics.Ingester { return gatedMetrics{g, ing} }

// Logs returns ing behind the gate.
func (g *Gate) Logs(ing logs.Ingester) logs.Ingester { return gatedLogs{g, ing} }

type gatedMetrics struct {
	g   *Gate
	ing metrics.Ingester
}

func (m gatedMetrics) Append(labels metrics.Labels, tsMs int64, value float64) error {
	if err := m.g.enter(); err != nil {
		return err
	}
	defer m.g.leave()
	return m.ing.Append(labels, tsMs, value)
}

// AppendWithGeneration passes a writer-assigned generation through the gate
// when the ingester behind it takes one, and falls back to Append otherwise.
func (m gatedMetrics) AppendWithGeneration(labels metrics.Labels, tsMs int64, value float64, gen int64) error {
	gi, ok := m.ing.(metrics.GenIngester)
	if !ok || gen == 0 {
		return m.Append(labels, tsMs, value)
	}
	if err := m.g.enter(); err != nil {
		return err
	}
	defer m.g.leave()
	return gi.AppendWithGeneration(labels, tsMs, value, gen)
}

type gatedLogs struct {
	g   *Gate
	ing logs.Ingester
}

func (l gatedLogs) Append(labels logs.StreamLabels, tsNs int64, line string) error {
	if err := l.g.enter(); err != nil {
		return err
	}
	defer l.g.leave()
	return l.ing.Append(labels, tsNs, line)
}
