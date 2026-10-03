package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

// Router sends validated writes to the ingesters a ring assigns them to: each
// sample by its series fingerprint, each log line by its stream's, so a stream
// travels together. Groups go out concurrently; the answer is the worst
// outcome among them (spec §5.3): a protocol error, then an outage, then a
// cancellation. Safe for concurrent use.
type Router struct {
	ring    *ring.Ring
	clients map[string]*Client
	observe func(member, outcome string)
}

// NewRouter builds a client per ring member. observe, when non-nil, is called
// once per group sent with the member's MemberLabel and its outcome: ok,
// unavailable, or error. A cancellation is the caller's, not the member's,
// and is not observed.
func NewRouter(r *ring.Ring, observe func(member, outcome string)) (*Router, error) {
	rt := &Router{ring: r, clients: map[string]*Client{}, observe: observe}
	for _, m := range r.Members() {
		c, err := NewClient("ingester "+MemberLabel(m), m)
		if err != nil {
			return nil, err
		}
		rt.clients[m] = c
	}
	return rt, nil
}

// MemberLabel is a member URL's host:port, the ingester label on gateway metrics.
func MemberLabel(member string) string {
	u, err := url.Parse(member)
	if err != nil || u.Host == "" {
		return member
	}
	return u.Host
}

// PushSamples sends each sample to the ingester that owns its series.
func (rt *Router) PushSamples(ctx context.Context, samples []metrics.PendingSample) error {
	groups := map[string][]metrics.PendingSample{}
	for _, s := range samples {
		m := rt.ring.Owner(s.Labels.Hash())
		groups[m] = append(groups[m], s)
	}
	return fanOut(ctx, rt, groups, func(ctx context.Context, c *Client, g []metrics.PendingSample) error {
		return c.PushSamples(ctx, g)
	})
}

// PushEntries sends each log entry to the ingester that owns its stream.
func (rt *Router) PushEntries(ctx context.Context, entries []logs.PendingEntry) error {
	groups := map[string][]logs.PendingEntry{}
	for _, e := range entries {
		m := rt.ring.Owner(e.Labels.Hash())
		groups[m] = append(groups[m], e)
	}
	return fanOut(ctx, rt, groups, func(ctx context.Context, c *Client, g []logs.PendingEntry) error {
		return c.PushEntries(ctx, g)
	})
}

func fanOut[T any](ctx context.Context, rt *Router, groups map[string][]T, send func(context.Context, *Client, []T) error) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// Same classification as Client.do: an expired deadline is an outage.
			return fmt.Errorf("%w: write routing: %w", ErrUnavailable, err)
		}
		return err
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for member, g := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := send(ctx, rt.clients[member], g)
			if rt.observe != nil && !errors.Is(err, context.Canceled) {
				rt.observe(MemberLabel(member), outcomeOf(err))
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return worst(errs)
}

func outcomeOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	default:
		return "error"
	}
}

// worst picks the error the caller answers with: a protocol error (any error
// that is neither an outage nor a cancellation) first, then an outage, then a
// cancellation.
func worst(errs []error) error {
	var unavailable, canceled error
	for _, err := range errs {
		switch {
		case errors.Is(err, ErrUnavailable):
			unavailable = err
		case errors.Is(err, context.Canceled):
			canceled = err
		default:
			return fmt.Errorf("rpc: write routing: %w", err)
		}
	}
	if unavailable != nil {
		return unavailable
	}
	return canceled
}
