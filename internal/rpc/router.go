package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

// Router sends validated writes to the ingesters a ring assigns them to: each
// sample by its series fingerprint, each log line by its stream's, so a stream
// travels together. Every key goes to the ReplicationFactor members
// ring.Replicas names, and a batch succeeds once every key has a majority
// (rf/2+1) of acknowledgements. Safe for concurrent use.
type Router struct {
	ring    *ring.Ring
	clients map[string]*Client
	rf      int
	opts    RouterOptions
	wg      sync.WaitGroup // pushes and batch observations still running
}

// RouterOptions configures a Router.
type RouterOptions struct {
	ReplicationFactor int                          // <= 0 means 1
	Timeout           time.Duration                // per push; <= 0 means none
	ObserveMember     func(member, outcome string) // per push: ok, unavailable, error
	ObserveBatch      func(outcome string)         // per batch, once every push ended: full, degraded, failed
}

// QuorumError reports a write batch in which some key could not reach a
// majority of its replicas. It unwraps to Cause, so errors.Is(err,
// ErrUnavailable) holds when the worst failure was an outage.
type QuorumError struct {
	Kind          string // "series" or "streams"
	Failed, Total int    // keys that missed quorum, of all keys in the batch
	Quorum, RF    int
	Cause         error // the worst failure among the failed keys' replicas
}

func (e *QuorumError) Error() string {
	return fmt.Sprintf("write quorum not met: %d of %d %s could not reach %d of %d ingesters",
		e.Failed, e.Total, e.Kind, e.Quorum, e.RF)
}

func (e *QuorumError) Unwrap() error { return e.Cause }

// NewRouter builds a client per ring member, each bounded by opts.Timeout.
// ObserveMember, when non-nil, is called once per push with the member's
// MemberLabel and its outcome: ok, unavailable, or error.
func NewRouter(r *ring.Ring, opts RouterOptions) (*Router, error) {
	rf := max(opts.ReplicationFactor, 1)
	if n := len(r.Members()); rf > n {
		return nil, fmt.Errorf("rpc: replication factor %d exceeds %d ring members", rf, n)
	}
	var copts []ClientOption
	if opts.Timeout > 0 {
		copts = append(copts, WithRequestTimeout(opts.Timeout))
	}
	rt := &Router{ring: r, clients: map[string]*Client{}, rf: rf, opts: opts}
	for _, m := range r.Members() {
		c, err := NewClient("ingester "+MemberLabel(m), m, copts...)
		if err != nil {
			return nil, err
		}
		rt.clients[m] = c
	}
	return rt, nil
}

// Wait blocks until every push and batch observation started so far has
// ended, including those still running after a batch was answered.
func (rt *Router) Wait() { rt.wg.Wait() }

// MemberLabel is a member URL's host:port, the ingester label on gateway metrics.
func MemberLabel(member string) string {
	u, err := url.Parse(member)
	if err != nil || u.Host == "" {
		return member
	}
	return u.Host
}

// PushSamples sends each sample to the replicas of its series.
func (rt *Router) PushSamples(ctx context.Context, samples []metrics.PendingSample) error {
	return replicate(ctx, rt, "series", samples,
		func(s metrics.PendingSample) uint64 { return s.Labels.Hash() },
		func(ctx context.Context, c *Client, g []metrics.PendingSample) error { return c.PushSamples(ctx, g) })
}

// PushEntries sends each log entry to the replicas of its stream.
func (rt *Router) PushEntries(ctx context.Context, entries []logs.PendingEntry) error {
	return replicate(ctx, rt, "streams", entries,
		func(e logs.PendingEntry) uint64 { return e.Labels.Hash() },
		func(ctx context.Context, c *Client, g []logs.PendingEntry) error { return c.PushEntries(ctx, g) })
}

// quorum is the majority of rf replicas.
func quorum(rf int) int { return rf/2 + 1 }

// replicate sends each item to the rf replicas of its key, one push per
// member. It answers nil the moment every key has quorum acknowledgements;
// the pushes still running then finish in the background on a context
// detached from the caller, bounded by the client timeout, and are still
// observed. If some key cannot reach quorum it waits for every push to end
// and answers a *QuorumError over the worst failure of all failed keys, so
// the answer does not depend on the order failures arrive in. A caller that
// gives up first gets its context's error; the pushes go on regardless.
func replicate[T any](ctx context.Context, rt *Router, kind string, items []T, key func(T) uint64,
	send func(context.Context, *Client, []T) error) error {
	if err := ctx.Err(); err != nil {
		return callerErr(err)
	}
	keyIdx := map[uint64]int{}
	var replicasOf [][]string
	groups := map[string][]T{}
	memberKeys := map[string][]int{}
	for _, it := range items {
		k := key(it)
		ki, seen := keyIdx[k]
		if !seen {
			reps, err := rt.ring.Replicas(k, rt.rf)
			if err != nil {
				return fmt.Errorf("rpc: write routing: %w", err)
			}
			ki = len(replicasOf)
			keyIdx[k] = ki
			replicasOf = append(replicasOf, reps)
			for _, m := range reps {
				memberKeys[m] = append(memberKeys[m], ki)
			}
		}
		for _, m := range replicasOf[ki] {
			groups[m] = append(groups[m], it)
		}
	}
	if len(groups) == 0 {
		return nil
	}

	type result struct {
		member string
		err    error
	}
	results := make(chan result, len(groups)) // never blocks a push
	bg := context.WithoutCancel(ctx)
	for member, g := range groups {
		rt.wg.Add(1)
		go func() {
			defer rt.wg.Done()
			err := send(bg, rt.clients[member], g) // the client's timeout bounds it
			if rt.opts.ObserveMember != nil && !errors.Is(err, context.Canceled) {
				rt.opts.ObserveMember(MemberLabel(member), OutcomeOf(err))
			}
			results <- result{member, err}
		}()
	}

	// One collector per batch owns all quorum state. It sends the decision on
	// decided (buffered: the caller may have left) and observes the batch
	// once every push has ended.
	decided := make(chan error, 1)
	need, maxFail := quorum(rt.rf), rt.rf-quorum(rt.rf)
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		acks := make([]int, len(replicasOf))
		fails := make([][]error, len(replicasOf))
		pending := len(replicasOf) // keys still short of quorum acks
		anyFailure, answered := false, false
		for range len(groups) {
			r := <-results
			if r.err != nil {
				anyFailure = true
			}
			for _, ki := range memberKeys[r.member] {
				if r.err != nil {
					fails[ki] = append(fails[ki], r.err)
					continue
				}
				if acks[ki]++; acks[ki] == need {
					pending--
				}
			}
			if pending == 0 && !answered {
				answered = true
				decided <- nil
			}
		}
		failed := 0
		var causes []error
		for ki := range replicasOf {
			if len(fails[ki]) > maxFail {
				failed++
				causes = append(causes, worst(fails[ki]))
			}
		}
		if !answered {
			// Every push ended and some key is short of quorum, so it failed.
			decided <- &QuorumError{Kind: kind, Failed: failed, Total: len(replicasOf),
				Quorum: need, RF: rt.rf, Cause: worst(causes)}
		}
		if rt.opts.ObserveBatch != nil {
			outcome := "full"
			switch {
			case failed > 0:
				outcome = "failed"
			case anyFailure:
				outcome = "degraded"
			}
			rt.opts.ObserveBatch(outcome)
		}
	}()

	select {
	case err := <-decided:
		return err
	case <-ctx.Done():
		return callerErr(ctx.Err())
	}
}

// callerErr classifies the caller's own context ending: an expired deadline
// is an outage, the same as Client.do; a cancellation stays a cancellation.
func callerErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: write routing: %w", ErrUnavailable, err)
	}
	return err
}

// OutcomeOf names a push's outcome for metrics: ok, unavailable, or error.
func OutcomeOf(err error) string {
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
