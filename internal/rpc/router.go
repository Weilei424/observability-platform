package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
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

	// mu guards closed and every wg.Add, so Close can stop admitting batches
	// and then wait: no Add can follow the Wait it starts. Shutdown cannot
	// rely on the HTTP server for that, since http.Server.Shutdown can give up
	// while a slow handler has yet to reach the router.
	mu     sync.Mutex
	closed bool
	// order holds, per member and key, the done channel of the latest push
	// carrying that key to that member, under mu. A push waits for the ones
	// before it on the same member and keys, so every replica applies a key's
	// writes in the order their batches were admitted: an acknowledged write's
	// late background push can never land after, and so outrank, an overwrite
	// sent after it.
	order map[string]map[uint64]chan struct{}
}

// ErrRouterClosed refuses a batch that arrives after Close: the gateway is
// shutting down. It is an outage to the client (503), which retries elsewhere.
var ErrRouterClosed = fmt.Errorf("%w: write routing: the gateway is shutting down", ErrUnavailable)

// RouterOptions configures a Router.
type RouterOptions struct {
	ReplicationFactor int                                      // <= 0 means 1
	Timeout           time.Duration                            // per push; <= 0 means none
	ObserveMember     func(member, outcome string)             // per push: ok, unavailable, error
	ObserveBatch      func(ctx context.Context, b BatchReport) // per batch, once every push ended; ctx is the request's, detached
}

// BatchReport is a routed batch's final account, once every push ended.
type BatchReport struct {
	Outcome    string   // full, degraded, or failed
	Ingesters  []string // MemberLabel of every replica that failed a push, sorted
	Err        *QuorumError
	CallerGone bool // the caller left before the batch was decided, so it got no QuorumError to log
}

// QuorumError reports a write batch in which some key could not reach a
// majority of its replicas. It unwraps to Cause, so errors.Is(err,
// ErrUnavailable) holds when the worst failure was an outage.
type QuorumError struct {
	Kind          string // "series" or "streams"
	Failed, Total int    // keys that had missed quorum when the batch was decided, of all keys
	Quorum, RF    int
	Ingesters     []string // MemberLabel of each replica that failed a key that missed quorum, sorted
	Cause         error    // the worst failure among the failed keys' replicas
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
	rt := &Router{ring: r, clients: map[string]*Client{}, rf: rf, opts: opts, order: map[string]map[uint64]chan struct{}{}}
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

// WaitContext is Wait bounded by ctx: it answers nil once every push and batch
// observation has ended, or ctx's error if ctx ends first. The pushes it
// stops waiting for still run, each bounded by the client timeout.
func (rt *Router) WaitContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		rt.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rpc: background replica pushes still running: %w", ctx.Err())
	}
}

// Close stops admitting batches — a write arriving from now on answers
// ErrRouterClosed — and then waits, within ctx, for the background pushes and
// batch observations already running. The gateway runs it at shutdown.
func (rt *Router) Close(ctx context.Context) error {
	rt.mu.Lock()
	rt.closed = true
	rt.mu.Unlock()
	return rt.WaitContext(ctx)
}

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
// observed. The moment some key can no longer reach quorum it answers a
// *QuorumError over the worst failure seen so far, without waiting for the
// replicas still running (a hung one would otherwise hold the answer for the
// whole timeout). Which code a mixed batch answers with can therefore depend
// on arrival order: a protocol error that lands after the decision is still
// counted and observed, but does not change the answer. A caller that gives
// up first gets its context's error; the pushes go on regardless.
func replicate[T any](ctx context.Context, rt *Router, kind string, items []T, key func(T) uint64,
	send func(context.Context, *Client, []T) error) error {
	if err := ctx.Err(); err != nil {
		return callerErr(err)
	}
	keyIdx := map[uint64]int{}
	var keys []uint64 // keys[ki] is the key of replicasOf[ki]
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
			keys = append(keys, k)
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
	// Admit the batch, or refuse it once Close has started: every Add happens
	// under mu before closed is set, so Close's Wait can never race one.
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return ErrRouterClosed
	}
	rt.wg.Add(len(groups) + 1) // every push, and the collector below
	// Take this batch's place in each member's per-key order, in the same
	// critical section as admission, so two batches order the same way on
	// every member.
	type turn struct {
		after []chan struct{} // pushes this one must not overtake
		done  chan struct{}
		keys  []uint64
	}
	turns := make(map[string]turn, len(groups))
	for member := range groups {
		byKey := rt.order[member]
		if byKey == nil {
			byKey = map[uint64]chan struct{}{}
			rt.order[member] = byKey
		}
		tn := turn{done: make(chan struct{})}
		seen := map[chan struct{}]bool{}
		for _, ki := range memberKeys[member] {
			k := keys[ki]
			if prev, ok := byKey[k]; ok && !seen[prev] {
				seen[prev] = true
				tn.after = append(tn.after, prev)
			}
			byKey[k] = tn.done
			tn.keys = append(tn.keys, k)
		}
		turns[member] = tn
	}
	rt.mu.Unlock()

	results := make(chan result, len(groups)) // never blocks a push
	bg := context.WithoutCancel(ctx)
	var callerGone atomic.Bool // set when the caller leaves before the decision
	for member, g := range groups {
		tn := turns[member]
		go func() {
			defer rt.wg.Done()
			defer rt.finishTurn(member, tn.keys, tn.done)
			err := rt.awaitTurn(member, tn.after)
			if err == nil {
				err = send(bg, rt.clients[member], g) // the client's timeout bounds it
			}
			if rt.opts.ObserveMember != nil && !errors.Is(err, context.Canceled) {
				rt.opts.ObserveMember(MemberLabel(member), OutcomeOf(err))
			}
			results <- result{member, err}
		}()
	}

	// One collector per batch owns all quorum state. It answers on decided
	// (buffered: the caller may have left) the moment the batch is decided —
	// every key at quorum, or some key past its failure budget — and observes
	// the batch once every push has ended.
	decided := make(chan error, 1)
	need, maxFail := quorum(rt.rf), rt.rf-quorum(rt.rf)
	go func() {
		defer rt.wg.Done()
		acks := make([]int, len(replicasOf))
		fails := make([][]error, len(replicasOf))
		failedBy := make([][]string, len(replicasOf)) // the members behind fails
		pending := len(replicasOf)                    // keys still short of quorum acks
		anyFailure, answered := false, false
		// quorumErr describes the keys past their failure budget so far. It is
		// called at the moment a key fails, so its cause is the worst failure
		// seen by then: a replica still running may yet fail worse, and is
		// counted and observed when it does, but cannot change the answer.
		quorumErr := func() (*QuorumError, int) {
			failed := 0
			var causes []error
			var culprits []string
			for ki := range replicasOf {
				if len(fails[ki]) > maxFail {
					failed++
					causes = append(causes, worst(fails[ki]))
					culprits = append(culprits, failedBy[ki]...)
				}
			}
			if failed == 0 {
				return nil, 0
			}
			slices.Sort(culprits)
			culprits = slices.Compact(culprits)
			return &QuorumError{Kind: kind, Failed: failed, Total: len(replicasOf),
				Quorum: need, RF: rt.rf, Ingesters: culprits, Cause: worst(causes)}, failed
		}
		for range len(groups) {
			r := <-results
			keyFailed := false
			if r.err != nil {
				anyFailure = true
			}
			for _, ki := range memberKeys[r.member] {
				if r.err != nil {
					fails[ki] = append(fails[ki], r.err)
					failedBy[ki] = append(failedBy[ki], MemberLabel(r.member))
					if len(fails[ki]) == maxFail+1 {
						keyFailed = true
					}
					continue
				}
				if acks[ki]++; acks[ki] == need {
					pending--
				}
			}
			switch {
			case answered:
			case keyFailed:
				// Quorum is now impossible for some key: answer without
				// waiting for the replicas still running (spec §5.2).
				answered = true
				qe, _ := quorumErr()
				decided <- qe
			case pending == 0:
				answered = true
				decided <- nil
			}
		}
		qe, failed := quorumErr()
		if rt.opts.ObserveBatch != nil {
			b := BatchReport{Outcome: "full", CallerGone: callerGone.Load()}
			for ki := range replicasOf {
				b.Ingesters = append(b.Ingesters, failedBy[ki]...)
			}
			slices.Sort(b.Ingesters)
			b.Ingesters = slices.Compact(b.Ingesters)
			switch {
			case failed > 0:
				b.Outcome, b.Err = "failed", qe
			case anyFailure:
				b.Outcome = "degraded"
			}
			rt.opts.ObserveBatch(bg, b)
		}
	}()

	select {
	case err := <-decided:
		return err
	case <-ctx.Done():
		callerGone.Store(true)
		return callerErr(ctx.Err())
	}
}

// awaitTurn waits for the earlier pushes to member that carry any of this
// push's keys, so a replica never applies a key's writes out of order. The
// wait is bounded by the router's timeout: if an earlier push is still
// running by then the ingester is hung, and this push fails as an outage
// rather than overtake it.
func (rt *Router) awaitTurn(member string, after []chan struct{}) error {
	if len(after) == 0 {
		return nil
	}
	var expired <-chan time.Time
	if rt.opts.Timeout > 0 {
		t := time.NewTimer(rt.opts.Timeout)
		defer t.Stop()
		expired = t.C
	}
	for _, prev := range after {
		select {
		case <-prev:
		case <-expired:
			return fmt.Errorf("%w: ingester %s: an earlier push of the same keys is still running", ErrUnavailable, MemberLabel(member))
		}
	}
	return nil
}

// finishTurn releases the pushes waiting on this one and forgets the keys it
// is still the latest push for.
func (rt *Router) finishTurn(member string, keys []uint64, done chan struct{}) {
	close(done)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	byKey := rt.order[member]
	for _, k := range keys {
		if byKey[k] == done {
			delete(byKey, k)
		}
	}
	if len(byKey) == 0 {
		delete(rt.order, member)
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
