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

	// genMu guards nextGen, the gateway's write-generation clock: each sample
	// is stamped max(nextGen, now in Unix µs) at admission and every replica
	// stores that generation, so which of two writes to one series and
	// timestamp is newer does not depend on the order, or the gateway, a
	// replica receives them in -- only on admission order (through several
	// gateways, on their clocks).
	genMu   sync.Mutex
	nextGen int64

	// genSupport records, per ingester client, what its last answer said
	// about taking a sample's generation (PushGenerationsHeader): unknown
	// until it has answered, then takes or legacy. A push to an unknown one
	// asks first (an empty push), so the first writes carry generations to an
	// ingester that takes them; a legacy one -- from before 6.3 -- is sent
	// none and assigns its own, as in 6.2, so a rolling upgrade never sends it
	// a sample it would refuse.
	genSupport map[*Client]*atomic.Int32
}

const (
	genUnknown int32 = iota
	genTakes
	genLegacy
)

// ErrRouterClosed refuses a batch that arrives after Close: the gateway is
// shutting down. It is an outage to the client (503), which retries elsewhere.
var ErrRouterClosed = fmt.Errorf("%w: write routing: the gateway is shutting down", ErrUnavailable)

// RouterOptions configures a Router.
type RouterOptions struct {
	ReplicationFactor int                                      // <= 0 means 1
	Timeout           time.Duration                            // per push; <= 0 means none
	ObserveMember     func(member, outcome string)             // per push: ok, unavailable, error
	ObserveBatch      func(ctx context.Context, b BatchReport) // per batch, once every push ended; ctx is the request's, detached
	Clock             func() int64                             // Unix µs for write generations; nil means the wall clock
	ProbeGenerations  bool                                     // at start, ask each ingester whether it takes generations
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
	rt := &Router{ring: r, clients: map[string]*Client{}, rf: rf, opts: opts, nextGen: 1, genSupport: map[*Client]*atomic.Int32{}}
	for _, m := range r.Members() {
		c, err := NewClient("ingester "+MemberLabel(m), m, copts...)
		if err != nil {
			return nil, err
		}
		rt.clients[m] = c
		rt.genSupport[c] = &atomic.Int32{}
	}
	if opts.ProbeGenerations {
		// Ask every ingester at start, so a write rarely waits on the
		// question. One down now stays unknown, and its first push asks.
		for _, c := range rt.clients {
			rt.wg.Add(1)
			go func() {
				defer rt.wg.Done()
				rt.learnGenSupport(context.Background(), c)
			}()
		}
	}
	return rt, nil
}

// learnGenSupport asks c, with an empty push, whether it takes generations,
// and records the answer. A failed ask leaves it unknown.
func (rt *Router) learnGenSupport(ctx context.Context, c *Client) {
	takes, err := c.PushSamplesGen(ctx, nil, false)
	if err == nil {
		rt.recordGenSupport(c, takes)
	}
}

func (rt *Router) recordGenSupport(c *Client, takes bool) {
	if takes {
		rt.genSupport[c].Store(genTakes)
	} else {
		rt.genSupport[c].Store(genLegacy)
	}
}

// pushTo sends g to c, with generations only if c has said it takes them, and
// learns from the answer whether it does. When c hasn't said yet, it is asked
// first, and the ask and the push share the one client timeout, so a hung
// ingester costs one timeout rather than two.
func (rt *Router) pushTo(ctx context.Context, c *Client, g []metrics.PendingSample) error {
	state := rt.genSupport[c]
	if state.Load() == genUnknown {
		if c.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, c.timeout)
			defer cancel()
		}
		rt.learnGenSupport(ctx, c)
	}
	takes, err := c.PushSamplesGen(ctx, g, state.Load() == genTakes)
	if err == nil {
		rt.recordGenSupport(c, takes)
	}
	return err
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

// PushSamples sends each sample to the RF ingesters that replicate its series,
// stamped with the generation its batch was admitted at.
func (rt *Router) PushSamples(ctx context.Context, samples []metrics.PendingSample) error {
	stamped := lastWritePerSample(samples)
	rt.stamp(stamped)
	return replicate(ctx, rt, "series", stamped, func(s metrics.PendingSample) uint64 { return s.Labels.Hash() },
		func(ctx context.Context, c *Client, g []metrics.PendingSample) error { return rt.pushTo(ctx, c, g) })
}

// lastWritePerSample copies samples keeping, for each series and timestamp,
// only the batch's last sample: the batch shares one generation, so an
// earlier duplicate could not otherwise lose to the later one it was
// overwritten by.
func lastWritePerSample(samples []metrics.PendingSample) []metrics.PendingSample {
	type key struct {
		series uint64
		ts     int64
	}
	last := make(map[key]int, len(samples))
	for i, s := range samples {
		last[key{s.Labels.Hash(), s.TimestampMs}] = i
	}
	out := make([]metrics.PendingSample, 0, len(last))
	for i, s := range samples {
		if last[key{s.Labels.Hash(), s.TimestampMs}] == i {
			out = append(out, s)
		}
	}
	return out
}

// stamp gives the batch one write generation, max(nextGen, now in Unix µs),
// and advances nextGen by one. One per batch, not per sample, keeps the
// counter at the wall clock however large batches are, so a write admitted
// later through another gateway with a synchronized clock still gets a higher
// generation. Two gateways stamping in the same microsecond produce a tie,
// which every read breaks the same way (chunk.Outranks). A gateway restarted
// on a clock that stepped back stamps lower generations than its previous run
// until the clock passes them (limitations.md).
func (rt *Router) stamp(samples []metrics.PendingSample) {
	now := rt.opts.Clock
	if now == nil {
		now = func() int64 { return time.Now().UnixMicro() }
	}
	rt.genMu.Lock()
	gen := max(rt.nextGen, now())
	rt.nextGen = gen + 1
	rt.genMu.Unlock()
	for i := range samples {
		samples[i].Gen = gen
	}
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
	// Admit the batch, or refuse it once Close has started: every Add happens
	// under mu before closed is set, so Close's Wait can never race one.
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return ErrRouterClosed
	}
	rt.wg.Add(len(groups) + 1) // every push, and the collector below
	rt.mu.Unlock()

	results := make(chan result, len(groups)) // never blocks a push
	bg := context.WithoutCancel(ctx)
	var callerGone atomic.Bool // set when the caller leaves before the decision
	for member, g := range groups {
		go func() {
			defer rt.wg.Done()
			err := send(bg, rt.clients[member], g) // the client's timeout bounds it
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
