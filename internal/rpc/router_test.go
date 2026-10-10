package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

type fakeIngester struct {
	url     string
	srv     *httptest.Server
	metrics *recordingMetrics
	logs    *recordingLogs
}

// startIngesters starts n push servers; status, when non-zero for an index,
// makes that server answer every push with it; hang, when true for an index,
// makes that server never answer until the client gives up or the test ends.
func startIngesters(t *testing.T, n int, status map[int]int, hang map[int]bool) []*fakeIngester {
	t.Helper()
	out := make([]*fakeIngester, n)
	for i := range out {
		f := &fakeIngester{metrics: &recordingMetrics{}, logs: &recordingLogs{}}
		r := chi.NewRouter()
		stop := make(chan struct{})
		if hang[i] {
			r.HandleFunc("/*", func(_ http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-stop:
				}
			})
		} else if code := status[i]; code != 0 {
			r.HandleFunc("/*", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
		} else {
			r.Route("/internal/v1", func(r chi.Router) { MountWrites(r, f.metrics, f.logs, observability.NewIngestMetrics()) })
		}
		f.srv = httptest.NewServer(r)
		t.Cleanup(f.srv.Close)
		t.Cleanup(func() { close(stop) }) // runs first: releases hung handlers so Close returns
		f.url = f.srv.URL
		out[i] = f
	}
	return out
}

func newTestRouter(t *testing.T, ings []*fakeIngester, rf int, observe func(string, string)) (*Router, *ring.Ring) {
	t.Helper()
	return newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: rf, Timeout: 2 * time.Second, ObserveMember: observe})
}

func newTestRouterOpts(t *testing.T, ings []*fakeIngester, opts RouterOptions) (*Router, *ring.Ring) {
	t.Helper()
	urls := make([]string, len(ings))
	for i, f := range ings {
		urls[i] = f.url
	}
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRouter(r, opts)
	if err == nil {
		t.Cleanup(rt.Wait)
	}
	if err != nil {
		t.Fatal(err)
	}
	return rt, r
}

func samplesFor(t *testing.T, n int) []metrics.PendingSample {
	t.Helper()
	out := make([]metrics.PendingSample, n)
	for i := range out {
		l, _ := metrics.NewLabels(map[string]string{"__name__": "m", "i": strconv.Itoa(i)})
		out[i] = metrics.PendingSample{Labels: l, TimestampMs: int64(i), Value: float64(i)}
	}
	return out
}

func TestRouterSendsEachSampleOnlyToItsOwner(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	rt, r := newTestRouter(t, ings, 1, nil)
	in := samplesFor(t, 300)
	if err := rt.PushSamples(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, f := range ings {
		for _, got := range f.metrics.got {
			if owner := r.Owner(got.Labels.Hash()); owner != f.url {
				t.Fatalf("sample for %s landed on %s", owner, f.url)
			}
		}
		if len(f.metrics.got) == 0 {
			t.Errorf("%s got no samples out of 300", f.url)
		}
		total += len(f.metrics.got)
	}
	if total != 300 {
		t.Errorf("delivered %d samples, want 300", total)
	}
}

func TestRouterKeepsAStreamTogether(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	rt, _ := newTestRouter(t, ings, 1, nil)
	s, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	var in []logs.PendingEntry
	for i := range 50 {
		in = append(in, logs.PendingEntry{Labels: s, TimestampNs: int64(i), Line: "l"})
	}
	if err := rt.PushEntries(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	holders := 0
	for _, f := range ings {
		if n := len(f.logs.got); n > 0 {
			holders++
			if n != 50 {
				t.Errorf("%s holds %d of the stream's 50 lines", f.url, n)
			}
		}
	}
	if holders != 1 {
		t.Errorf("stream spread over %d ingesters, want 1", holders)
	}
}

func TestRouterOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status map[int]int
		check  func(error) bool
		want   string
	}{
		{"one unavailable", map[int]int{1: http.StatusServiceUnavailable}, func(e error) bool { return errors.Is(e, ErrUnavailable) }, "ErrUnavailable"},
		{"protocol error", map[int]int{0: http.StatusBadRequest},
			func(e error) bool { return e != nil && !errors.Is(e, ErrUnavailable) }, "a non-unavailable error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ings := startIngesters(t, 3, tc.status, nil)
			var mu sync.Mutex
			outcomes := map[string]int{}
			rt, _ := newTestRouter(t, ings, 1, func(_, outcome string) { mu.Lock(); outcomes[outcome]++; mu.Unlock() })
			err := rt.PushSamples(context.Background(), samplesFor(t, 300))
			if !tc.check(err) {
				t.Fatalf("err = %v, want %s", err, tc.want)
			}
			rt.Wait() // the answer comes at the first failed key; the other pushes run on
			mu.Lock()
			defer mu.Unlock()
			if outcomes["ok"] == 0 || outcomes["ok"]+outcomes["unavailable"]+outcomes["error"] != 3 {
				t.Errorf("outcomes = %v, want one per ingester with at least one ok", outcomes)
			}
		})
	}
}

// A batch that touches only healthy ingesters succeeds although another is down.
func TestRouterSucceedsWhenOnlyHealthyMembersAreTouched(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{2: http.StatusServiceUnavailable}, nil)
	rt, r := newTestRouter(t, ings, 1, nil)
	var healthyOnly []metrics.PendingSample
	for _, s := range samplesFor(t, 300) {
		if r.Owner(s.Labels.Hash()) != ings[2].url {
			healthyOnly = append(healthyOnly, s)
		}
	}
	if err := rt.PushSamples(context.Background(), healthyOnly); err != nil {
		t.Fatalf("batch avoiding the down ingester failed: %v", err)
	}
}

func TestRouterCanceledContext(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	rt, _ := newTestRouter(t, ings, 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rt.PushSamples(ctx, samplesFor(t, 30))
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want context.Canceled and not ErrUnavailable", err)
	}
}

func TestMemberLabel(t *testing.T) {
	if got := MemberLabel("http://ingester-0.ingester-headless:8080"); got != "ingester-0.ingester-headless:8080" {
		t.Errorf("MemberLabel = %q", got)
	}
}

func TestRouterExpiredDeadlineIsUnavailable(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	rt, _ := newTestRouter(t, ings, 1, nil)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := rt.PushSamples(ctx, samplesFor(t, 30))
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrUnavailable wrapping DeadlineExceeded", err)
	}
}

func TestRouterReplicatesEveryKeyToRFMembers(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	rt, _ := newTestRouter(t, ings, 3, nil)
	if err := rt.PushSamples(context.Background(), samplesFor(t, 60)); err != nil {
		t.Fatal(err)
	}
	rt.Wait() // the third replica may still be landing after the quorum answer
	for _, f := range ings {
		if n := len(f.metrics.got); n != 60 {
			t.Errorf("%s holds %d of 60 samples, want all 60 at RF=3 on 3 members", f.url, n)
		}
	}
}

func TestRouterQuorumMetWithOneDown(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{1: http.StatusServiceUnavailable}, nil)
	var batches []string
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second,
		ObserveBatch: func(_ context.Context, b BatchReport) { batches = append(batches, b.Outcome) }})
	if err := rt.PushSamples(context.Background(), samplesFor(t, 60)); err != nil {
		t.Fatalf("one of three down at RF=3: %v, want success", err)
	}
	rt.Wait() // test hook: waits for background pushes and the batch observation
	if !slices.Equal(batches, []string{"degraded"}) {
		t.Errorf("batch outcomes = %v, want [degraded]", batches)
	}
}

func TestRouterBatchOutcomeFull(t *testing.T) {
	ings := startIngesters(t, 3, nil, nil)
	var batches []string
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second,
		ObserveBatch: func(_ context.Context, b BatchReport) { batches = append(batches, b.Outcome) }})
	if err := rt.PushSamples(context.Background(), samplesFor(t, 30)); err != nil {
		t.Fatal(err)
	}
	rt.Wait()
	if !slices.Equal(batches, []string{"full"}) {
		t.Errorf("batch outcomes = %v, want [full]", batches)
	}
}

func TestRouterQuorumMissedWithTwoDown(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusServiceUnavailable, 2: http.StatusServiceUnavailable}, nil)
	var batches []string
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second,
		ObserveBatch: func(_ context.Context, b BatchReport) { batches = append(batches, b.Outcome) }})
	err := rt.PushSamples(context.Background(), samplesFor(t, 10))
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) || qe.Quorum != 2 || qe.RF != 3 || qe.Kind != "series" || qe.Failed < 1 || qe.Total != 10 {
		t.Fatalf("err = %#v, want a QuorumError over ErrUnavailable", err)
	}
	if want := "write quorum not met: 10 of 10 series could not reach 2 of 3 ingesters"; err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
	}
	want := []string{MemberLabel(ings[0].url), MemberLabel(ings[2].url)}
	slices.Sort(want)
	if !slices.Equal(qe.Ingesters, want) {
		t.Errorf("Ingesters = %v, want the two failing members, sorted: %v", qe.Ingesters, want)
	}
	rt.Wait()
	if !slices.Equal(batches, []string{"failed"}) {
		t.Errorf("batch outcomes = %v, want [failed]", batches)
	}
}

func TestRouterProtocolErrorBreakingQuorumIsNotAnOutage(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusBadRequest, 2: http.StatusServiceUnavailable}, nil)
	rt, _ := newTestRouter(t, ings, 3, nil)
	err := rt.PushSamples(context.Background(), samplesFor(t, 10))
	var qe *QuorumError
	if !errors.As(err, &qe) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a QuorumError whose cause is a protocol error", err)
	}
}

func TestRouterAnswersAtQuorumWhileAReplicaHangs(t *testing.T) {
	ings := startIngesters(t, 3, nil, map[int]bool{2: true})
	var mu sync.Mutex
	outcomes := map[string]int{}
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 300 * time.Millisecond,
		ObserveMember: func(_, o string) { mu.Lock(); outcomes[o]++; mu.Unlock() }})
	start := time.Now()
	if err := rt.PushSamples(context.Background(), samplesFor(t, 20)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Errorf("answered after %v; quorum was met by the two healthy replicas long before the 300ms timeout", d)
	}
	rt.Wait()
	mu.Lock()
	defer mu.Unlock()
	if outcomes["ok"] != 2 || outcomes["unavailable"] != 1 {
		t.Errorf("member outcomes = %v, want 2 ok and the hung one unavailable after its timeout", outcomes)
	}
}

// Asking an ingester the router hasn't heard from whether it takes generations
// shares the push's one timeout: a hung unknown ingester costs one timeout, not
// one for the ask and another for the push.
func TestRouterAskAndPushToAnUnknownIngesterShareOneTimeout(t *testing.T) {
	ings := startIngesters(t, 1, nil, map[int]bool{0: true})
	const timeout = 300 * time.Millisecond
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 1, Timeout: timeout})
	start := time.Now()
	if err := rt.PushSamples(context.Background(), samplesFor(t, 3)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	rt.Wait()
	if d := time.Since(start); d > timeout*3/2 {
		t.Errorf("the write and its pushes took %v, want about one %v timeout", d, timeout)
	}
}

// A caller that gives up does not cancel the replicas still pushing: they
// finish in the background and are observed.
func TestRouterCallerCancelDoesNotCancelPushes(t *testing.T) {
	ings := startIngesters(t, 3, nil, map[int]bool{0: true, 1: true, 2: true})
	var mu sync.Mutex
	outcomes := map[string]int{}
	var batches []string
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 200 * time.Millisecond,
		ObserveMember: func(_, o string) { mu.Lock(); outcomes[o]++; mu.Unlock() },
		ObserveBatch:  func(_ context.Context, b BatchReport) { batches = append(batches, b.Outcome) }})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ctx, cancelNow := context.WithCancel(ctx)
	time.AfterFunc(20*time.Millisecond, cancelNow)
	err := rt.PushSamples(ctx, samplesFor(t, 5))
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	rt.Wait()
	mu.Lock()
	defer mu.Unlock()
	if outcomes["unavailable"] != 3 {
		t.Errorf("member outcomes = %v, want all three observed as unavailable after their own timeout", outcomes)
	}
	if !slices.Equal(batches, []string{"failed"}) {
		t.Errorf("batch outcomes = %v, want [failed]", batches)
	}
}

func TestRouterLogsReplicateWholeStreams(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusServiceUnavailable}, nil)
	rt, _ := newTestRouter(t, ings, 3, nil)
	s, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	var in []logs.PendingEntry
	for i := range 20 {
		in = append(in, logs.PendingEntry{Labels: s, TimestampNs: int64(i), Line: "l"})
	}
	if err := rt.PushEntries(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	rt.Wait()
	for _, f := range ings[1:] {
		if len(f.logs.got) != 20 {
			t.Errorf("%s holds %d of the stream's 20 lines", f.url, len(f.logs.got))
		}
	}
}

// With 5 ingesters at RF=3 and two down, only the keys whose replicas include
// both down members miss quorum; the others are not failed by them.
func TestRouterQuorumIsPerKey(t *testing.T) {
	ings := startIngesters(t, 5, map[int]int{0: http.StatusServiceUnavailable, 1: http.StatusServiceUnavailable}, nil)
	rt, r := newTestRouter(t, ings, 3, nil)
	var batch []metrics.PendingSample
	bothDown, clear := 0, 0
	for _, s := range samplesFor(t, 500) {
		reps, err := r.Replicas(s.Labels.Hash(), 3)
		if err != nil {
			t.Fatal(err)
		}
		down := 0
		for _, m := range reps {
			if m == ings[0].url || m == ings[1].url {
				down++
			}
		}
		switch {
		case down == 2 && bothDown < 10:
			bothDown++
			batch = append(batch, s)
		case down == 0 && clear < 10:
			clear++
			batch = append(batch, s)
		}
	}
	if bothDown == 0 || clear == 0 {
		t.Fatalf("sample set lacks a kind: %d keys on both down members, %d on neither", bothDown, clear)
	}
	err := rt.PushSamples(context.Background(), batch)
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a QuorumError over ErrUnavailable", err)
	}
	if qe.Failed != bothDown || qe.Total != len(batch) {
		t.Errorf("Failed/Total = %d/%d, want %d/%d: only keys on both down members miss quorum", qe.Failed, qe.Total, bothDown, len(batch))
	}
	want := []string{MemberLabel(ings[0].url), MemberLabel(ings[1].url)}
	slices.Sort(want)
	if !slices.Equal(qe.Ingesters, want) {
		t.Errorf("Ingesters = %v, want only the down members, sorted: %v", qe.Ingesters, want)
	}
}

// WaitContext gives up when its context ends, so a hung background push
// cannot hold a shutdown past its budget; Wait would block until the push's
// own timeout.
func TestRouterWaitContextHonoursItsContext(t *testing.T) {
	ings := startIngesters(t, 3, nil, map[int]bool{2: true})
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: time.Second})
	if err := rt.PushSamples(context.Background(), samplesFor(t, 20)); err != nil {
		t.Fatal(err) // quorum met by the two healthy replicas; the hung push runs on
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := rt.WaitContext(ctx)
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("WaitContext returned after %v, want about its 50ms context", d)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the context's deadline", err)
	}
	if err := rt.WaitContext(context.Background()); err != nil {
		t.Errorf("WaitContext with no deadline = %v, want nil once the push timed out", err)
	}
}

// Quorum impossible is answered at once, not after the replicas still
// running end (spec §5.2): with two of three replicas refused and the third
// hung, the write fails in milliseconds, not after the push timeout. The hung
// push still ends in the background, bounded, and is counted.
func TestRouterAnswersAtOnceWhenQuorumIsImpossible(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusServiceUnavailable, 1: http.StatusServiceUnavailable}, map[int]bool{2: true})
	var mu sync.Mutex
	var batches []string
	outcomes := map[string]int{}
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second,
		ObserveMember: func(_, o string) { mu.Lock(); outcomes[o]++; mu.Unlock() },
		ObserveBatch:  func(_ context.Context, b BatchReport) { mu.Lock(); batches = append(batches, b.Outcome); mu.Unlock() }})
	start := time.Now()
	err := rt.PushSamples(context.Background(), samplesFor(t, 10))
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) || qe.Failed != 10 {
		t.Fatalf("err = %v, want a QuorumError over ErrUnavailable for all 10 series", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("answered after %v; quorum was impossible once two replicas refused, long before the 2s push timeout", d)
	}
	rt.Wait()
	mu.Lock()
	defer mu.Unlock()
	if outcomes["unavailable"] != 3 || !slices.Equal(batches, []string{"failed"}) {
		t.Errorf("member outcomes %v, batch %v; want 3 unavailable (the hung one after its timeout) and [failed]", outcomes, batches)
	}
}

// A protocol error that breaks a key's quorum answers 500 even when the
// outage on the same key arrived first: the key fails at its second failure,
// and the answer is the worse of the two.
func TestRouterProtocolErrorWinsWithinAFailedKey(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusServiceUnavailable}, nil)
	slow := chi.NewRouter()
	slow.HandleFunc("/*", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond) // arrives after the outage
		w.WriteHeader(http.StatusBadRequest)
	})
	srv := httptest.NewServer(slow)
	t.Cleanup(srv.Close)
	ings[1].url = srv.URL
	rt, _ := newTestRouter(t, ings, 3, nil)
	err := rt.PushSamples(context.Background(), samplesFor(t, 5))
	var qe *QuorumError
	if !errors.As(err, &qe) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a QuorumError whose cause is the protocol error", err)
	}
}

// countingIngester counts the pushes it accepts, safely across goroutines.
func countingIngester(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		n.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &n
}

// After Close a write is refused as an outage, and nothing reaches an ingester.
func TestRouterRefusesBatchesAfterClose(t *testing.T) {
	u1, n1 := countingIngester(t)
	u2, n2 := countingIngester(t)
	u3, n3 := countingIngester(t)
	r, _ := ring.New([]string{u1, u2, u3})
	rt, err := NewRouter(r, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = rt.PushSamples(context.Background(), samplesFor(t, 5))
	if !errors.Is(err, ErrRouterClosed) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("push after Close: err = %v, want ErrRouterClosed (an outage)", err)
	}
	if got := n1.Load() + n2.Load() + n3.Load(); got != 0 {
		t.Errorf("%d pushes reached an ingester after Close", got)
	}
}

// Close racing writes: every write is either admitted before Close (and
// waited for) or refused, so nothing lands after Close returns, and a
// WaitGroup Add never races Close's Wait (run under -race).
func TestRouterCloseRacingWritesAdmitsNothingAfterward(t *testing.T) {
	u1, n1 := countingIngester(t)
	u2, n2 := countingIngester(t)
	u3, n3 := countingIngester(t)
	r, _ := ring.New([]string{u1, u2, u3})
	rt, err := NewRouter(r, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = rt.PushSamples(context.Background(), samplesFor(t, 3))
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	landed := n1.Load() + n2.Load() + n3.Load()
	time.Sleep(100 * time.Millisecond) // writers keep trying, and are refused
	close(stop)
	writers.Wait()
	if after := n1.Load() + n2.Load() + n3.Load(); after != landed {
		t.Errorf("%d pushes landed after Close returned", after-landed)
	}
}

// genRecorder records, per value, the generation each push carried, safely
// across goroutines. It takes generations (metrics.GenIngester), as an
// ingester's WALStore does.
type genRecorder struct {
	mu   sync.Mutex
	vals []float64
	gens []int64
}

func (g *genRecorder) Append(_ metrics.Labels, _ int64, v float64) error {
	return g.AppendWithGeneration(metrics.Labels{}, 0, v, 0)
}

func (g *genRecorder) AppendWithGeneration(_ metrics.Labels, _ int64, v float64, gen int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.vals = append(g.vals, v)
	g.gens = append(g.gens, gen)
	return nil
}

// newest is the value with the highest generation, as a read's dedup picks it.
func (g *genRecorder) newest() (float64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	best := -1
	for i := range g.vals {
		if best < 0 || g.gens[i] > g.gens[best] {
			best = i
		}
	}
	if best < 0 {
		return 0, false
	}
	return g.vals[best], true
}

// genIngester serves pushes into a genRecorder; with slowFirst, its first push
// carrying samples (not the gateway's empty probe) is held for that long
// before it is applied.
func genIngester(t *testing.T, slowFirst time.Duration) (string, *genRecorder) {
	t.Helper()
	m := &genRecorder{}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { MountWrites(r, m, &recordingLogs{}, observability.NewIngestMetrics()) })
	var first atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		if slowFirst > 0 && bytes.Contains(body, []byte(`"samples"`)) && first.CompareAndSwap(false, true) {
			time.Sleep(slowFirst)
		}
		r.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, m
}

// The gateway stamps each batch one generation at admission, never repeating
// one: max(previous + 1, now). One per batch keeps the counter at the clock.
func TestRouterStampsIncreasingGenerations(t *testing.T) {
	u, m := genIngester(t, 0)
	r, _ := ring.New([]string{u})
	var clock atomic.Int64
	clock.Store(1000)
	rt, err := NewRouter(r, RouterOptions{Clock: clock.Load, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Wait)
	rt.Wait() // the probe has heard the ingester takes generations
	l, _ := metrics.NewLabels(map[string]string{"__name__": "g"})
	in := []metrics.PendingSample{{Labels: l, TimestampMs: 1, Value: 1}, {Labels: l, TimestampMs: 2, Value: 2}}
	if err := rt.PushSamples(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	clock.Store(500) // a clock that stepped back
	if err := rt.PushSamples(context.Background(), in[:1]); err != nil {
		t.Fatal(err)
	}
	rt.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if want := []int64{1000, 1000, 1001}; !slices.Equal(m.gens, want) {
		t.Errorf("generations %v, want %v", m.gens, want)
	}
	if in[0].Gen != 0 {
		t.Error("PushSamples stamped the caller's slice")
	}
}

// An overwrite outranks the write it overwrites on every replica, even when
// the two go through different gateways and the older write's push reaches a
// slow replica last: each replica stores the generation the gateway stamped
// at admission, not one of its own at arrival. Two routers stand in for two
// gateway pods.
func TestOverwriteThroughTwoGatewaysWinsOnEveryReplica(t *testing.T) {
	ua, a := genIngester(t, 0)
	ub, b := genIngester(t, 0)
	uc, c := genIngester(t, 300*time.Millisecond) // v1's push lands here after v2's
	r, _ := ring.New([]string{ua, ub, uc})
	gw1, err := NewRouter(r, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	gw2, err := NewRouter(r, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gw1.Wait)
	t.Cleanup(gw2.Wait)
	gw1.Wait() // both probes heard every ingester takes generations
	gw2.Wait()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "over"})
	if err := gw1.PushSamples(context.Background(), []metrics.PendingSample{{Labels: l, TimestampMs: 1000, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := gw2.PushSamples(context.Background(), []metrics.PendingSample{{Labels: l, TimestampMs: 1000, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	gw1.Wait()
	gw2.Wait()
	c.mu.Lock()
	order := slices.Clone(c.vals)
	c.mu.Unlock()
	if !slices.Equal(order, []float64{2, 1}) {
		t.Fatalf("slow replica applied %v; the test needs v1 to land after v2 there", order)
	}
	for name, m := range map[string]*genRecorder{"a": a, "b": b, "c (v1 landed last)": c} {
		if v, ok := m.newest(); !ok || v != 2 {
			t.Errorf("replica %s: the highest generation holds %v, want the overwrite 2", name, v)
		}
	}
}

// A large batch does not push a gateway's counter ahead of its clock: the
// batch takes one generation, so a write admitted a microsecond later through
// another gateway still outranks it.
func TestALargeBatchDoesNotOutrankALaterWriteElsewhere(t *testing.T) {
	u, m := genIngester(t, 0)
	r, _ := ring.New([]string{u})
	gw1, err := NewRouter(r, RouterOptions{Clock: func() int64 { return 1000 }, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	gw2, err := NewRouter(r, RouterOptions{Clock: func() int64 { return 1001 }, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gw1.Wait)
	t.Cleanup(gw2.Wait)
	gw1.Wait()
	gw2.Wait()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "big"})
	big := make([]metrics.PendingSample, 10000)
	for i := range big {
		big[i] = metrics.PendingSample{Labels: l, TimestampMs: int64(i), Value: 1}
	}
	if err := gw1.PushSamples(context.Background(), big); err != nil {
		t.Fatal(err)
	}
	if err := gw2.PushSamples(context.Background(), []metrics.PendingSample{{Labels: l, TimestampMs: 0, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	gw1.Wait()
	gw2.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if last := m.gens[len(m.gens)-1]; last != 1001 || m.gens[0] != 1000 {
		t.Errorf("big batch stamped %d.., the later write %d; want 1000 and 1001", m.gens[0], last)
	}
}

// A batch repeating one series and timestamp keeps only its last sample: the
// batch shares a generation, so an earlier duplicate could not lose otherwise.
func TestRouterKeepsTheLastDuplicateInABatch(t *testing.T) {
	u, m := genIngester(t, 0)
	r, _ := ring.New([]string{u})
	rt, err := NewRouter(r, RouterOptions{ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Wait)
	rt.Wait()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "dup"})
	in := []metrics.PendingSample{{Labels: l, TimestampMs: 5, Value: 1}, {Labels: l, TimestampMs: 6, Value: 7}, {Labels: l, TimestampMs: 5, Value: 2}}
	if err := rt.PushSamples(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	rt.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !slices.Equal(m.vals, []float64{7, 2}) {
		t.Errorf("applied %v, want [7 2]: the earlier duplicate at ts 5 dropped", m.vals)
	}
}

// legacyIngester answers like an ingester from before 6.3: it refuses a push
// sample carrying a third element (400) and never says it takes generations.
func legacyIngester(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	var refused atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Series []struct {
				Samples [][]json.RawMessage `json:"samples"`
			} `json:"series"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		for _, s := range body.Series {
			for _, smp := range s.Samples {
				if len(smp) != 2 {
					refused.Add(1)
					http.Error(w, "push sample must be [timestamp, value]", http.StatusBadRequest)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &refused
}

// A rolling upgrade: a new gateway before two of three ingesters are new. It
// sends generations only to the ingester that says it takes them, so the old
// ones never refuse a sample and the write meets quorum.
func TestNewGatewayOverOldIngestersMeetsQuorum(t *testing.T) {
	o1, r1 := legacyIngester(t)
	o2, r2 := legacyIngester(t)
	n, m := genIngester(t, 0)
	r, _ := ring.New([]string{o1, o2, n})
	rt, err := NewRouter(r, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second, ProbeGenerations: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Wait)
	rt.Wait()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "upgrade"})
	for i := range 3 {
		if err := rt.PushSamples(context.Background(), []metrics.PendingSample{{Labels: l, TimestampMs: int64(i), Value: 1}}); err != nil {
			t.Fatalf("write %d during the upgrade: %v", i, err)
		}
	}
	rt.Wait()
	if r1.Load()+r2.Load() != 0 {
		t.Errorf("old ingesters refused %d samples: the gateway sent them generations", r1.Load()+r2.Load())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.gens {
		if g == 0 {
			t.Errorf("the new ingester got a sample without the gateway's generation: %v", m.gens)
			break
		}
	}
}
