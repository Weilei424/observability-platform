package rpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
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
		{"protocol error beats unavailable", map[int]int{0: http.StatusBadRequest, 1: http.StatusServiceUnavailable},
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
		ObserveBatch: func(o string) { batches = append(batches, o) }})
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
		ObserveBatch: func(o string) { batches = append(batches, o) }})
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
		ObserveBatch: func(o string) { batches = append(batches, o) }})
	err := rt.PushSamples(context.Background(), samplesFor(t, 10))
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) || qe.Quorum != 2 || qe.RF != 3 || qe.Kind != "series" || qe.Failed < 1 || qe.Total != 10 {
		t.Fatalf("err = %#v, want a QuorumError over ErrUnavailable", err)
	}
	if want := "write quorum not met: 10 of 10 series could not reach 2 of 3 ingesters"; err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
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

// A caller that gives up does not cancel the replicas still pushing: they
// finish in the background and are observed.
func TestRouterCallerCancelDoesNotCancelPushes(t *testing.T) {
	ings := startIngesters(t, 3, nil, map[int]bool{0: true, 1: true, 2: true})
	var mu sync.Mutex
	outcomes := map[string]int{}
	var batches []string
	rt, _ := newTestRouterOpts(t, ings, RouterOptions{ReplicationFactor: 3, Timeout: 200 * time.Millisecond,
		ObserveMember: func(_, o string) { mu.Lock(); outcomes[o]++; mu.Unlock() },
		ObserveBatch:  func(o string) { batches = append(batches, o) }})
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
}
