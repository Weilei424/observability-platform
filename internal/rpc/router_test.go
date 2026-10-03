package rpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
// makes that server answer every push with it.
func startIngesters(t *testing.T, n int, status map[int]int) []*fakeIngester {
	t.Helper()
	out := make([]*fakeIngester, n)
	for i := range out {
		f := &fakeIngester{metrics: &recordingMetrics{}, logs: &recordingLogs{}}
		r := chi.NewRouter()
		if code := status[i]; code != 0 {
			r.HandleFunc("/*", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
		} else {
			r.Route("/internal/v1", func(r chi.Router) { MountWrites(r, f.metrics, f.logs, observability.NewIngestMetrics()) })
		}
		f.srv = httptest.NewServer(r)
		t.Cleanup(f.srv.Close)
		f.url = f.srv.URL
		out[i] = f
	}
	return out
}

func newTestRouter(t *testing.T, ings []*fakeIngester, observe func(string, string)) (*Router, *ring.Ring) {
	t.Helper()
	urls := make([]string, len(ings))
	for i, f := range ings {
		urls[i] = f.url
	}
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRouter(r, observe)
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
	ings := startIngesters(t, 3, nil)
	rt, r := newTestRouter(t, ings, nil)
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
	ings := startIngesters(t, 3, nil)
	rt, _ := newTestRouter(t, ings, nil)
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
			ings := startIngesters(t, 3, tc.status)
			var mu sync.Mutex
			outcomes := map[string]int{}
			rt, _ := newTestRouter(t, ings, func(_, outcome string) { mu.Lock(); outcomes[outcome]++; mu.Unlock() })
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
	ings := startIngesters(t, 3, map[int]int{2: http.StatusServiceUnavailable})
	rt, r := newTestRouter(t, ings, nil)
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
	ings := startIngesters(t, 3, nil)
	rt, _ := newTestRouter(t, ings, nil)
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
	ings := startIngesters(t, 3, nil)
	rt, _ := newTestRouter(t, ings, nil)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := rt.PushSamples(ctx, samplesFor(t, 30))
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrUnavailable wrapping DeadlineExceeded", err)
	}
}
