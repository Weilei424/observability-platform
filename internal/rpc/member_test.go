package rpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

func TestRequireMemberRefusesAnotherMembersRequest(t *testing.T) {
	h := RequireMember("http://ingester-1:8080")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, tc := range []struct {
		member string
		want   int
	}{
		{"http://ingester-1:8080", http.StatusNoContent},
		{"", http.StatusNoContent}, // no header: an older client, or curl
		{"http://ingester-2:8080", http.StatusMisdirectedRequest},
	} {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/metrics/push", nil)
		if tc.member != "" {
			req.Header.Set(MemberHeader, tc.member)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("member %q: status %d, want %d", tc.member, rec.Code, tc.want)
		}
		if tc.want == http.StatusMisdirectedRequest && !strings.Contains(rec.Body.String(), "this ingester is http://ingester-1:8080, not http://ingester-2:8080") {
			t.Errorf("421 body = %s", rec.Body.String())
		}
	}
}

// A misdirected answer is an outage for the member the client meant, and the
// client drops its pooled connection so the next request dials afresh.
func TestClientTreatsMisdirectedAsUnavailableAndRedials(t *testing.T) {
	var dials atomic.Int32
	srv := httptest.NewUnstartedServer(RequireMember("http://someone-else:1")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			dials.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	c, err := NewClient("ingester", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := c.PushSamplesGen(context.Background(), nil, false); !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "421") {
			t.Fatalf("push %d: err = %v, want ErrUnavailable over a 421", i, err)
		}
	}
	if n := dials.Load(); n != 2 {
		t.Errorf("connections dialed = %d, want 2: the misdirected one must not be reused", n)
	}
}

// Two ring members whose names reach the same ingester -- a connection held
// past a container restart that reshuffled addresses -- must not count that
// ingester's one acknowledgement twice toward quorum.
func TestRouterDoesNotCountOneIngesterTwice(t *testing.T) {
	f := &fakeIngester{metrics: &recordingMetrics{}, logs: &recordingLogs{}}
	r := chi.NewRouter()
	var self string
	r.Route("/internal/v1", func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { RequireMember(self)(next).ServeHTTP(w, req) })
		})
		MountWrites(r, f.metrics, f.logs, observability.NewIngestMetrics())
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	self = srv.URL
	// "localhost" dials the same listener as srv.URL's 127.0.0.1 but is another
	// member's name: the misrouted member.
	misrouted := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)
	down := startIngesters(t, 1, map[int]int{0: http.StatusServiceUnavailable}, nil)[0]
	rg, err := ring.New([]string{srv.URL, misrouted, down.url})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRouter(rg, RouterOptions{ReplicationFactor: 3, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Wait)
	err = rt.PushSamples(context.Background(), samplesFor(t, 5))
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a QuorumError over ErrUnavailable: one live ingester is not two replicas", err)
	}
	rt.Wait()
	if got := len(f.metrics.got); got != 5 {
		t.Errorf("the live ingester stored %d samples, want 5 (its own push only)", got)
	}
}
