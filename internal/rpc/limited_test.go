package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientClassifies429AsLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusTooManyRequests, "tenant b would exceed its active series limit (2)")
	}))
	defer srv.Close()
	c, err := NewClient("ingester", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.PushSamples(context.Background(), samplesFor(t, 1))
	if !errors.Is(err, ErrLimited) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrLimited and not ErrUnavailable", err)
	}
}

func TestWorstRanksLimited(t *testing.T) {
	limited := fmt.Errorf("%w: x", ErrLimited)
	unavail := fmt.Errorf("%w: y", ErrUnavailable)
	protocol := errors.New("bad")
	if got := worst([]error{unavail, limited, context.Canceled}); !errors.Is(got, ErrLimited) {
		t.Fatalf("limited vs unavailable: %v", got)
	}
	got := worst([]error{limited, protocol})
	if errors.Is(got, ErrLimited) || !errors.Is(got, protocol) {
		t.Fatalf("protocol vs limited: %v", got)
	}
	if OutcomeOf(limited) != "limited" {
		t.Fatalf("outcome = %q", OutcomeOf(limited))
	}
}

func TestStoreCauseRanksLimited(t *testing.T) {
	limited := fmt.Errorf("%w: x", ErrLimited)
	unavail := fmt.Errorf("%w: y", ErrUnavailable)
	protocol := errors.New("bad")
	if got := storeCause([]error{unavail, context.Canceled, limited}); !errors.Is(got, ErrLimited) {
		t.Fatalf("limited over cancel: %v", got)
	}
	if got := storeCause([]error{limited, protocol}); got != protocol {
		t.Fatalf("protocol over limited: %v", got)
	}
	if got := storeCause([]error{unavail, context.Canceled}); !errors.Is(got, context.Canceled) {
		t.Fatalf("cancel over outage: %v", got)
	}
}

func TestRouterQuorumErrorIsLimited(t *testing.T) {
	ings := startIngesters(t, 3, map[int]int{0: http.StatusTooManyRequests, 1: http.StatusTooManyRequests}, nil)
	rt, _ := newTestRouter(t, ings, 3, nil)
	err := rt.PushSamples(context.Background(), samplesFor(t, 5))
	var qe *QuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrLimited) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want QuorumError wrapping ErrLimited", err)
	}
}
