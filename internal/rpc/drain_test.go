package rpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func drainServer(t *testing.T, drain func(context.Context) error, timeout time.Duration) string {
	t.Helper()
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { MountDrain(r, drain, timeout) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv.URL + "/internal/v1/drain"
}

func postDrain(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	b.Write(buf[:n])
	return resp.StatusCode, b.String()
}

// The drain route is the acknowledgment a removal waits for: 200 only when
// everything reached the store, 503 with the reason otherwise.
func TestDrainRouteAnswers200OnlyOnSuccess(t *testing.T) {
	if code, body := postDrain(t, drainServer(t, func(context.Context) error { return nil }, time.Second)); code != http.StatusOK || !strings.Contains(body, `"drained":true`) {
		t.Errorf("successful drain = %d %s, want 200 {\"drained\":true}", code, body)
	}
	code, body := postDrain(t, drainServer(t, func(context.Context) error { return errors.New("store unreachable") }, time.Second))
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "store unreachable") {
		t.Errorf("failed drain = %d %s, want 503 naming the cause", code, body)
	}
}

func TestDrainRouteIsBoundedByItsTimeout(t *testing.T) {
	url := drainServer(t, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, 50*time.Millisecond)
	start := time.Now()
	if code, _ := postDrain(t, url); code != http.StatusServiceUnavailable {
		t.Errorf("drain past its timeout = %d, want 503", code)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("drain took %v; the timeout did not bound it", time.Since(start))
	}
}
