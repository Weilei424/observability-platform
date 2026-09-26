package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

var (
	_ metrics.Source         = (*rpc.MetricsSource)(nil)
	_ logs.Source            = (*rpc.LogsSource)(nil)
	_ metrics.BlockSink      = (*rpc.BlockSink)(nil)
	_ logs.ChunkSink         = (*rpc.ChunkSink)(nil)
	_ compactor.BlockManager = (*rpc.BlockManager)(nil)
)

func client(t *testing.T, url string) *rpc.Client {
	t.Helper()
	c, err := rpc.NewClient("store", url)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientsRoundTripAgainstTheRealRoutes(t *testing.T) {
	f := newStoreFixture(t)
	c := client(t, f.srv.URL)

	// Flush through the sink client, then read back through the source client.
	l, _ := metrics.NewLabels(map[string]string{"__name__": "rt"})
	head := metrics.NewMemoryStore()
	for i := range 120 {
		if err := head.Append(l, int64(i)*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := rpc.NewBlockSink(c).IngestSeriesChunks(context.Background(), head.SealedChunksSnapshot())
	if err != nil || meta.NumSamples != 120 {
		t.Fatalf("IngestSeriesChunks = %+v, %v", meta, err)
	}
	got, err := rpc.NewMetricsSource(c).Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: "rt"}, MinT: 0, MaxT: 119_000,
	})
	if err != nil || len(got) != 1 || len(got[0].Samples) != 120 {
		t.Fatalf("Select = %v, %v", got, err)
	}
	names, err := rpc.NewMetricsSource(c).SelectLabelNames(context.Background())
	if err != nil || len(names) != 1 || names[0] != "__name__" {
		t.Fatalf("SelectLabelNames = %v, %v", names, err)
	}

	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	if err := rpc.NewChunkSink(c).IngestStreams(context.Background(), []logs.StreamData{{
		Labels: sl, Entries: []logs.LogEntry{{TimestampNs: 1, Line: "a"}},
	}}); err != nil {
		t.Fatal(err)
	}
	streams, err := rpc.NewLogsSource(c).SelectStreams(context.Background(), nil, 0, 10)
	if err != nil || len(streams) != 1 || streams[0].Entries[0].Line != "a" {
		t.Fatalf("SelectStreams = %v, %v", streams, err)
	}

	n, err := rpc.NewBlockManager(c).ApplyRetention(time.UnixMilli(10_000_000_000_000), time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("ApplyRetention = %d, %v", n, err)
	}
}

func TestClientClassifiesFailures(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	if _, err := rpc.NewMetricsSource(client(t, deadURL)).SelectLabelNames(context.Background()); !errors.Is(err, rpc.ErrUnavailable) {
		t.Errorf("refused connection = %v, want ErrUnavailable", err)
	}

	fiveHundred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"disk on fire"}`, http.StatusInternalServerError)
	}))
	t.Cleanup(fiveHundred.Close)
	if _, err := rpc.NewMetricsSource(client(t, fiveHundred.URL)).SelectLabelNames(context.Background()); !errors.Is(err, rpc.ErrUnavailable) {
		t.Errorf("5xx = %v, want ErrUnavailable", err)
	}

	fourHundred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"bad"}`, http.StatusBadRequest)
	}))
	t.Cleanup(fourHundred.Close)
	_, err := rpc.NewMetricsSource(client(t, fourHundred.URL)).SelectLabelNames(context.Background())
	if err == nil || errors.Is(err, rpc.ErrUnavailable) {
		t.Errorf("4xx = %v, want a non-unavailable error: it is a protocol disagreement, not an outage", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rpc.NewMetricsSource(client(t, fourHundred.URL)).SelectLabelNames(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled = %v, want context.Canceled, not an outage", err)
	}
}

func TestClientForwardsOrMintsARequestID(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("X-Request-Id"))
		_, _ = w.Write([]byte(`{"names":[]}`))
	}))
	t.Cleanup(srv.Close)
	src := rpc.NewMetricsSource(client(t, srv.URL))
	ctx := context.WithValue(context.Background(), chimiddleware.RequestIDKey, "req-123")
	_, _ = src.SelectLabelNames(ctx)
	_, _ = src.SelectLabelNames(context.Background())
	if len(seen) != 2 || seen[0] != "req-123" || seen[1] == "" {
		t.Fatalf("request IDs seen = %q, want the inbound one forwarded, then a minted one", seen)
	}
}

func TestBlockManagerPlansLocallyAndKeepsPartialCounts(t *testing.T) {
	var planned []block.BlockInfo
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/v1/metrics/blocks":
			_, _ = w.Write([]byte(`{"blocks":[{"id":"a","level":1,"min_time":0,"max_time":10,"size_bytes":5}]}`))
		case "/internal/v1/metrics/compact":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"compacted":1,"error":"second group failed"}`))
		}
	}))
	t.Cleanup(srv.Close)
	bm := rpc.NewBlockManager(client(t, srv.URL))
	n, err := bm.CompactOnce(func(infos []block.BlockInfo) [][]string {
		planned = infos
		return [][]string{{"a", "b"}}
	})
	if len(planned) != 1 || planned[0].ID != "a" || planned[0].SizeBytes != 5 {
		t.Fatalf("plan saw %+v, want the listed block", planned)
	}
	if n != 1 || err == nil {
		t.Fatalf("CompactOnce = %d, %v; want the partial count 1 and the error", n, err)
	}
}

// TestChunkSinkWritesLinesWithoutHTMLEscaping pins the controller ruling: every
// request body must go through the codec's marshalJSON, never json.Marshal.
// The ingester sizes a log-flush batch assuming HTML escaping is off; a line
// containing '<', '>', '&' must reach the wire with those bytes literal, not
// expanded into six-byte \u00XX escapes each (which json.Marshal would do).
func TestChunkSinkWritesLinesWithoutHTMLEscaping(t *testing.T) {
	var raw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
		_, _ = w.Write([]byte(`{"streams":1,"entries":1}`))
	}))
	t.Cleanup(srv.Close)

	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	err := rpc.NewChunkSink(client(t, srv.URL)).IngestStreams(context.Background(), []logs.StreamData{{
		Labels: sl, Entries: []logs.LogEntry{{TimestampNs: 1, Line: "<>&"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, "<>&") {
		t.Fatalf("request body = %s, want the line's <>& literal on the wire", raw)
	}
	// Compare against whatever encoding/json's own default HTML escaping would
	// produce for the same string, rather than hardcoding the escape sequence.
	badBytes, err := json.Marshal("<>&")
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Trim(string(badBytes), `"`)
	if strings.Contains(raw, bad) {
		t.Fatalf("request body = %s, contains %s: was HTML-escaped by json.Marshal instead of marshalJSON", raw, bad)
	}
}

// TestClientContextCancellationAbortsInFlightRequest pins the controller
// ruling: the ingester's per-request flush timeout relies on the client
// building every request with the caller's context. This checks cancellation
// actually aborts a request already in flight against a slow peer, not merely
// a request whose context was already cancelled before it started (that case
// is covered above by TestClientClassifiesFailures).
//
// The source is built before the goroutine starts: client(t, ...) may call
// t.Fatal, which must run on the test's own goroutine, not one this test
// spawns. An arrived channel — closed once the handler is actually running —
// replaces a fixed sleep, so the test doesn't guess how long "in flight" takes.
func TestClientContextCancellationAbortsInFlightRequest(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release // hang until the test releases it, simulating a slow peer
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	src := rpc.NewMetricsSource(client(t, srv.URL))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := src.SelectLabelNames(ctx)
		errCh <- err
	}()

	<-arrived // the request has reached the server and is blocked there
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not abort after its context was cancelled")
	}
}

// TestClientContextDeadlineIsUnavailable pins the fix-round-1 ruling: unlike a
// cancellation, a deadline means the peer failed to answer in time — the same
// as any other timeout — so it must classify as ErrUnavailable (in addition to
// still satisfying errors.Is against the deadline itself).
func TestClientContextDeadlineIsUnavailable(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hang past the deadline, simulating an unresponsive peer
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := rpc.NewMetricsSource(client(t, srv.URL)).SelectLabelNames(ctx)
	if !errors.Is(err, rpc.ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline = %v, want both ErrUnavailable and context.DeadlineExceeded", err)
	}
}

// TestClientTreatsATruncated200AsUnavailable pins the fix-round-1 ruling that a
// 2xx whose body is cut short mid-transfer is a transport failure like any
// other, not a decode error to surface as a protocol disagreement: the peer
// promises more bytes than it (or the connection) ever delivers.
func TestClientTreatsATruncated200AsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"names":`)) // far short of the promised length
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(srv.Close)

	_, err := rpc.NewMetricsSource(client(t, srv.URL)).SelectLabelNames(context.Background())
	if !errors.Is(err, rpc.ErrUnavailable) {
		t.Errorf("truncated 2xx = %v, want ErrUnavailable", err)
	}
}

// TestClientFiveHundredErrorNamesPeerAndMessage pins the fix-round-1 ruling
// that a 5xx's error keeps enough for an operator to act on it: which peer
// answered, and the peer's own error text, not just a bare classification.
func TestClientFiveHundredErrorNamesPeerAndMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"disk on fire"}`, http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	_, err := rpc.NewMetricsSource(client(t, srv.URL)).SelectLabelNames(context.Background())
	if err == nil || !strings.Contains(err.Error(), "store") || !strings.Contains(err.Error(), "disk on fire") {
		t.Fatalf("5xx error = %v, want it to name the peer (%q) and carry the peer's message (%q)", err, "store", "disk on fire")
	}
}

// TestClientJoinsPeerURLWithoutDoubleSlash pins the controller ruling: peer
// URLs arrive from config with any trailing "/" already removed, but the join
// onto /internal/v1/... must never produce a double slash whichever way it
// receives a base — tested here with no trailing slash (the guaranteed case)
// and defensively with one and two trailing slashes.
func TestClientJoinsPeerURLWithoutDoubleSlash(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"names":[]}`))
	}))
	t.Cleanup(srv.Close)

	for _, base := range []string{srv.URL, srv.URL + "/", srv.URL + "//"} {
		gotPath = ""
		if _, err := rpc.NewMetricsSource(client(t, base)).SelectLabelNames(context.Background()); err != nil {
			t.Fatalf("base %q: %v", base, err)
		}
		if want := "/internal/v1/metrics/labels"; gotPath != want || strings.Contains(gotPath, "//") {
			t.Errorf("base %q -> path %q, want %q with no double slash", base, gotPath, want)
		}
	}
}
