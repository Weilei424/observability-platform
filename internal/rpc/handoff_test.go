package rpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

const leavingSelf = "http://leaving-store:8080"

type handoffStore struct {
	srv     *httptest.Server
	blocks  *metrics.BlockStore
	chunks  *logs.ChunkStore
	barrier *rpc.FlushBarrier
	handler http.Handler
}

// newHandoffStore is a real store (block store and chunk store in a temp dir)
// serving reads, flush-in, and the handoff, with wrap (when non-nil) in front.
func newHandoffStore(t *testing.T, self string, wrap func(http.Handler) http.Handler) handoffStore {
	t.Helper()
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	cs, err := logs.OpenChunkStore(filepath.Join(dir, "logs", "chunks"), filepath.Join(dir, "logs", "index"))
	if err != nil {
		t.Fatal(err)
	}
	barrier := &rpc.FlushBarrier{}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountReads(r, bs, cs)
		rpc.MountStore(r, bs, cs, barrier)
		rpc.MountHandoff(r, bs, cs, barrier, self, metrics.DefaultFlushTimeout)
	})
	var h http.Handler = r
	if wrap != nil {
		h = wrap(r)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return handoffStore{srv: srv, blocks: bs, chunks: cs, barrier: barrier, handler: h}
}

// fillLeaving flushes two series (in two blocks, one shared) and one log stream
// into s.
func fillLeaving(t *testing.T, s handoffStore) {
	t.Helper()
	for _, body := range []string{
		fmt.Sprintf(`{"series":[{"labels":{"__name__":"a"},"chunks":[%q]}]}`, sealedChunkBytes(t, 0)),
		fmt.Sprintf(`{"series":[{"labels":{"__name__":"a"},"chunks":[%q]},{"labels":{"__name__":"b"},"chunks":[%q]}]}`,
			sealedChunkBytes(t, 200_000), sealedChunkBytes(t, 0)),
	} {
		if resp, out := post(t, s.srv.URL+"/internal/v1/metrics/flush", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("metrics flush: %d %s", resp.StatusCode, out)
		}
	}
	logsBody := `{"streams":[{"labels":{"service":"api"},"entries":[[1000,"one"],[2000,"two <&> three"]]}]}`
	if resp, out := post(t, s.srv.URL+"/internal/v1/logs/flush", logsBody); resp.StatusCode != http.StatusOK {
		t.Fatalf("logs flush: %d %s", resp.StatusCode, out)
	}
}

type handoffAnswer struct {
	Series  int    `json:"series"`
	Streams int    `json:"streams"`
	Error   string `json:"error"`
}

func handoff(t *testing.T, s handoffStore, body string) (int, handoffAnswer, string) {
	t.Helper()
	resp, out := post(t, s.srv.URL+"/internal/v1/handoff", body)
	var a handoffAnswer
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusServiceUnavailable {
		if err := json.Unmarshal([]byte(out), &a); err != nil {
			t.Fatalf("handoff answer %q: %v", out, err)
		}
	}
	return resp.StatusCode, a, out
}

func handoffBody(rf int, targets ...handoffStore) string {
	urls := make([]string, len(targets))
	for i, s := range targets {
		urls[i] = fmt.Sprintf("%q", s.srv.URL)
	}
	return fmt.Sprintf(`{"stores":[%s],"replication_factor":%d}`, strings.Join(urls, ","), rf)
}

func checkTargetHolds(t *testing.T, s handoffStore) {
	t.Helper()
	for name, want := range map[string]int{"a": 240, "b": 120} {
		l, _ := metrics.NewLabels(map[string]string{"__name__": name})
		got, err := s.blocks.QueryRange(metrics.SeriesID(l.Hash()), 0, math.MaxInt64)
		if err != nil || len(got) != want {
			t.Errorf("target %s series %s: %d samples (%v), want %d", s.srv.URL, name, len(got), err, want)
		}
	}
	l, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	entries, err := s.chunks.StreamEntries(context.Background(), logs.StreamIDOf(l), math.MinInt64, math.MaxInt64)
	if err != nil || len(entries) != 2 || entries[0].Line != "one" || entries[1].Line != "two <&> three" {
		t.Errorf("target %s stream: %+v (%v), want the two entries", s.srv.URL, entries, err)
	}
}

func flushStatus(t *testing.T, s handoffStore) (int, string) {
	t.Helper()
	body := fmt.Sprintf(`{"series":[{"labels":{"__name__":"late"},"chunks":[%q]}]}`, sealedChunkBytes(t, 0))
	resp, out := post(t, s.srv.URL+"/internal/v1/metrics/flush", body)
	return resp.StatusCode, out
}

func TestHandoffCopiesEverythingToEveryOwner(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	fillLeaving(t, leaving)
	targets := []handoffStore{newHandoffStore(t, "", nil), newHandoffStore(t, "", nil), newHandoffStore(t, "", nil)}
	code, a, out := handoff(t, leaving, handoffBody(3, targets...))
	if code != http.StatusOK || a.Series != 2 || a.Streams != 1 || a.Error != "" {
		t.Fatalf("handoff = %d %s, want 200 with series 2, streams 1", code, out)
	}
	for _, s := range targets {
		checkTargetHolds(t, s)
	}
}

func TestHandoffClosesTheFlushBarrier(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	fillLeaving(t, leaving)
	target := newHandoffStore(t, "", nil)
	if code, _, out := handoff(t, leaving, handoffBody(1, target)); code != http.StatusOK {
		t.Fatalf("handoff = %d %s", code, out)
	}
	if !leaving.barrier.Closed() {
		t.Error("barrier open after a handoff")
	}
	for _, path := range []string{"metrics/flush", "logs/flush"} {
		resp, out := post(t, leaving.srv.URL+"/internal/v1/"+path, `{}`)
		if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(out, "store is handing off: it takes no new flushes") {
			t.Errorf("%s after handoff = %d %s, want 503 with the handoff message", path, resp.StatusCode, out)
		}
	}
	// A second handoff (a retry) still exports everything.
	if code, a, out := handoff(t, leaving, handoffBody(1, target)); code != http.StatusOK || a.Series != 2 || a.Streams != 1 {
		t.Fatalf("second handoff = %d %s, want 200 with series 2, streams 1", code, out)
	}
}

func TestHandoffOfAnEmptyStore(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	target := newHandoffStore(t, "", nil)
	code, _, out := handoff(t, leaving, handoffBody(1, target))
	if code != http.StatusOK || strings.TrimSpace(out) != `{"series":0,"streams":0}` {
		t.Fatalf("empty handoff = %d %s, want 200 {\"series\":0,\"streams\":0}", code, out)
	}
}

// Review Focus 3: a refused body answers 400 and leaves the barrier open.
func TestHandoffRefusesBadBodiesBeforeTheBarrier(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	target := newHandoffStore(t, "", nil)
	for name, body := range map[string]string{
		"no stores":           `{"stores":[],"replication_factor":1}`,
		"stores missing":      `{"replication_factor":1}`,
		"rf above stores":     `{"stores":["http://a:1"],"replication_factor":2}`,
		"rf zero":             `{"stores":["http://a:1"],"replication_factor":0}`,
		"rf missing":          `{"stores":["http://a:1"]}`,
		"self":                `{"stores":["http://a:1","HTTP://Leaving-Store.:8080/"],"replication_factor":1}`,
		"duplicate":           `{"stores":["http://a:1","http://A:1/"],"replication_factor":1}`,
		"bad url":             `{"stores":["ftp://a:1"],"replication_factor":1}`,
		"url with path":       `{"stores":["http://a:1/x"],"replication_factor":1}`,
		"unknown field":       `{"stores":["http://a:1"],"replication_factor":1,"x":1}`,
		"not json":            `nope`,
		"credentials in url":  `{"stores":["http://u:s3cret@a:1"],"replication_factor":1}`,
		"target is not valid": fmt.Sprintf(`{"stores":[%q,""],"replication_factor":1}`, target.srv.URL),
	} {
		resp, out := post(t, leaving.srv.URL+"/internal/v1/handoff", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d %s, want 400", name, resp.StatusCode, out)
		}
		if strings.Contains(out, "s3cret") {
			t.Errorf("%s: answer leaks credentials: %s", name, out)
		}
		if leaving.barrier.Closed() {
			t.Fatalf("%s: barrier closed by a refused handoff", name)
		}
		if code, out := flushStatus(t, leaving); code != http.StatusOK {
			t.Fatalf("%s: flush after the 400 = %d %s, want 200", name, code, out)
		}
	}
}

// failFirst answers 503 to the first n requests whose path ends in suffix.
func failFirst(n int32, suffix string) func(http.Handler) http.Handler {
	var seen atomic.Int32
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, suffix) && seen.Add(1) <= n {
				http.Error(w, `{"error":"not yet"}`, http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// One owner down at RF 3 fails the handoff with the counts sent so far; once
// it is back, a retry succeeds and every owner holds everything.
func TestHandoffFailsWhileAnOwnerIsDownAndARetrySucceeds(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	fillLeaving(t, leaving)
	targets := []handoffStore{newHandoffStore(t, "", nil), newHandoffStore(t, "", failFirst(1, "/logs/flush")), newHandoffStore(t, "", nil)}
	code, a, out := handoff(t, leaving, handoffBody(3, targets...))
	if code != http.StatusServiceUnavailable || a.Series != 2 || a.Streams != 0 || a.Error == "" {
		t.Fatalf("handoff with an owner down = %d %s, want 503 with series 2, streams 0, and the error", code, out)
	}
	code, a, out = handoff(t, leaving, handoffBody(3, targets...))
	if code != http.StatusOK || a.Series != 2 || a.Streams != 1 {
		t.Fatalf("retry = %d %s, want 200 with series 2, streams 1", code, out)
	}
	for _, s := range targets {
		checkTargetHolds(t, s)
	}
}

// The first metrics batch failing reports nothing sent.
func TestHandoffFailingFirstBatchReportsZero(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	fillLeaving(t, leaving)
	targets := []handoffStore{newHandoffStore(t, "", failFirst(100, "/metrics/flush")), newHandoffStore(t, "", nil)}
	code, a, out := handoff(t, leaving, handoffBody(2, targets...))
	if code != http.StatusServiceUnavailable || a.Series != 0 || a.Streams != 0 || a.Error == "" {
		t.Fatalf("handoff = %d %s, want 503 with zero counts and the error", code, out)
	}
}

// A caller that has gone away cancels the handoff; it must never answer
// success for a copy it did not finish.
func TestHandoffStopsWhenTheCallerCancels(t *testing.T) {
	leaving := newHandoffStore(t, leavingSelf, nil)
	fillLeaving(t, leaving)
	target := newHandoffStore(t, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/handoff", strings.NewReader(handoffBody(1, target))).WithContext(ctx)
	rec := httptest.NewRecorder()
	leaving.handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("cancelled handoff answered 200 %s", rec.Body.String())
	}
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "canceled") {
		t.Errorf("cancelled handoff = %d %s, want 503 naming the cancellation", rec.Code, rec.Body.String())
	}
}
