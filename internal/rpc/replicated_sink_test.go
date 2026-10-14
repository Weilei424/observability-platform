package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// fakeStore answers every flush with status (200 when 0), recording bodies.
type fakeStore struct {
	url    string
	mu     sync.Mutex
	bodies []string
}

func startStores(t *testing.T, n int, status map[int]int) []*fakeStore {
	t.Helper()
	out := make([]*fakeStore, n)
	for i := range out {
		f := &fakeStore{}
		code := status[i]
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.bodies = append(f.bodies, string(b))
			f.mu.Unlock()
			if code != 0 {
				w.WriteHeader(code)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "metrics/flush") {
				_, _ = io.WriteString(w, `{"block_id":"b","series":1,"samples":1}`)
			} else {
				_, _ = io.WriteString(w, `{"streams":1,"entries":1}`)
			}
		}))
		t.Cleanup(srv.Close)
		f.url = srv.URL
		out[i] = f
	}
	return out
}

type recordQueue struct {
	mu    sync.Mutex
	items []string // store URLs queued
}

func (q *recordQueue) Add(store, _ string, _ []byte) {
	q.mu.Lock()
	q.items = append(q.items, store)
	q.mu.Unlock()
}

func seriesBatch(t *testing.T, n int) []metrics.SeriesChunks {
	t.Helper()
	out := make([]metrics.SeriesChunks, n)
	for i := range out {
		l, _ := metrics.NewLabels(map[string]string{"__name__": "m", "i": strings.Repeat("x", i+1)})
		c := chunk.NewChunk()
		if err := c.Append(1000, 1, 1); err != nil {
			t.Fatal(err)
		}
		out[i] = metrics.SeriesChunks{ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{c}}
	}
	return out
}

func storeSink(t *testing.T, stores []*fakeStore, rf int, q Straggler, observe FlushObserver) (*ReplicatedBlockSink, *ring.Ring) {
	t.Helper()
	urls := make([]string, len(stores))
	clients := map[string]*Client{}
	for i, s := range stores {
		urls[i] = s.url
		c, err := NewClient("store", s.url)
		if err != nil {
			t.Fatal(err)
		}
		clients[s.url] = c
	}
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	return NewReplicatedBlockSink(r, rf, clients, q, observe), r
}

// Every series reaches exactly its R_s stores, one request per store.
func TestReplicatedBlockSinkSendsEachSeriesToItsStores(t *testing.T) {
	stores := startStores(t, 4, nil)
	sink, r := storeSink(t, stores, 3, &recordQueue{}, nil)
	batch := seriesBatch(t, 20)
	if _, err := sink.IngestSeriesChunks(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		if len(s.bodies) > 1 {
			t.Errorf("store %s got %d requests, want at most 1", s.url, len(s.bodies))
		}
	}
	for _, sc := range batch {
		reps, _ := r.Replicas(sc.Labels.Hash(), 3)
		name := sc.Labels.Map()["i"]
		for _, s := range stores {
			has := len(s.bodies) == 1 && strings.Contains(s.bodies[0], `"i":"`+name+`"`)
			if has != containsStr(reps, s.url) {
				t.Errorf("series i=%s on %s = %v, want %v", name, s.url, has, !has)
			}
		}
	}
}

func containsStr(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// One store down: quorum met, the batch succeeds, that store's group is queued, outcome degraded.
func TestReplicatedBlockSinkDegradedQueuesTheStraggler(t *testing.T) {
	stores := startStores(t, 4, map[int]int{1: http.StatusServiceUnavailable})
	q := &recordQueue{}
	var outcomes []string
	sink, _ := storeSink(t, stores, 3, q, func(o string) { outcomes = append(outcomes, o) })
	if _, err := sink.IngestSeriesChunks(context.Background(), seriesBatch(t, 20)); err != nil {
		t.Fatalf("one store down: %v", err)
	}
	if len(q.items) != 1 || q.items[0] != stores[1].url {
		t.Errorf("queued %v, want the down store once", q.items)
	}
	if len(outcomes) != 1 || outcomes[0] != "degraded" {
		t.Errorf("outcomes = %v, want [degraded]", outcomes)
	}
}

// Two stores down with 4 stores at R_s 3: some key loses quorum; the batch fails as an outage.
func TestReplicatedBlockSinkQuorumMissFailsTheBatch(t *testing.T) {
	stores := startStores(t, 4, map[int]int{0: http.StatusServiceUnavailable, 1: http.StatusServiceUnavailable})
	q := &recordQueue{}
	var outcomes []string
	sink, _ := storeSink(t, stores, 3, q, func(o string) { outcomes = append(outcomes, o) })
	_, err := sink.IngestSeriesChunks(context.Background(), seriesBatch(t, 20))
	var qe *StoreQuorumError
	if !errors.As(err, &qe) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a StoreQuorumError over ErrUnavailable", err)
	}
	if msg := err.Error(); !strings.Contains(msg, MemberLabel(stores[0].url)) || !strings.Contains(msg, qe.Cause.Error()) {
		t.Errorf("message %q names neither a failing store nor the cause %q", msg, qe.Cause)
	}
	if len(q.items) != 0 {
		t.Errorf("queued %v; a failed batch stays in the head, nothing is queued", q.items)
	}
	if len(outcomes) != 1 || outcomes[0] != "failed" {
		t.Errorf("outcomes = %v, want [failed]", outcomes)
	}
}

// A protocol error that breaks quorum is not an outage.
func TestReplicatedBlockSinkProtocolErrorIsNotAnOutage(t *testing.T) {
	stores := startStores(t, 3, map[int]int{0: http.StatusBadRequest, 1: http.StatusBadRequest})
	sink, _ := storeSink(t, stores, 3, &recordQueue{}, nil)
	_, err := sink.IngestSeriesChunks(context.Background(), seriesBatch(t, 5))
	var qe *StoreQuorumError
	if !errors.As(err, &qe) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a StoreQuorumError over the protocol error", err)
	}
}

// Review Focus 2: different keys fail on different stores, every key still at
// quorum. Two stores are down, and the batch keeps only keys that have at most
// one of them among their replicas, so each down store misses its own keys and
// is queued once with them.
func TestReplicatedBlockSinkQueuesEachFailedStoreOnce(t *testing.T) {
	stores := startStores(t, 5, map[int]int{0: http.StatusServiceUnavailable, 1: http.StatusServiceUnavailable})
	q := &recordQueue{}
	sink, r := storeSink(t, stores, 3, q, nil)
	var batch []metrics.SeriesChunks
	onDown := map[string]int{}
	for _, sc := range seriesBatch(t, 80) {
		reps, err := r.Replicas(sc.Labels.Hash(), 3)
		if err != nil {
			t.Fatal(err)
		}
		on0, on1 := containsStr(reps, stores[0].url), containsStr(reps, stores[1].url)
		if on0 && on1 {
			continue
		}
		if on0 {
			onDown[stores[0].url]++
		}
		if on1 {
			onDown[stores[1].url]++
		}
		batch = append(batch, sc)
	}
	if onDown[stores[0].url] == 0 || onDown[stores[1].url] == 0 {
		t.Fatalf("keys on each down store = %v; the batch must fail keys on both", onDown)
	}
	if _, err := sink.IngestSeriesChunks(context.Background(), batch); err != nil {
		t.Fatalf("every key has quorum, but the flush failed: %v", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) != 2 || !containsStr(q.items, stores[0].url) || !containsStr(q.items, stores[1].url) {
		t.Errorf("queued %v, want each down store's one group", q.items)
	}
	for _, s := range stores[:2] {
		if len(s.bodies) != 1 {
			t.Errorf("down store %s got %d requests, want 1", s.url, len(s.bodies))
		}
	}
}

// R_s 1 over one store: exactly 6.3's single-store flush.
func TestReplicatedBlockSinkSingleStoreIsPlainFlush(t *testing.T) {
	stores := startStores(t, 1, nil)
	sink, _ := storeSink(t, stores, 1, &recordQueue{}, nil)
	if _, err := sink.IngestSeriesChunks(context.Background(), seriesBatch(t, 3)); err != nil {
		t.Fatal(err)
	}
	if len(stores[0].bodies) != 1 {
		t.Fatalf("store got %d requests, want 1", len(stores[0].bodies))
	}
}

func TestReplicatedChunkSinkDegradedAndFailed(t *testing.T) {
	streams := make([]logs.StreamData, 20)
	for i := range streams {
		l, err := logs.NewStreamLabels(map[string]string{"service": fmt.Sprint("s", i)})
		if err != nil {
			t.Fatal(err)
		}
		streams[i] = logs.StreamData{Labels: l, Entries: []logs.LogEntry{{TimestampNs: 1, Line: "x"}}}
	}
	build := func(status map[int]int, q Straggler) (*ReplicatedChunkSink, []*fakeStore) {
		stores := startStores(t, 4, status)
		urls := make([]string, len(stores))
		clients := map[string]*Client{}
		for i, s := range stores {
			urls[i] = s.url
			c, err := NewClient("store", s.url)
			if err != nil {
				t.Fatal(err)
			}
			clients[s.url] = c
		}
		r, err := ring.New(urls)
		if err != nil {
			t.Fatal(err)
		}
		return NewReplicatedChunkSink(r, 3, clients, q, nil), stores
	}
	q := &recordQueue{}
	sink, stores := build(map[int]int{1: http.StatusServiceUnavailable}, q)
	if err := sink.IngestStreams(context.Background(), streams); err != nil {
		t.Fatalf("one store down: %v", err)
	}
	if len(q.items) != 1 || q.items[0] != stores[1].url {
		t.Errorf("queued %v, want the down store once", q.items)
	}
	q2 := &recordQueue{}
	sink, _ = build(map[int]int{0: http.StatusServiceUnavailable, 1: http.StatusServiceUnavailable}, q2)
	err := sink.IngestStreams(context.Background(), streams)
	var qe *StoreQuorumError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want a StoreQuorumError", err)
	}
	if len(q2.items) != 0 {
		t.Errorf("queued %v on a failed batch", q2.items)
	}
}

// WithRequireAll: one owner down fails the batch even though quorum holds --
// the handoff needs every owner to take a copy.
func TestReplicatedSinksRequireAllNeedEveryOwner(t *testing.T) {
	stores := startStores(t, 3, map[int]int{1: http.StatusServiceUnavailable})
	urls := make([]string, len(stores))
	clients := map[string]*Client{}
	for i, s := range stores {
		urls[i] = s.url
		c, err := NewClient("store", s.url)
		if err != nil {
			t.Fatal(err)
		}
		clients[s.url] = c
	}
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	q := &recordQueue{}
	_, err = NewReplicatedBlockSink(r, 3, clients, q, nil, WithRequireAll()).IngestSeriesChunks(context.Background(), seriesBatch(t, 5))
	var qe *StoreQuorumError
	if !errors.As(err, &qe) || qe.Quorum != 3 || qe.Failed != 5 || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("block sink err = %v, want a StoreQuorumError needing 3 of 3 for all 5 series", err)
	}
	l, _ := logs.NewStreamLabels(map[string]string{"service": "a"})
	err = NewReplicatedChunkSink(r, 3, clients, q, nil, WithRequireAll()).IngestStreams(context.Background(),
		[]logs.StreamData{{Labels: l, Entries: []logs.LogEntry{{TimestampNs: 1, Line: "x"}}}})
	if !errors.As(err, &qe) || qe.Quorum != 3 {
		t.Fatalf("chunk sink err = %v, want a StoreQuorumError needing 3 of 3", err)
	}
	if len(q.items) != 0 {
		t.Errorf("queued %v; a require-all flush queues nothing", q.items)
	}
	// Without the option the same outage is a degraded success.
	if _, err := NewReplicatedBlockSink(r, 3, clients, nil, nil).IngestSeriesChunks(context.Background(), seriesBatch(t, 5)); err != nil {
		t.Fatalf("quorum flush with one owner down: %v", err)
	}
}

// A quorum miss's cause is an outage only when every failure was an outage; a
// cancellation among them wins, and a protocol error wins over both.
func TestStoreCausePrefersProtocolThenCancellation(t *testing.T) {
	outage := fmt.Errorf("%w: s1 down", ErrUnavailable)
	canceled := fmt.Errorf("s2: %w", context.Canceled)
	protocol := errors.New("s3 bad answer")
	for _, tc := range []struct {
		name string
		errs []error
		want error
	}{
		{"outages only", []error{outage, outage}, outage},
		{"outage then cancellation", []error{outage, canceled}, canceled},
		{"cancellation then outage", []error{canceled, outage}, canceled},
		{"protocol among both", []error{outage, protocol, canceled}, protocol},
	} {
		if got := storeCause(tc.errs); got != tc.want {
			t.Errorf("%s: cause = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// One store answers, one is down, and one is cancelled mid-flush: the batch
// misses quorum, and the error is a cancellation, not an outage.
func TestReplicatedBlockSinkOutageAndCancellationIsNotAnOutage(t *testing.T) {
	stores := startStores(t, 2, map[int]int{1: http.StatusServiceUnavailable})
	downAnswered := make(chan struct{})
	// It reads the body first: the server notices the client went away only once
	// the body is consumed.
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(hung.Close)
	stores = append(stores, &fakeStore{url: hung.URL})
	// Cancel once the down store has taken its request, so its 503 is (almost
	// always) in before the cancellation; if it is not, both failures are
	// cancellations and the assertion below still holds.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			stores[1].mu.Lock()
			n := len(stores[1].bodies)
			stores[1].mu.Unlock()
			if n > 0 {
				close(downAnswered)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		<-downAnswered
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	sink, _ := storeSink(t, stores, 3, &recordQueue{}, nil)
	_, err := sink.IngestSeriesChunks(ctx, seriesBatch(t, 5))
	var qe *StoreQuorumError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want a StoreQuorumError", err)
	}
	if errors.Is(err, ErrUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a cancellation, not an outage", err)
	}
}
