package rpc

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// StoreQuorumError is a flush in which some key could not reach W_s stores.
// The head keeps the whole batch; its Cause is the worst store failure, so
// errors.Is(err, ErrUnavailable) holds only when every failure was an outage.
type StoreQuorumError struct {
	Kind                      string // "series" or "streams"
	Failed, Total, Quorum, RF int
	Stores                    []string
	Cause                     error
}

func (e *StoreQuorumError) Error() string {
	return fmt.Sprintf("store quorum not met: %d of %d %s could not reach %d of %d stores", e.Failed, e.Total, e.Kind, e.Quorum, e.RF)
}
func (e *StoreQuorumError) Unwrap() error { return e.Cause }

// Straggler takes a store's flush body that a quorum flush failed to deliver.
type Straggler interface {
	Add(store, path string, body []byte)
}

// FlushObserver is told each batch's outcome: full, degraded, or failed.
type FlushObserver func(outcome string)

// SinkOption configures a replicated sink.
type SinkOption func(*sinkOptions)

type sinkOptions struct {
	requireAll bool
}

// WithRequireAll makes a batch succeed only when every owner of every key
// acknowledged it (need = rf, not the W_s quorum), and queues nothing. The
// store handoff uses it: the leaving store's copy is the one being retired, so
// each owner on the target ring must take its own.
func WithRequireAll() SinkOption {
	return func(o *sinkOptions) { o.requireAll = true }
}

func applySinkOptions(opts []SinkOption) sinkOptions {
	var o sinkOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// replicateFlush sends each store its part of a batch -- body(keys) built from the
// key indexes that store owns -- concurrently, and decides per-key quorum: W_s
// (rf/2+1), or every owner when requireAll is set.
func replicateFlush(ctx context.Context, kind string, keys []uint64, r *ring.Ring, rf int, requireAll bool, clients map[string]*Client,
	path string, body func(idx []int) ([]byte, error), q Straggler, observe FlushObserver) error {
	owned := map[string][]int{}
	replicasOf := make([][]string, len(keys))
	for i, k := range keys {
		reps, err := r.Replicas(k, rf)
		if err != nil {
			return fmt.Errorf("rpc: store routing: %w", err)
		}
		replicasOf[i] = reps
		for _, s := range reps {
			owned[s] = append(owned[s], i)
		}
	}
	type result struct {
		store string
		body  []byte
		err   error
	}
	var wg sync.WaitGroup
	results := make(chan result, len(owned))
	for store, idx := range owned {
		b, err := body(idx)
		if err != nil {
			return fmt.Errorf("rpc: encode %s flush: %w", kind, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- result{store, b, clients[store].postRaw(ctx, path, b)}
		}()
	}
	wg.Wait()
	close(results)
	failedStore := map[string]result{}
	for res := range results {
		if res.err != nil {
			failedStore[res.store] = res
		}
	}
	need := rf/2 + 1
	if requireAll {
		need = rf
	}
	failedKeys := 0
	var causes []error
	var culprits []string
	for i := range keys {
		acks := 0
		for _, s := range replicasOf[i] {
			if _, bad := failedStore[s]; !bad {
				acks++
			} else {
				causes = append(causes, failedStore[s].err)
				culprits = append(culprits, MemberLabel(s))
			}
		}
		if acks < need {
			failedKeys++
		}
	}
	if failedKeys > 0 {
		if observe != nil {
			observe("failed")
		}
		slices.Sort(culprits)
		return &StoreQuorumError{Kind: kind, Failed: failedKeys, Total: len(keys), Quorum: need, RF: rf,
			Stores: slices.Compact(culprits), Cause: worst(causes)}
	}
	for store, res := range failedStore {
		if q != nil {
			q.Add(store, path, res.body)
		}
	}
	if observe != nil {
		if len(failedStore) > 0 {
			observe("degraded")
		} else {
			observe("full")
		}
	}
	return nil
}

// ReplicatedBlockSink flushes sealed chunks to every store that owns each
// series, on the store ring, and succeeds once each series is on W_s stores.
type ReplicatedBlockSink struct {
	ring    *ring.Ring
	rf      int
	clients map[string]*Client
	q       Straggler
	observe FlushObserver
	opts    sinkOptions
}

var _ metrics.BlockSink = (*ReplicatedBlockSink)(nil)

func NewReplicatedBlockSink(r *ring.Ring, rf int, clients map[string]*Client, q Straggler, observe FlushObserver, opts ...SinkOption) *ReplicatedBlockSink {
	return &ReplicatedBlockSink{ring: r, rf: max(rf, 1), clients: clients, q: q, observe: observe, opts: applySinkOptions(opts)}
}

// IngestSeriesChunks returns a zero block.Meta: several stores each wrote
// their own block, and no caller reads the meta of a remote flush.
func (s *ReplicatedBlockSink) IngestSeriesChunks(ctx context.Context, series []metrics.SeriesChunks) (block.Meta, error) {
	keys := make([]uint64, len(series))
	for i, sc := range series {
		keys[i] = sc.Labels.Hash()
	}
	return block.Meta{}, replicateFlush(ctx, "series", keys, s.ring, s.rf, s.opts.requireAll, s.clients, "metrics/flush",
		func(idx []int) ([]byte, error) {
			req := metricsFlushRequest{Series: make([]wireChunkSeries, 0, len(idx))}
			for _, i := range idx {
				sc := series[i]
				ws := wireChunkSeries{Labels: sc.Labels.Map(), Chunks: make([][]byte, len(sc.Chunks))}
				for j, c := range sc.Chunks {
					ws.Chunks[j] = c.Bytes()
				}
				req.Series = append(req.Series, ws)
			}
			return marshalJSON(req)
		}, s.q, s.observe)
}

// ReplicatedChunkSink is ReplicatedBlockSink for log streams.
type ReplicatedChunkSink struct {
	ring    *ring.Ring
	rf      int
	clients map[string]*Client
	q       Straggler
	observe FlushObserver
	opts    sinkOptions
}

var _ logs.ChunkSink = (*ReplicatedChunkSink)(nil)

func NewReplicatedChunkSink(r *ring.Ring, rf int, clients map[string]*Client, q Straggler, observe FlushObserver, opts ...SinkOption) *ReplicatedChunkSink {
	return &ReplicatedChunkSink{ring: r, rf: max(rf, 1), clients: clients, q: q, observe: observe, opts: applySinkOptions(opts)}
}

func (s *ReplicatedChunkSink) IngestStreams(ctx context.Context, streams []logs.StreamData) error {
	keys := make([]uint64, len(streams))
	for i, sd := range streams {
		keys[i] = sd.Labels.Hash()
	}
	return replicateFlush(ctx, "streams", keys, s.ring, s.rf, s.opts.requireAll, s.clients, "logs/flush",
		func(idx []int) ([]byte, error) {
			part := make([]logs.StreamData, len(idx))
			for j, i := range idx {
				part[j] = streams[i]
			}
			return marshalJSON(logsFlushRequest{Streams: streamsToWire(part)})
		}, s.q, s.observe)
}

// postRaw POSTs an encoded JSON body to /internal/v1/<path> and expects 200,
// with the same error classification as do.
func (c *Client) postRaw(ctx context.Context, path string, body []byte) error {
	return c.callInto(ctx, http.MethodPost, path, nil, rawJSON(body), nil, http.StatusOK, nil)
}

// rawJSON is an already-encoded body; marshalJSON passes it through unchanged.
type rawJSON []byte
