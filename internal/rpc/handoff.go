package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

// handoffDeadline bounds a whole handoff; each batch is bounded separately by
// MountHandoff's timeout.
const handoffDeadline = 10 * time.Minute

// handingOffMessage is the flush-in routes' answer once the barrier is closed.
const handingOffMessage = "store is handing off: it takes no new flushes"

// FlushBarrier refuses store flush-ins once a handoff starts, until restart.
// The zero value is open.
//
// Close also waits for every flush-in already admitted to finish: a flush that
// passed the check just before Close must be durable before the handoff's
// export starts, or its block could land after the export's last snapshot
// and leave with the store.
type FlushBarrier struct {
	mu     sync.RWMutex // held for reading by each admitted flush-in
	closed atomic.Bool
}

// Close shuts the barrier, waiting for the flush-ins already admitted. It is
// idempotent: a second handoff closes it again harmlessly.
func (b *FlushBarrier) Close() {
	b.mu.Lock()
	b.closed.Store(true)
	b.mu.Unlock()
}

// Closed reports whether a handoff has closed the barrier.
func (b *FlushBarrier) Closed() bool { return b.closed.Load() }

// enter admits one flush-in unless the barrier is closed; release must be
// called once its data is durable (or it failed).
func (b *FlushBarrier) enter() (release func(), ok bool) {
	b.mu.RLock()
	if b.closed.Load() {
		b.mu.RUnlock()
		return nil, false
	}
	return b.mu.RUnlock, true
}

// MountHandoff registers POST /handoff (spec §8.2): the leaving store closes
// its flush barrier and copies every metrics series, then every log stream,
// to every owner on the target store ring through their flush-in routes. It
// answers 200 only when every owner acknowledged every batch; otherwise 503
// with the counts sent so far, and a retry resends everything (duplicates
// collapse on read and compaction). self is the store's own URL, refused as a
// target when set; timeout bounds each batch request.
func MountHandoff(r chi.Router, blocks *metrics.BlockStore, chunks *logs.ChunkStore, barrier *FlushBarrier, self string, timeout time.Duration) {
	if s, err := config.CanonicalPeerURL(self); err == nil {
		self = s
	}
	r.Post("/handoff", func(w http.ResponseWriter, req *http.Request) {
		handoff(w, req, blocks, chunks, barrier, self, timeout)
	})
}

func handoff(w http.ResponseWriter, r *http.Request, blocks *metrics.BlockStore, chunks *logs.ChunkStore,
	barrier *FlushBarrier, self string, timeout time.Duration) {
	body, ok := readBody(w, r, selectBodyLimit)
	if !ok {
		return
	}
	var req handoffRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	// Everything about the body is checked before the barrier closes: a
	// refused handoff must leave the store taking flushes.
	targets, msg := handoffTargets(req, self)
	if msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}
	rg, err := ring.New(targets)
	if err != nil {
		writeError(w, http.StatusBadRequest, "stores: "+err.Error())
		return
	}
	clients := make(map[string]*Client, len(targets))
	for _, u := range targets {
		c, err := NewClient("store", u, WithRequestTimeout(timeout))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		defer c.http.CloseIdleConnections()
		clients[u] = c
	}

	log := observability.Component(observability.FromContext(r.Context()), "rpc")
	barrier.Close()
	log.Info("store handoff started", slog.Int("stores", len(targets)), slog.Int("replication_factor", req.ReplicationFactor))
	ctx, cancel := context.WithTimeout(r.Context(), handoffDeadline)
	defer cancel()
	series, streams, err := runHandoff(ctx, blocks, chunks, rg, req.ReplicationFactor, clients)
	if err != nil {
		log.Error("store handoff failed", slog.Int("series", series), slog.Int("streams", streams), slog.String("error", err.Error()))
		writeJSON(w, http.StatusServiceUnavailable, handoffResponse{Series: series, Streams: streams, Error: err.Error()})
		return
	}
	log.Info("store handoff done", slog.Int("series", series), slog.Int("streams", streams))
	writeJSON(w, http.StatusOK, handoffResponse{Series: series, Streams: streams})
}

// handoffTargets normalizes and checks the body's stores, returning a 400
// message when the body is refused.
func handoffTargets(req handoffRequest, self string) ([]string, string) {
	if len(req.Stores) == 0 {
		return nil, "stores must list at least one store"
	}
	targets := make([]string, len(req.Stores))
	seen := make(map[string]int, len(req.Stores))
	for i, raw := range req.Stores {
		u, err := config.CanonicalPeerURL(raw)
		if err != nil {
			return nil, fmt.Sprintf("stores[%d]: %v", i, err)
		}
		if j, dup := seen[u]; dup {
			return nil, fmt.Sprintf("stores[%d] %q is stores[%d] again", i, u, j)
		}
		if self != "" && u == self {
			return nil, fmt.Sprintf("stores[%d] %q is this store: a handoff copies to the stores that remain", i, u)
		}
		seen[u] = i
		targets[i] = u
	}
	if req.ReplicationFactor < 1 || req.ReplicationFactor > len(targets) {
		return nil, fmt.Sprintf("replication_factor must be between 1 and the %d stores listed, got %d", len(targets), req.ReplicationFactor)
	}
	return targets, ""
}

// runHandoff exports metrics, then logs, through require-all replicated sinks.
// series counts distinct series with an acknowledged batch, streams the
// streams acknowledged. Any batch error stops it. A cancelled or expired ctx
// is never success, even when nothing was left to send.
func runHandoff(ctx context.Context, blocks *metrics.BlockStore, chunks *logs.ChunkStore, rg *ring.Ring, rf int,
	clients map[string]*Client) (series, streams int, err error) {
	sent := make(map[metrics.SeriesID]struct{})
	bsink := NewReplicatedBlockSink(rg, rf, clients, nil, nil, WithRequireAll())
	if _, err := blocks.ExportSeries(ctx, metrics.DefaultFlushBatchBytes, func(batch []metrics.SeriesChunks) error {
		if _, err := bsink.IngestSeriesChunks(ctx, batch); err != nil {
			return err
		}
		for _, sc := range batch {
			sent[sc.ID] = struct{}{}
		}
		return nil
	}); err != nil {
		return len(sent), 0, fmt.Errorf("handoff metrics: %w", err)
	}
	csink := NewReplicatedChunkSink(rg, rf, clients, nil, nil, WithRequireAll())
	if _, err := chunks.ExportStreams(ctx, logs.DefaultLogFlushBatchBytes, func(batch []logs.StreamData) error {
		if err := csink.IngestStreams(ctx, batch); err != nil {
			return err
		}
		streams += len(batch)
		return nil
	}); err != nil {
		return len(sent), streams, fmt.Errorf("handoff logs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return len(sent), streams, fmt.Errorf("handoff: %w", err)
	}
	return len(sent), streams, nil
}
