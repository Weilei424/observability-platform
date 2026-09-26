package rpc

import (
	"errors"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// MountStore registers the routes only the store serves: flush-in for both
// signals, and the block maintenance the compactor drives. A flush answers
// only once the data is durable and queryable — the ingester drops what it
// sent when it gets the 200, and that ordering is what keeps a flush invisible
// to queries.
func MountStore(r chi.Router, blocks *metrics.BlockStore, chunks *logs.ChunkStore) {
	r.Post("/metrics/flush", func(w http.ResponseWriter, req *http.Request) { metricsFlush(w, req, blocks) })
	r.Post("/logs/flush", func(w http.ResponseWriter, req *http.Request) { logsFlush(w, req, chunks) })
	r.Get("/metrics/blocks", func(w http.ResponseWriter, req *http.Request) {
		infos := blocks.BlockInfos()
		out := blocksResponse{Blocks: make([]wireBlockInfo, len(infos))}
		for i, b := range infos {
			out.Blocks[i] = wireBlockInfo{ID: b.ID, Level: b.Level, MinTime: b.MinTime, MaxTime: b.MaxTime, SizeBytes: b.SizeBytes}
		}
		writeJSON(w, http.StatusOK, out)
	})
	r.Post("/metrics/compact", func(w http.ResponseWriter, req *http.Request) { compact(w, req, blocks) })
	r.Post("/metrics/retention", func(w http.ResponseWriter, req *http.Request) { retention(w, req, blocks) })
}

func metricsFlush(w http.ResponseWriter, r *http.Request, blocks *metrics.BlockStore) {
	body, ok := readBody(w, r, FlushBodyLimit)
	if !ok {
		return
	}
	if !utf8.Valid(body) {
		writeError(w, http.StatusBadRequest, "request body is not valid UTF-8")
		return
	}
	var req metricsFlushRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	if len(req.Series) == 0 {
		writeError(w, http.StatusBadRequest, "flush carries no series")
		return
	}
	series := make([]metrics.SeriesChunks, 0, len(req.Series))
	for i, ws := range req.Series {
		l, err := metrics.NewLabels(ws.Labels)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("series %d: %v", i, err))
			return
		}
		if len(ws.Chunks) == 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("series %d carries no chunks", i))
			return
		}
		sc := metrics.SeriesChunks{ID: metrics.SeriesID(l.Hash()), Labels: l}
		for j, raw := range ws.Chunks {
			c, err := chunk.FromBytes(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("series %d chunk %d: %v", i, j, err))
				return
			}
			sc.Chunks = append(sc.Chunks, c)
		}
		series = append(series, sc)
	}
	meta, err := blocks.IngestSeriesChunks(r.Context(), series)
	if err != nil {
		// IngestSeriesChunks wraps ErrInvalidSeriesChunks for a caller error in the
		// series it was handed (e.g. the same series twice in one flush): that is a
		// 400, not a storage failure, per spec §6.3. Anything else is a 500.
		if errors.Is(err, metrics.ErrInvalidSeriesChunks) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		internalError(w, r, "metrics flush-in failed", err)
		return
	}
	writeJSON(w, http.StatusOK, metricsFlushResponse{BlockID: meta.BlockID, Series: meta.NumSeries, Samples: meta.NumSamples})
}

func logsFlush(w http.ResponseWriter, r *http.Request, chunks *logs.ChunkStore) {
	body, ok := readBody(w, r, FlushBodyLimit)
	if !ok {
		return
	}
	// encoding/json would replace invalid UTF-8 with U+FFFD and store a line
	// that is not the one sent. The push path already refuses such bodies; the
	// store refuses them again rather than trust its caller.
	if !utf8.Valid(body) {
		writeError(w, http.StatusBadRequest, "request body is not valid UTF-8")
		return
	}
	var req logsFlushRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	if len(req.Streams) == 0 {
		writeError(w, http.StatusBadRequest, "flush carries no streams")
		return
	}
	streams := make([]logs.StreamData, 0, len(req.Streams))
	entries := 0
	for i, ws := range req.Streams {
		l, err := logs.NewStreamLabels(ws.Labels)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("stream %d: %v", i, err))
			return
		}
		id := logs.StreamIDOf(l)
		sd := logs.StreamData{Labels: l, Entries: make([]logs.LogEntry, 0, len(ws.Entries))}
		for j, e := range ws.Entries {
			le := logs.LogEntry{StreamID: id, TimestampNs: e.T, Line: e.Line}
			if err := logs.ValidateEntry(le); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("stream %d entry %d: %v", i, j, err))
				return
			}
			sd.Entries = append(sd.Entries, le)
		}
		entries += len(sd.Entries)
		streams = append(streams, sd)
	}
	if err := chunks.IngestStreams(r.Context(), streams); err != nil {
		internalError(w, r, "logs flush-in failed", err)
		return
	}
	writeJSON(w, http.StatusOK, logsFlushResponse{Streams: len(streams), Entries: entries})
}

// compact runs the groups the compactor planned. BlockStore.CompactOnce skips
// any group naming a block that no longer exists, so a plan made from a stale
// listing is harmless.
func compact(w http.ResponseWriter, r *http.Request, blocks *metrics.BlockStore) {
	body, ok := readBody(w, r, selectBodyLimit)
	if !ok {
		return
	}
	var req compactRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	n, err := blocks.CompactOnce(func([]block.BlockInfo) [][]string { return req.Groups })
	if err != nil {
		observability.Component(observability.FromContext(r.Context()), "rpc").Error("compaction failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, compactResponse{Compacted: n, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, compactResponse{Compacted: n})
}

// retention applies the compactor's clock and window: the policy is the
// compactor's, the deletion the store's.
func retention(w http.ResponseWriter, r *http.Request, blocks *metrics.BlockStore) {
	body, ok := readBody(w, r, selectBodyLimit)
	if !ok {
		return
	}
	var req retentionRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	if req.RetentionMs < 0 {
		writeError(w, http.StatusBadRequest, "retention_ms must be >= 0")
		return
	}
	n, err := blocks.ApplyRetention(time.UnixMilli(req.NowMs), time.Duration(req.RetentionMs)*time.Millisecond)
	if err != nil {
		observability.Component(observability.FromContext(r.Context()), "rpc").Error("retention failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, retentionResponse{Deleted: n, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, retentionResponse{Deleted: n})
}
