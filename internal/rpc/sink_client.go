package rpc

import (
	"context"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// BlockSink sends sealed chunks to the store's flush-in route. The ingester's
// HeadStore flushes through it, batching so each request fits the store's limit.
type BlockSink struct{ c *Client }

var _ metrics.BlockSink = (*BlockSink)(nil)

func NewBlockSink(c *Client) *BlockSink { return &BlockSink{c: c} }

// IngestSeriesChunks flushes series to the store's flush-in route. The
// returned block.Meta is partial: only BlockID, NumSeries, and NumSamples are
// populated, matching the flush response's fields (spec §6.2). MinTime,
// MaxTime, CreatedAt, Level, Sources, and MaxGen are left at their zero value —
// the flush response carries nothing for them, so a caller must not read a
// zero there as a real Level 0 or MaxGen 0.
func (s *BlockSink) IngestSeriesChunks(ctx context.Context, series []metrics.SeriesChunks) (block.Meta, error) {
	req := metricsFlushRequest{Series: make([]wireChunkSeries, len(series))}
	for i, sc := range series {
		ws := wireChunkSeries{Labels: sc.Labels.Map(), Chunks: make([][]byte, len(sc.Chunks))}
		for j, c := range sc.Chunks {
			ws.Chunks[j] = c.Bytes()
		}
		req.Series[i] = ws
	}
	var resp metricsFlushResponse
	if err := s.c.do(ctx, http.MethodPost, "metrics/flush", nil, req, &resp); err != nil {
		return block.Meta{}, err
	}
	return block.Meta{BlockID: resp.BlockID, NumSeries: resp.Series, NumSamples: resp.Samples}, nil
}

// ChunkSink sends head streams to the store's logs flush-in route.
type ChunkSink struct{ c *Client }

var _ logs.ChunkSink = (*ChunkSink)(nil)

func NewChunkSink(c *Client) *ChunkSink { return &ChunkSink{c: c} }

func (s *ChunkSink) IngestStreams(ctx context.Context, streams []logs.StreamData) error {
	return s.c.do(ctx, http.MethodPost, "logs/flush", nil, logsFlushRequest{Streams: streamsToWire(streams)}, &logsFlushResponse{})
}
