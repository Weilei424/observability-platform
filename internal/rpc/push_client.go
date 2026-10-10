package rpc

import (
	"context"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// PushSamples sends samples to the ingester's push route, grouped by series in
// first-seen order; each series keeps its samples' order.
func (c *Client) PushSamples(ctx context.Context, samples []metrics.PendingSample) error {
	_, err := c.PushSamplesGen(ctx, samples, true)
	return err
}

// PushSamplesGen sends samples to the ingester's push route, with each
// sample's generation only when withGen is set: an ingester from before 6.3
// refuses a sample that carries one. It reports whether the ingester said it
// takes generations (PushGenerationsHeader on its answer).
func (c *Client) PushSamplesGen(ctx context.Context, samples []metrics.PendingSample, withGen bool) (bool, error) {
	req := metricsPushRequest{}
	pos := make(map[uint64]int)
	for _, s := range samples {
		h := s.Labels.Hash()
		i, ok := pos[h]
		if !ok {
			i = len(req.Series)
			pos[h] = i
			req.Series = append(req.Series, wirePushSeries{Labels: s.Labels.Map()})
		}
		ps := wirePushSample{T: s.TimestampMs, V: s.Value}
		if withGen {
			ps.G = s.Gen
		}
		req.Series[i].Samples = append(req.Series[i].Samples, ps)
	}
	hdr, err := c.callHeader(ctx, http.MethodPost, "metrics/push", req, http.StatusNoContent)
	return err == nil && hdr.Get(PushGenerationsHeader) == "1", err
}

// PushEntries sends log lines to the ingester's push route, grouped by stream
// in first-seen order; each stream keeps its lines' order.
func (c *Client) PushEntries(ctx context.Context, entries []logs.PendingEntry) error {
	req := logsPushRequest{}
	pos := make(map[uint64]int)
	for _, e := range entries {
		h := e.Labels.Hash()
		i, ok := pos[h]
		if !ok {
			i = len(req.Streams)
			pos[h] = i
			req.Streams = append(req.Streams, wireStream{Labels: e.Labels.Map()})
		}
		req.Streams[i].Entries = append(req.Streams[i].Entries, wireEntry{T: e.TimestampNs, Line: e.Line})
	}
	return c.doNoContent(ctx, http.MethodPost, "logs/push", req)
}
