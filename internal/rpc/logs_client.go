package rpc

import (
	"context"
	"net/http"
	"net/url"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// LogsSource is a logs.Source served by a peer's /internal/v1/logs routes.
type LogsSource struct{ c *Client }

var _ logs.Source = (*LogsSource)(nil)

func NewLogsSource(c *Client) *LogsSource { return &LogsSource{c: c} }

func (s *LogsSource) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]logs.StreamData, error) {
	var resp logsSelectResponse
	if err := s.c.do(ctx, http.MethodPost, "logs/select", nil, logsSelectRequest{
		Matchers: pairsToWire(matchers), MinNs: minTs, MaxNs: maxTs,
	}, &resp); err != nil {
		return nil, err
	}
	return streamsFromWire(resp.Streams)
}

func (s *LogsSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	var resp namesResponse
	if err := s.c.do(ctx, http.MethodGet, "logs/labels", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Names, nil
}

func (s *LogsSource) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	var resp valuesResponse
	if err := s.c.do(ctx, http.MethodGet, "logs/label-values", url.Values{"name": {name}}, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Values, nil
}
