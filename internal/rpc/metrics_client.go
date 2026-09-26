package rpc

import (
	"context"
	"net/http"
	"net/url"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// MetricsSource is a metrics.Source served by a peer's /internal/v1/metrics
// routes. The querier holds one for the ingester and one for the store.
type MetricsSource struct{ c *Client }

var _ metrics.Source = (*MetricsSource)(nil)

func NewMetricsSource(c *Client) *MetricsSource { return &MetricsSource{c: c} }

func (s *MetricsSource) Select(ctx context.Context, p metrics.SelectParams) ([]metrics.SeriesData, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	var resp metricsSelectResponse
	if err := s.c.do(ctx, http.MethodPost, "metrics/select", nil, metricsSelectRequest{
		Matchers:   selectorToWire(p.Selector),
		MinMs:      p.MinT,
		MaxMs:      p.MaxT,
		Anchor:     p.Anchor,
		SeriesOnly: p.SeriesOnly,
		AnyTime:    p.AnyTime,
	}, &resp); err != nil {
		return nil, err
	}
	return seriesFromWire(resp.Series)
}

func (s *MetricsSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	var resp namesResponse
	if err := s.c.do(ctx, http.MethodGet, "metrics/labels", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Names, nil
}

func (s *MetricsSource) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	var resp valuesResponse
	if err := s.c.do(ctx, http.MethodGet, "metrics/label-values", url.Values{"name": {name}}, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Values, nil
}
