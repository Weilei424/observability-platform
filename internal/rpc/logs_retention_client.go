package rpc

import (
	"context"
	"net/http"
	"time"

	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// LogsRetentionClient drives one store's logs retention for the compactor
// target: the policy and clock are the compactor's, the deletion the store's.
//
// It satisfies compactor.LogManager; the assertion lives in the tests, so this
// file carries no rpc -> compactor import edge.
type LogsRetentionClient struct{ c *Client }

func NewLogsRetentionClient(c *Client) *LogsRetentionClient { return &LogsRetentionClient{c: c} }

// ApplyLogsRetention returns the chunk files the store deleted, even when it
// also reports an error part way through.
func (l *LogsRetentionClient) ApplyLogsRetention(now time.Time, policy tenant.RetentionPolicy) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MaintenanceTimeout)
	defer cancel()
	var resp retentionResponse
	err := l.c.do(ctx, http.MethodPost, "logs/retention", nil, newRetentionRequest(now, policy), &resp)
	return resp.Deleted, err
}
