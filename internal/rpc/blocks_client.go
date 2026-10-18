package rpc

import (
	"context"
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"net/http"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// MaintenanceTimeout bounds one compaction or retention call. The loop's
// interface carries no context, and a merge of large blocks takes a while.
const MaintenanceTimeout = 2 * time.Minute

// BlockManager drives the store's block maintenance for the compactor target.
// CompactOnce lists the store's blocks, runs the plan locally — planning is
// the compactor's policy — and posts the chosen groups; the store re-checks
// that each group's blocks still exist and executes under its own lock.
//
// BlockManager satisfies compactor.BlockManager; the assertion lives in
// client_test.go rather than here, so this file does not carry a rpc ->
// compactor import edge it otherwise has no need for.
type BlockManager struct{ c *Client }

func NewBlockManager(c *Client) *BlockManager { return &BlockManager{c: c} }

func (b *BlockManager) CompactOnce(plan func([]block.BlockInfo) [][]string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MaintenanceTimeout)
	defer cancel()
	var list blocksResponse
	if err := b.c.do(ctx, http.MethodGet, "metrics/blocks", nil, nil, &list); err != nil {
		return 0, err
	}
	infos := make([]block.BlockInfo, len(list.Blocks))
	for i, w := range list.Blocks {
		infos[i] = block.BlockInfo{ID: w.ID, Level: w.Level, MinTime: w.MinTime, MaxTime: w.MaxTime, SizeBytes: w.SizeBytes, Tenant: w.Tenant}
	}
	groups := plan(infos)
	if len(groups) == 0 {
		return 0, nil
	}
	var resp compactResponse
	err := b.c.do(ctx, http.MethodPost, "metrics/compact", nil, compactRequest{Groups: groups}, &resp)
	return resp.Compacted, err
}

func (b *BlockManager) ApplyRetention(now time.Time, policy tenant.RetentionPolicy) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MaintenanceTimeout)
	defer cancel()
	var resp retentionResponse
	req := retentionRequest{NowMs: now.UnixMilli(), RetentionMs: policy.Default.Milliseconds()}
	if len(policy.Overrides) > 0 {
		req.OverridesMs = make(map[string]int64, len(policy.Overrides))
		for id, d := range policy.Overrides {
			req.OverridesMs[id] = d.Milliseconds()
		}
	}
	err := b.c.do(ctx, http.MethodPost, "metrics/retention", nil, req, &resp)
	return resp.Deleted, err
}
