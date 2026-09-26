package rpc

import (
	"context"
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
		infos[i] = block.BlockInfo{ID: w.ID, Level: w.Level, MinTime: w.MinTime, MaxTime: w.MaxTime, SizeBytes: w.SizeBytes}
	}
	groups := plan(infos)
	if len(groups) == 0 {
		return 0, nil
	}
	var resp compactResponse
	err := b.c.do(ctx, http.MethodPost, "metrics/compact", nil, compactRequest{Groups: groups}, &resp)
	return resp.Compacted, err
}

func (b *BlockManager) ApplyRetention(now time.Time, retention time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MaintenanceTimeout)
	defer cancel()
	var resp retentionResponse
	err := b.c.do(ctx, http.MethodPost, "metrics/retention", nil, retentionRequest{
		NowMs: now.UnixMilli(), RetentionMs: retention.Milliseconds(),
	}, &resp)
	return resp.Deleted, err
}
