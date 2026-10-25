package rpc

import (
	"context"
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"math"
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
	err := b.c.do(ctx, http.MethodPost, "metrics/retention", nil, newRetentionRequest(now, policy), &resp)
	return resp.Deleted, err
}

// newRetentionRequest is the wire form of a retention pass, shared by the
// metrics and logs retention clients.
func newRetentionRequest(now time.Time, policy tenant.RetentionPolicy) retentionRequest {
	req := retentionRequest{NowMs: now.UnixMilli(), RetentionMs: retentionMs(policy.Default)}
	if len(policy.Overrides) > 0 {
		req.OverridesMs = make(map[string]int64, len(policy.Overrides))
		for id, d := range policy.Overrides {
			req.OverridesMs[id] = retentionMs(d)
		}
	}
	return req
}

// retentionMs puts a retention on the wire in whole milliseconds. A store reads
// 0 as "keep forever", so a positive retention rounds up -- truncating 500µs to
// 0 would turn a retention into none -- while 0 and below stay 0.
func retentionMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d.Milliseconds()
	if d%time.Millisecond != 0 {
		ms++
	}
	// Rounding up the longest Duration passes what converts back to one;
	// sending the largest that does keeps it the same positive retention.
	return min(ms, maxRetentionMs)
}

// maxRetentionMs is the longest retention, in milliseconds, that converts back
// to a time.Duration without overflowing. The store refuses anything longer: an
// overflow would turn negative, which reads as "keep forever".
const maxRetentionMs = math.MaxInt64 / int64(time.Millisecond)
