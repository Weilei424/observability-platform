package compactor

import (
	"context"
	"log/slog"
	"time"

	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// Flusher flushes sealed head chunks to a new block and advances the WAL
// checkpoint. FlushBlock reports whether a block was actually written (false
// for a no-op when no sealed chunks exist) so the loop counts only real
// flushes. SealedChunkCount is the head's backlog, the count-based trigger.
type Flusher interface {
	FlushBlock() (bool, error)
	SealedChunkCount() int
}

// WALSizer reports the WAL's current on-disk size.
type WALSizer interface {
	WALBytes() (int64, error)
}

// BlockManager is the block-set mechanism the compactor drives. In all-in-one
// it is the local BlockStore; in the compactor target it is a client for the
// store component, which executes each plan under its own lock.
type BlockManager interface {
	CompactOnce(plan func([]block.BlockInfo) [][]string) (int, error)
	ApplyRetention(now time.Time, retention time.Duration) (int, error)
}

// Config controls maintenance cadence and policy.
type Config struct {
	MaintenanceInterval time.Duration
	FlushInterval       time.Duration
	FlushSealedChunks   int
	FlushWALBytes       int64
	Ranges              []int64 // ascending compaction ranges in ms
	Retention           time.Duration
}

// Compactor runs background storage maintenance: flush, compaction, retention.
type Compactor struct {
	flusher   Flusher
	blocks    BlockManager
	wal       WALSizer
	clock     func() time.Time
	cfg       Config
	metrics   *observability.Metrics
	log       *slog.Logger
	lastFlush time.Time
}

// New builds a Compactor. clock defaults to time.Now when nil. Any of flusher,
// blocks, and walSizer may be nil: the ingester passes only a flusher and a
// WAL sizer, the compactor target only a block manager, and all-in-one all three.
func New(flusher Flusher, blocks BlockManager, walSizer WALSizer, clock func() time.Time, cfg Config, metrics *observability.Metrics, log *slog.Logger) *Compactor {
	if clock == nil {
		clock = time.Now
	}
	return &Compactor{
		flusher:   flusher,
		blocks:    blocks,
		wal:       walSizer,
		clock:     clock,
		cfg:       cfg,
		metrics:   metrics,
		log:       log,
		lastFlush: clock(),
	}
}

// RunOnce performs one maintenance pass: flush (if due) → compact to stability →
// retention. Errors are logged and metered; a pass is best-effort.
func (c *Compactor) RunOnce(ctx context.Context) {
	c.maybeFlush()
	c.compactToStable(ctx)
	c.applyRetention()
}

func (c *Compactor) maybeFlush() {
	if c.flusher == nil {
		return
	}
	due := c.clock().Sub(c.lastFlush) >= c.cfg.FlushInterval
	if !due && c.cfg.FlushSealedChunks > 0 && c.flusher.SealedChunkCount() >= c.cfg.FlushSealedChunks {
		due = true
	}
	if !due && c.cfg.FlushWALBytes > 0 && c.wal != nil {
		if n, err := c.wal.WALBytes(); err == nil && n >= c.cfg.FlushWALBytes {
			due = true
		}
	}
	if !due {
		return
	}
	wrote, err := c.flusher.FlushBlock()
	if err != nil {
		c.metrics.FlushFailuresTotal.Inc()
		c.log.Warn("flush failed", slog.String("error", err.Error()))
		return
	}
	c.lastFlush = c.clock()
	if wrote {
		c.metrics.FlushesTotal.Inc()
	}
}

func (c *Compactor) compactToStable(ctx context.Context) {
	if c.blocks == nil {
		return
	}
	plan := func(infos []block.BlockInfo) [][]string { return Plan(infos, c.cfg.Ranges) }
	for {
		if ctx.Err() != nil {
			return
		}
		start := c.clock()
		n, err := c.blocks.CompactOnce(plan)
		if err != nil {
			c.metrics.CompactionFailuresTotal.Inc()
			c.log.Warn("compaction failed", slog.String("error", err.Error()))
			return
		}
		if n == 0 {
			return
		}
		c.metrics.CompactionsTotal.Add(float64(n))
		c.metrics.CompactionDuration.Observe(c.clock().Sub(start).Seconds())
	}
}

func (c *Compactor) applyRetention() {
	if c.blocks == nil {
		return
	}
	deleted, err := c.blocks.ApplyRetention(c.clock(), c.cfg.Retention)
	if err != nil {
		c.log.Warn("retention failed", slog.String("error", err.Error()))
	}
	if deleted > 0 {
		c.metrics.RetentionDeletedTotal.Add(float64(deleted))
	}
}

// Run drives RunOnce on the maintenance ticker until ctx is cancelled, then does
// one final flush so a clean shutdown persists sealed chunks.
func (c *Compactor) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.MaintenanceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if c.flusher != nil {
				if _, err := c.flusher.FlushBlock(); err != nil {
					c.log.Warn("final flush failed", slog.String("error", err.Error()))
				}
			}
			return
		case <-t.C:
			c.RunOnce(ctx)
		}
	}
}
