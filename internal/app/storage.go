package app

import (
	"log/slog"

	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// replayMetricsWAL replays walDir after checkpoint through appendFn, warning
// about and skipping records that no longer validate. It returns the number of
// samples restored. All-in-one replays into its BlockStore and the ingester
// into its HeadStore; both then set the head fence to checkpoint+1.
func replayMetricsWAL(walLog *slog.Logger, walDir string, checkpoint int, appendFn func(metrics.Labels, int64, float64) error) (int, error) {
	var restored int
	err := wal.ReplayFrom(walDir, checkpoint, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		labels, err := metrics.NewLabels(lm)
		if err != nil {
			walLog.Warn("WAL replay: skipping record with invalid labels", slog.String("error", err.Error()))
			return
		}
		if err := appendFn(labels, tsMs, value); err != nil {
			walLog.Warn("WAL replay: failed to append sample", slog.String("error", err.Error()))
			return
		}
		restored++
	})
	return restored, err
}

// maintenanceConfig is the loop configuration every target derives from cfg;
// each target runs the loop with only the collaborators it owns.
func maintenanceConfig(cfg *config.Config) compactor.Config {
	return compactor.Config{
		MaintenanceInterval: cfg.MaintenanceInterval,
		FlushInterval:       cfg.FlushInterval,
		FlushSealedChunks:   cfg.FlushSealedChunks,
		FlushWALBytes:       cfg.FlushWALBytes,
		Ranges:              compactor.Ranges(cfg.CompactionBaseRange.Milliseconds(), int64(cfg.CompactionMultiplier), cfg.CompactionLevels),
		Retention:           cfg.Retention,
	}
}
