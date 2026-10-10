package app

import (
	"context"
	"log/slog"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// skipNote collects, for one read, the ingesters the head merge skipped. The
// head merge records into it (MergeHeadsWith's OnSkip) when the heads are
// done; the noted source around the whole read -- heads, then store -- logs it
// only once that read has succeeded, through the request's logger.
type skipNote struct {
	mu      sync.Mutex
	skipped []string
}

type skipNoteKey struct{}

func withSkipNote(ctx context.Context) (context.Context, *skipNote) {
	n := &skipNote{}
	return context.WithValue(ctx, skipNoteKey{}, n), n
}

// recordSkip adds names to the read's note, if the read carries one.
func recordSkip(ctx context.Context, names []string) {
	if n, ok := ctx.Value(skipNoteKey{}).(*skipNote); ok {
		n.mu.Lock()
		n.skipped = append(n.skipped, names...)
		n.mu.Unlock()
	}
}

// report logs a successful read that relied on replication.
func (n *skipNote) report(ctx context.Context, err error) {
	if err != nil {
		return
	}
	n.mu.Lock()
	skipped := n.skipped
	n.mu.Unlock()
	if len(skipped) > 0 {
		observability.Component(observability.FromContext(ctx), "querier").Warn(
			"read answered by replication", slog.Any("skipped", skipped))
	}
}

// notedMetrics wraps the querier's whole metrics source (heads, then store).
type notedMetrics struct{ metrics.Source }

func (s notedMetrics) Select(ctx context.Context, p metrics.SelectParams) ([]metrics.SeriesData, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.Select(ctx, p)
	n.report(ctx, err)
	return out, err
}

func (s notedMetrics) SelectLabelNames(ctx context.Context) ([]string, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.SelectLabelNames(ctx)
	n.report(ctx, err)
	return out, err
}

func (s notedMetrics) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.SelectLabelValues(ctx, name)
	n.report(ctx, err)
	return out, err
}

// notedLogs wraps the querier's whole logs source (heads, then store).
type notedLogs struct{ logs.Source }

func (s notedLogs) SelectStreams(ctx context.Context, m []index.Pair, minTs, maxTs int64) ([]logs.StreamData, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.SelectStreams(ctx, m, minTs, maxTs)
	n.report(ctx, err)
	return out, err
}

func (s notedLogs) SelectLabelNames(ctx context.Context) ([]string, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.SelectLabelNames(ctx)
	n.report(ctx, err)
	return out, err
}

func (s notedLogs) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	ctx, n := withSkipNote(ctx)
	out, err := s.Source.SelectLabelValues(ctx, name)
	n.report(ctx, err)
	return out, err
}
