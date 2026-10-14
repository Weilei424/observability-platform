package app

import (
	"io"
	"log/slog"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

func TestReplayMetricsWALKeepsTenantSeries(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	pairs := []wal.LabelPair{{Name: "__name__", Value: "up"}, {Name: "__tenant__", Value: "team-b"}}
	if err := w.WriteRecordGen(pairs, 1000, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var got []metrics.Labels
	n, err := replayMetricsWAL(slog.New(slog.NewTextHandler(io.Discard, nil)), dir, 0,
		func(l metrics.Labels, ts int64, v float64, gen int64) error {
			got = append(got, l)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(got) != 1 || labels.TenantOf(got[0]) != "team-b" {
		t.Fatalf("restored %d, got %v", n, got)
	}
}
