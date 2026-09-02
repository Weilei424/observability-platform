package observability

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeLogStats struct {
	streams, chunks int
	bytes           int64
	err             error
}

func (f fakeLogStats) Stats() (int, int, int64, error) {
	return f.streams, f.chunks, f.bytes, f.err
}

func TestWALCollectorReportsBytesAndSegmentsPerWAL(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{s: 1, n: 2, p: 3},
		WALs: []WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 4096, 2, nil }},
			{Name: "logs", Stats: func() (int64, int, error) { return 512, 1, nil }},
		},
	})

	want := `
# HELP obs_wal_bytes Total size in bytes of the WAL segment files.
# TYPE obs_wal_bytes gauge
obs_wal_bytes{wal="logs"} 512
obs_wal_bytes{wal="metrics"} 4096
# HELP obs_wal_segments Number of WAL segment files.
# TYPE obs_wal_segments gauge
obs_wal_segments{wal="logs"} 1
obs_wal_segments{wal="metrics"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "obs_wal_bytes", "obs_wal_segments"); err != nil {
		t.Error(err)
	}
}

// A failed read must produce a gap, never a zero. A zero here would render as a
// WAL that had shrunk to nothing, which is what a real data-loss incident looks
// like on the dashboard.
//
// This constructs walCollector directly rather than going through NewRegistry.
// obs_collector_errors_total is a *prometheus.CounterVec registered as its own
// top-level collector alongside walCollector, and Registry.Collect iterates its
// registered collectors via a Go map — unordered by design. walCollector.Inc()s
// the shared counter as a side effect of its own Collect, so whether that
// increment is visible in the SAME Gather call that produced it depends on
// which of the two collectors the map happens to visit first: reading the
// count back through a full Gather is therefore genuinely racy (confirmed by
// running it in a loop — it fails roughly as often as it passes). Reading the
// counter directly via testutil.ToFloat64 right after driving the collector by
// hand sidesteps the ordering entirely: there is only one collector involved,
// called synchronously, so there is nothing left to race against.
func TestWALCollectorOmitsGaugesAndCountsErrorOnFailure(t *testing.T) {
	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "obs_collector_errors_total",
		Help: "Total scrape-time collector failures by collector.",
	}, []string{"collector"})
	c := &walCollector{
		sources: []WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 0, 0, errors.New("permission denied") }},
		},
		errors:   errs,
		bytes:    prometheus.NewDesc("obs_wal_bytes", "Total size in bytes of the WAL segment files.", []string{"wal"}, nil),
		segments: prometheus.NewDesc("obs_wal_segments", "Number of WAL segment files.", []string{"wal"}, nil),
	}

	if n := testutil.CollectAndCount(c, "obs_wal_bytes", "obs_wal_segments"); n != 0 {
		t.Errorf("wal gauges emitted %d series on error, want 0 (a gap, not a zero)", n)
	}
	if got := testutil.ToFloat64(errs.WithLabelValues("wal")); got != 1 {
		t.Errorf(`obs_collector_errors_total{collector="wal"} = %v, want 1`, got)
	}
}

// The whole scrape must survive one broken collector. promhttp is configured with
// HTTPErrorOnError, so a Gather error would 500 the endpoint and blank every panel.
func TestScrapeSucceedsWhenACollectorFails(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{s: 7},
		WALs: []WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 0, 0, errors.New("boom") }},
		},
	})
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather returned an error, which promhttp turns into a 500 for the whole scrape: %v", err)
	}
	// Unrelated metrics must still be scraped.
	want := `
# HELP obs_active_series Number of active metric series.
# TYPE obs_active_series gauge
obs_active_series 7
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "obs_active_series"); err != nil {
		t.Error(err)
	}
}

func TestLogsCollectorReportsStreamsChunksAndBytes(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{},
		Logs:        fakeLogStats{streams: 3, chunks: 5, bytes: 900},
	})
	want := `
# HELP obs_log_chunk_bytes Total on-disk size of persisted log chunk files in bytes.
# TYPE obs_log_chunk_bytes gauge
obs_log_chunk_bytes 900
# HELP obs_log_chunks_total Number of persisted log chunk files.
# TYPE obs_log_chunks_total gauge
obs_log_chunks_total 5
# HELP obs_log_streams_total Number of distinct log streams.
# TYPE obs_log_streams_total gauge
obs_log_streams_total 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"obs_log_streams_total", "obs_log_chunks_total", "obs_log_chunk_bytes"); err != nil {
		t.Error(err)
	}
}

// See TestWALCollectorOmitsGaugesAndCountsErrorOnFailure for why this drives
// logsCollector directly instead of reading obs_collector_errors_total back
// through NewRegistry's Gather: the counter is a separately-registered
// collector, and a shared Registry visits its registered collectors in
// unordered fashion, so whether logsCollector's Inc() lands before the
// counter's own Collect reads it is not guaranteed on any given scrape.
func TestLogsCollectorOmitsGaugesAndCountsErrorOnFailure(t *testing.T) {
	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "obs_collector_errors_total",
		Help: "Total scrape-time collector failures by collector.",
	}, []string{"collector"})
	c := &logsCollector{
		src:     fakeLogStats{err: errors.New("disk gone")},
		errors:  errs,
		streams: prometheus.NewDesc("obs_log_streams_total", "Number of distinct log streams.", nil, nil),
		chunks:  prometheus.NewDesc("obs_log_chunks_total", "Number of persisted log chunk files.", nil, nil),
		bytes:   prometheus.NewDesc("obs_log_chunk_bytes", "Total on-disk size of persisted log chunk files in bytes.", nil, nil),
	}

	if n := testutil.CollectAndCount(c, "obs_log_streams_total", "obs_log_chunks_total", "obs_log_chunk_bytes"); n != 0 {
		t.Errorf("log gauges emitted %d series on error, want 0", n)
	}
	if got := testutil.ToFloat64(errs.WithLabelValues("logs")); got != 1 {
		t.Errorf(`obs_collector_errors_total{collector="logs"} = %v, want 1`, got)
	}
}

// Optional sources stay optional: tests and probe modes construct a registry with
// no WAL and no log store, and must not panic or emit empty-labelled series.
func TestOptionalSourcesAreOmittedEntirely(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{Cardinality: fakeCard{}})
	for _, name := range []string{"obs_wal_bytes", "obs_wal_segments", "obs_log_streams_total"} {
		if n := testutil.CollectAndCount(reg, name); n != 0 {
			t.Errorf("%s emitted %d series with no source configured, want 0", name, n)
		}
	}
}
