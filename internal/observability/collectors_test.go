package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// discardLog silences the collectors' failure lines in tests that fail a read on
// purpose to check the metrics, not the logging.
var discardLog = slog.New(slog.NewTextHandler(io.Discard, nil))

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
	c := newWALCollector([]WALSource{
		{Name: "metrics", Stats: func() (int64, int, error) { return 0, 0, errors.New("permission denied") }},
	}, errs, discardLog)

	if n := testutil.CollectAndCount(c, "obs_wal_bytes", "obs_wal_segments"); n != 0 {
		t.Errorf("wal gauges emitted %d series on error, want 0 (a gap, not a zero)", n)
	}
	if got := testutil.ToFloat64(errs.WithLabelValues("wal")); got != 1 {
		t.Errorf(`obs_collector_errors_total{collector="wal"} = %v, want 1`, got)
	}
}

// The whole scrape must survive one broken collector. promhttp is configured with
// HTTPErrorOnError, so a Gather error would 500 the endpoint and blank every panel.
//
// This also configures a failing Logs source alongside the failing WAL source, so
// this test is the one place obs_collector_errors_total is read back through the
// real NewRegistry rather than a locally constructed collector (see the two
// "OmitsGaugesAndCountsErrorOnFailure" tests above): NewRegistry.go wires the same
// *prometheus.CounterVec into both walCollector and logsCollector, and neither
// gauge-omission test exercises that wiring, since each builds its own collector
// directly and hands it its own local counter. A nil errors field here would panic
// inside a Gather worker instead of failing a test, so this is worth pinning
// end-to-end even though it duplicates part of what the two tests above already
// check in isolation.
//
// This asserts identity only (name, help, type, label name, value >= 1) rather
// than an exact accumulated count. Registry.Gather has no barrier between one
// collector's Collect and the Write of an already-collected metric: Gather runs a
// pool of collectWorker goroutines that call each registered collector's Collect
// concurrently, while Gather's own main loop drains the metric channel and calls
// processMetric (which calls Write to read the value) as metrics arrive — see
// client_golang's registry.go, the collectWorker/processMetric loop starting
// around line 452. So collectorErrors.Collect can send an already-registered
// child for Write to read while a *different, concurrently running* worker
// goroutine is still in the middle of walCollector's or logsCollector's own Inc on
// that same child, for this very Gather call. An earlier version of this test
// asserted an exact count reached after a fixed number of Gather calls, reasoning
// that Collect-then-Write was two strictly sequential phases; that is wrong, and
// the exact count raced roughly 20-21 times per 3000 runs under -race. A warm-up
// Gather (below) still matters: it guarantees the {collector="wal"} and
// {collector="logs"} children exist before the assertion, which avoids the
// separate, much higher-probability (~70%) race where the family is entirely
// absent from a virgin registry's first-ever Gather (the same race the two
// "OmitsGauges" tests avoid by driving their collector directly instead of
// through a registry). Once a child exists, presence is guaranteed on every later
// Gather; only its exact value is unsafe to pin here. The exact increment count
// is already covered deterministically by the two direct-drive
// "OmitsGaugesAndCountsErrorOnFailure" tests, which call Collect synchronously in
// the test's own goroutine with no registry worker pool involved, so nothing is
// lost by asserting only identity and >= 1 here.
func TestScrapeSucceedsWhenACollectorFails(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{s: 7},
		WALs: []WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 0, 0, errors.New("boom") }},
		},
		Logs: fakeLogStats{err: errors.New("disk gone")},
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

	// Pins obs_collector_errors_total's name, help text, type, and label name
	// through the real registry (metric-shape drift here would otherwise be
	// invisible to the suite, since the two tests above each use a local
	// duplicate CounterVec literal) and confirms NewRegistry wired the same
	// counter into logsCollector, not just walCollector — a nil there panics
	// inside a Gather worker instead of failing a test.
	const (
		wantName = "obs_collector_errors_total"
		wantHelp = "Total scrape-time collector failures by collector."
	)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var family *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == wantName {
			family = mf
			break
		}
	}
	if family == nil {
		t.Fatalf("%s: family not found", wantName)
	}
	if got := family.GetHelp(); got != wantHelp {
		t.Errorf("%s help = %q, want %q", wantName, got, wantHelp)
	}
	if got := family.GetType(); got != dto.MetricType_COUNTER {
		t.Errorf("%s type = %v, want COUNTER", wantName, got)
	}
	if len(family.GetMetric()) == 0 {
		t.Fatalf("%s: no metrics in family", wantName)
	}
	for _, m := range family.GetMetric() {
		hasCollectorLabel := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "collector" {
				hasCollectorLabel = true
			}
		}
		if !hasCollectorLabel {
			t.Errorf("%s%v: missing label \"collector\"", wantName, m.GetLabel())
		}
		if got := m.GetCounter().GetValue(); got < 1 {
			t.Errorf("%s%v = %v, want >= 1", wantName, m.GetLabel(), got)
		}
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
	c := newLogsCollector(fakeLogStats{err: errors.New("disk gone")}, errs, discardLog)

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

// toggleSource is a WAL source whose failure can be switched between scrapes.
type toggleSource struct{ fail atomic.Bool }

func (ts *toggleSource) Stats() (int64, int, error) {
	if ts.fail.Load() {
		return 0, 0, errors.New("open /data/metrics/wal: permission denied")
	}
	return 128, 1, nil
}

// syncBuffer lets concurrent scrapes write log lines without racing each other
// on the buffer itself, so the race test below measures the collector only.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func gather(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather: %v", err)
	}
}

// The runbook sends operators to the backend logs for the exact error when
// obs_collector_errors_total climbs. Before this, the collectors discarded the
// error entirely and no such line existed. This pins the whole lifecycle: one
// line naming the source and error when a read starts failing, silence while it
// keeps failing, and one line when it recovers.
func TestCollectorLogsReadFailureOnceAndRecoveryOnce(t *testing.T) {
	var buf syncBuffer
	src := &toggleSource{}
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{},
		WALs:        []WALSource{{Name: "metrics", Stats: src.Stats}},
		Logger:      slog.New(slog.NewJSONHandler(&buf, nil)),
	})

	gather(t, reg) // healthy: nothing to say
	if n := len(buf.lines(t)); n != 0 {
		t.Fatalf("healthy scrape logged %d lines, want 0", n)
	}

	src.fail.Store(true)
	gather(t, reg)
	lines := buf.lines(t)
	if len(lines) != 1 {
		t.Fatalf("first failing scrape logged %d lines, want exactly 1", len(lines))
	}
	l := lines[0]
	if l["component"] != CollectorWAL {
		t.Errorf("component = %v, want %q (matching obs_collector_errors_total's label)", l["component"], CollectorWAL)
	}
	if l["source"] != "metrics" {
		t.Errorf("source = %v, want \"metrics\" — it is what tells the two WALs apart", l["source"])
	}
	if e, _ := l["error"].(string); !strings.Contains(e, "permission denied") {
		t.Errorf("error = %v, want the underlying read error so the runbook's diagnosis step works", l["error"])
	}

	// Still failing: the counter keeps climbing, but no new line.
	gather(t, reg)
	gather(t, reg)
	if n := len(buf.lines(t)); n != 1 {
		t.Errorf("after three failing scrapes there are %d lines, want still 1 — a persistent failure must not repeat every scrape", n)
	}

	// Recover, and read the counter from THIS scrape. It must not be read from a
	// failing one: Gather has no barrier between a collector's Collect and the
	// Write of an already-collected metric, so a scrape that increments the
	// counter can report it either before or after its own increment. A healthy
	// scrape increments nothing, so the three prior failures read exactly 3.
	src.fail.Store(false)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if got := counterValue(families, "obs_collector_errors_total", "collector", CollectorWAL); got == nil || *got != 3 {
		t.Errorf("obs_collector_errors_total{collector=wal} = %v, want 3 — the counter must still count every failed scrape", got)
	}
	lines = buf.lines(t)
	if len(lines) != 2 {
		t.Fatalf("after recovery there are %d lines, want 2", len(lines))
	}
	if msg, _ := lines[1]["msg"].(string); !strings.Contains(msg, "recovered") || lines[1]["source"] != "metrics" {
		t.Errorf("recovery line = %v, want a recovered message for source metrics", lines[1])
	}
}

// Both WALs report under collector="wal", so the counter alone cannot say which
// one broke. The log line must, and must not blame the healthy one.
func TestCollectorLogsOnlyTheFailingWALSource(t *testing.T) {
	var buf syncBuffer
	broken := &toggleSource{}
	broken.fail.Store(true)
	healthy := &toggleSource{}
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{},
		WALs: []WALSource{
			{Name: "metrics", Stats: healthy.Stats},
			{Name: "logs", Stats: broken.Stats},
		},
		Logger: slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	gather(t, reg)
	lines := buf.lines(t)
	if len(lines) != 1 || lines[0]["source"] != "logs" {
		t.Fatalf("lines = %v, want exactly one, for source logs", lines)
	}
}

func TestLogsCollectorLogsFailureWithItsOwnComponent(t *testing.T) {
	var buf syncBuffer
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{},
		Logs:        fakeLogStats{err: errors.New("readdir /data/logs/chunks: no such file or directory")},
		Logger:      slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	gather(t, reg)
	lines := buf.lines(t)
	if len(lines) != 1 {
		t.Fatalf("logged %d lines, want 1", len(lines))
	}
	if lines[0]["component"] != CollectorLogs {
		t.Errorf("component = %v, want %q", lines[0]["component"], CollectorLogs)
	}
	if n := strings.Count(func() string { b, _ := json.Marshal(lines[0]); return string(b) }(), `"component"`); n != 1 {
		t.Errorf("line carries %d component keys, want exactly 1", n)
	}
}

// Prometheus may scrape while a previous scrape is still in flight, so a newly
// broken source can be read by several scrapes at once.
//
// What this test catches, verified by mutation: a missing transition guard (32
// lines instead of 1), a missing log line, and — under -race — a plain bool in
// place of atomic.Bool, which -race reports as a data race.
//
// What it does NOT catch, and cannot: an atomic Load followed by a separate
// Store. That check-then-set race is real, but its window is two instructions
// wide and the slow log call comes after the Store, so it could not be observed
// even with 256 goroutines released simultaneously over 600 rounds. That
// reportReadState uses Swap — one atomic read-modify-write — is therefore the
// guarantee, established by reading the code, not by this test. Keep Swap.
func TestConcurrentScrapesLogAFailureExactlyOnce(t *testing.T) {
	var buf syncBuffer
	src := &toggleSource{}
	src.fail.Store(true)
	reg, _ := NewRegistry(RegistryOptions{
		Cardinality: fakeCard{},
		WALs:        []WALSource{{Name: "metrics", Stats: src.Stats}},
		Logger:      slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reg.Gather()
		}()
	}
	wg.Wait()
	if n := len(buf.lines(t)); n != 1 {
		t.Errorf("32 concurrent scrapes of a failing source logged %d lines, want exactly 1", n)
	}
}
