// Package rpc is the internal HTTP API the split components use to talk to one
// another, and the clients that call it. Every route lives under /internal/v1
// on the component's own port. The gateway never proxies it, Compose never
// publishes it, and Kubernetes exposes it only on ClusterIP Services.
//
// Encoding is JSON. Timestamps and generations are integers. Sample values are
// strings, formatted with strconv.FormatFloat(v, 'g', -1, 64): ingest accepts
// NaN and ±Inf, which JSON numbers cannot carry, and it is Prometheus' own wire
// convention. Flushed chunks travel in their persisted encoding — base64 in
// JSON — so the store validates them exactly as it validates a chunk read from
// disk. Every internal body is written through marshalJSON: encoding/json's
// default HTML escaping would turn '<', '>', '&' in a log line or label value
// into a six-byte backslash-u escape each, which the ingester's flush-batch
// sizing does not account for.
//
// /internal/v1 carries no cross-version promise: a split deployment runs one
// binary version, and a protocol break becomes /internal/v2.
package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// marshalJSON is the one encoding path for every internal API body: it is what
// this package's future client (Task 18) must use for every request it sends,
// and it is what wireEntry.MarshalJSON routes its line through below. It turns
// off HTML escaping and trims the trailing newline json.Encoder appends.
//
// This has to be the only place that encodes internal bodies, not just the
// default at the outermost call: once a nested MarshalJSON (like wireEntry's)
// has already escaped a value into a backslash-u sequence, no outer encoder
// setting can undo it — the outer encoder only escapes literal '<'/'>'/'&'
// bytes it finds, it never unescapes a sequence that is already escaped. So
// escaping has to be disabled at every level that hand-assembles JSON, not
// just at the top.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

type wireMatcher struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// wireSample is [timestamp_ms, "value", generation].
type wireSample struct {
	T int64
	V float64
	G int64
}

func (s wireSample) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 48)
	b = append(b, '[')
	b = strconv.AppendInt(b, s.T, 10)
	b = append(b, ',', '"')
	b = strconv.AppendFloat(b, s.V, 'g', -1, 64)
	b = append(b, '"', ',')
	b = strconv.AppendInt(b, s.G, 10)
	return append(b, ']'), nil
}

func (s *wireSample) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("rpc: sample: %w", err)
	}
	if len(raw) != 3 {
		return fmt.Errorf("rpc: sample must be [timestamp, value, generation], got %d elements", len(raw))
	}
	if err := json.Unmarshal(raw[0], &s.T); err != nil {
		return fmt.Errorf("rpc: sample timestamp: %w", err)
	}
	var v string
	if err := json.Unmarshal(raw[1], &v); err != nil {
		return fmt.Errorf("rpc: sample value: %w", err)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fmt.Errorf("rpc: sample value %q: %w", v, err)
	}
	s.V = f
	if err := json.Unmarshal(raw[2], &s.G); err != nil {
		return fmt.Errorf("rpc: sample generation: %w", err)
	}
	return nil
}

// wireEntry is [timestamp_ns, "line"].
type wireEntry struct {
	T    int64
	Line string
}

func (e wireEntry) MarshalJSON() ([]byte, error) {
	// The line is routed through marshalJSON, not json.Marshal: a default
	// json.Marshal call HTML-escapes, and once that has happened here, no
	// setting on whatever encoder later embeds these bytes can undo it.
	line, err := marshalJSON(e.Line)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, len(line)+24)
	b = append(b, '[')
	b = strconv.AppendInt(b, e.T, 10)
	b = append(b, ',')
	b = append(b, line...)
	return append(b, ']'), nil
}

func (e *wireEntry) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("rpc: entry: %w", err)
	}
	if len(raw) != 2 {
		return fmt.Errorf("rpc: entry must be [timestamp, line], got %d elements", len(raw))
	}
	if err := json.Unmarshal(raw[0], &e.T); err != nil {
		return fmt.Errorf("rpc: entry timestamp: %w", err)
	}
	if err := json.Unmarshal(raw[1], &e.Line); err != nil {
		return fmt.Errorf("rpc: entry line: %w", err)
	}
	return nil
}

type wireSeries struct {
	Labels  map[string]string `json:"labels"`
	Anchor  *wireSample       `json:"anchor,omitempty"`
	Samples []wireSample      `json:"samples,omitempty"`
}

type wireStream struct {
	Labels  map[string]string `json:"labels"`
	Entries []wireEntry       `json:"entries"`
}

// wireChunkSeries carries a series' sealed chunks in their persisted encoding;
// encoding/json writes []byte as base64.
type wireChunkSeries struct {
	Labels map[string]string `json:"labels"`
	Chunks [][]byte          `json:"chunks"`
}

type wireBlockInfo struct {
	ID        string `json:"id"`
	Level     int    `json:"level"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
	SizeBytes int64  `json:"size_bytes"`
}

type metricsSelectRequest struct {
	Matchers   []wireMatcher `json:"matchers"`
	MinMs      int64         `json:"min_ms"`
	MaxMs      int64         `json:"max_ms"`
	Anchor     bool          `json:"anchor,omitempty"`
	SeriesOnly bool          `json:"series_only,omitempty"`
	AnyTime    bool          `json:"any_time,omitempty"`
}

type metricsSelectResponse struct {
	Series []wireSeries `json:"series"`
}

type logsSelectRequest struct {
	Matchers []wireMatcher `json:"matchers"`
	MinNs    int64         `json:"min_ns"`
	MaxNs    int64         `json:"max_ns"`
}

type logsSelectResponse struct {
	Streams []wireStream `json:"streams"`
}

type namesResponse struct {
	Names []string `json:"names"`
}

type valuesResponse struct {
	Values []string `json:"values"`
}

type metricsFlushRequest struct {
	Series []wireChunkSeries `json:"series"`
}

type metricsFlushResponse struct {
	BlockID string `json:"block_id"`
	Series  int    `json:"series"`
	Samples int    `json:"samples"`
}

type logsFlushRequest struct {
	Streams []wireStream `json:"streams"`
}

type logsFlushResponse struct {
	Streams int `json:"streams"`
	Entries int `json:"entries"`
}

type blocksResponse struct {
	Blocks []wireBlockInfo `json:"blocks"`
}

type compactRequest struct {
	Groups [][]string `json:"groups"`
}

// compactResponse carries the count even on failure: compaction reports how
// many groups it finished before the error.
type compactResponse struct {
	Compacted int    `json:"compacted"`
	Error     string `json:"error,omitempty"`
}

type retentionRequest struct {
	NowMs       int64 `json:"now_ms"`
	RetentionMs int64 `json:"retention_ms"`
}

// retentionResponse carries the count even on failure: retention reports how
// many blocks it reclaimed before the error.
type retentionResponse struct {
	Deleted int    `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

// selectorToWire folds the metric name into a __name__ matcher, as the index
// does, so the receiving side needs no metric-name field.
func selectorToWire(sel metrics.Selector) []wireMatcher {
	out := make([]wireMatcher, 0, len(sel.Matchers)+1)
	if sel.MetricName != "" {
		out = append(out, wireMatcher{Name: "__name__", Value: sel.MetricName})
	}
	for _, m := range sel.Matchers {
		out = append(out, wireMatcher{Name: m.Name, Value: m.Value})
	}
	return out
}

func selectorFromWire(ms []wireMatcher) (metrics.Selector, error) {
	sel := metrics.Selector{Matchers: make([]metrics.Matcher, 0, len(ms))}
	for _, m := range ms {
		if m.Name == "" {
			return metrics.Selector{}, fmt.Errorf("rpc: matcher with an empty label name")
		}
		sel.Matchers = append(sel.Matchers, metrics.Matcher{Name: m.Name, Value: m.Value})
	}
	return sel, nil
}

func pairsToWire(ps []index.Pair) []wireMatcher {
	out := make([]wireMatcher, len(ps))
	for i, p := range ps {
		out[i] = wireMatcher{Name: p.Name, Value: p.Value}
	}
	return out
}

func pairsFromWire(ms []wireMatcher) ([]index.Pair, error) {
	out := make([]index.Pair, len(ms))
	for i, m := range ms {
		if m.Name == "" {
			return nil, fmt.Errorf("rpc: matcher with an empty label name")
		}
		out[i] = index.Pair{Name: m.Name, Value: m.Value}
	}
	return out, nil
}

func seriesToWire(sds []metrics.SeriesData) []wireSeries {
	out := make([]wireSeries, len(sds))
	for i, sd := range sds {
		ws := wireSeries{Labels: sd.Labels.Map()}
		if sd.Anchor != nil {
			ws.Anchor = &wireSample{T: sd.Anchor.TimestampMs, V: sd.Anchor.Value, G: sd.Anchor.Gen}
		}
		if len(sd.Samples) > 0 {
			ws.Samples = make([]wireSample, len(sd.Samples))
			for j, s := range sd.Samples {
				ws.Samples[j] = wireSample{T: s.TimestampMs, V: s.Value, G: s.Gen}
			}
		}
		out[i] = ws
	}
	return out
}

func seriesFromWire(ws []wireSeries) ([]metrics.SeriesData, error) {
	out := make([]metrics.SeriesData, len(ws))
	for i, w := range ws {
		l, err := metrics.NewLabels(w.Labels)
		if err != nil {
			return nil, fmt.Errorf("rpc: series labels: %w", err)
		}
		id := metrics.SeriesID(l.Hash())
		sd := metrics.SeriesData{Labels: l}
		if w.Anchor != nil {
			sd.Anchor = &metrics.Sample{SeriesID: id, TimestampMs: w.Anchor.T, Value: w.Anchor.V, Gen: w.Anchor.G}
		}
		if len(w.Samples) > 0 {
			sd.Samples = make([]metrics.Sample, len(w.Samples))
			for j, s := range w.Samples {
				sd.Samples[j] = metrics.Sample{SeriesID: id, TimestampMs: s.T, Value: s.V, Gen: s.G}
			}
		}
		out[i] = sd
	}
	return out, nil
}

func streamsToWire(sds []logs.StreamData) []wireStream {
	out := make([]wireStream, len(sds))
	for i, sd := range sds {
		ws := wireStream{Labels: sd.Labels.Map(), Entries: make([]wireEntry, len(sd.Entries))}
		for j, e := range sd.Entries {
			ws.Entries[j] = wireEntry{T: e.TimestampNs, Line: e.Line}
		}
		out[i] = ws
	}
	return out
}

func streamsFromWire(ws []wireStream) ([]logs.StreamData, error) {
	out := make([]logs.StreamData, len(ws))
	for i, w := range ws {
		l, err := logs.NewStreamLabels(w.Labels)
		if err != nil {
			return nil, fmt.Errorf("rpc: stream labels: %w", err)
		}
		id := logs.StreamIDOf(l)
		sd := logs.StreamData{Labels: l, Entries: make([]logs.LogEntry, len(w.Entries))}
		for j, e := range w.Entries {
			sd.Entries[j] = logs.LogEntry{StreamID: id, TimestampNs: e.T, Line: e.Line}
		}
		out[i] = sd
	}
	return out, nil
}
