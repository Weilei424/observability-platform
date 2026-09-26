package rpc

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func TestWireSampleRoundTripsEveryFloat(t *testing.T) {
	for _, v := range []float64{0, -0.5, 1e-300, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1), 0.1 + 0.2} {
		in := wireSample{T: -1_000, V: v, G: 1<<62 - 7}
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal %v: %v", v, err)
		}
		var out wireSample
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		same := out.V == v || (math.IsNaN(out.V) && math.IsNaN(v))
		if out.T != in.T || out.G != in.G || !same || math.Signbit(out.V) != math.Signbit(v) && !math.IsNaN(v) {
			t.Fatalf("%s decoded to %+v, want %+v", raw, out, in)
		}
	}
}

func TestWireSampleRejectsMalformedInput(t *testing.T) {
	for _, raw := range []string{`[1,"2"]`, `[1,2,3]`, `[1,"x",3]`, `{"t":1}`, `[1,"2",3.5]`} {
		var s wireSample
		if err := json.Unmarshal([]byte(raw), &s); err == nil {
			t.Errorf("%s decoded without error", raw)
		}
	}
}

func TestWireEntryRoundTripsAnyUTF8Line(t *testing.T) {
	for _, line := range []string{"", `quote " and \ backslash`, "tab\tnewline\n", "日本語 🚀", "<html>&amp;"} {
		raw, err := json.Marshal(wireEntry{T: 1758600000000000000, Line: line})
		if err != nil {
			t.Fatal(err)
		}
		var out wireEntry
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		if out.Line != line || out.T != 1758600000000000000 {
			t.Fatalf("%q decoded to %+v", line, out)
		}
	}
}

func TestSeriesSurviveTheWire(t *testing.T) {
	l, err := metrics.NewLabels(map[string]string{"__name__": "m", "city": "東京"})
	if err != nil {
		t.Fatal(err)
	}
	id := metrics.SeriesID(l.Hash())
	in := []metrics.SeriesData{{
		Labels:  l,
		Anchor:  &metrics.Sample{SeriesID: id, TimestampMs: 1, Value: math.Inf(-1), Gen: 3},
		Samples: []metrics.Sample{{SeriesID: id, TimestampMs: 5, Value: 2.5, Gen: 1<<62 - 1}},
	}}
	raw, err := json.Marshal(metricsSelectResponse{Series: seriesToWire(in)})
	if err != nil {
		t.Fatal(err)
	}
	var resp metricsSelectResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	out, err := seriesFromWire(resp.Series)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Labels.Hash() != l.Hash() || *out[0].Anchor != *in[0].Anchor || out[0].Samples[0] != in[0].Samples[0] {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

func TestStreamsSurviveTheWire(t *testing.T) {
	l, _ := logs.NewStreamLabels(map[string]string{"service": "api", "note": "ünïcode"})
	in := []logs.StreamData{{Labels: l, Entries: []logs.LogEntry{{StreamID: logs.StreamIDOf(l), TimestampNs: 42, Line: "x\ny"}}}}
	raw, err := json.Marshal(logsSelectResponse{Streams: streamsToWire(in)})
	if err != nil {
		t.Fatal(err)
	}
	var resp logsSelectResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	out, err := streamsFromWire(resp.Streams)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Labels.Hash() != l.Hash() || out[0].Entries[0] != in[0].Entries[0] {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

func TestSelectorFoldsTheMetricName(t *testing.T) {
	sel := metrics.Selector{MetricName: "m", Matchers: []metrics.Matcher{{Name: "job", Value: "a"}}}
	back, err := selectorFromWire(selectorToWire(sel))
	if err != nil {
		t.Fatal(err)
	}
	if back.MetricName != "" || len(back.Matchers) != 2 || back.Matchers[0].Name != "__name__" {
		t.Fatalf("selector = %+v, want __name__ folded into the matchers", back)
	}
	if _, err := selectorFromWire([]wireMatcher{{Name: "", Value: "x"}}); err == nil || !strings.Contains(err.Error(), "empty label name") {
		t.Fatalf("an empty matcher name must be refused, got %v", err)
	}
}

// TestWireEncodingIsNeverHTMLEscaped pins the controller ruling: the ingester
// sizes its log-flush batches assuming the JSON encoding never HTML-escapes
// '<', '>', '&'. Two encoding paths must honor that — marshalJSON (the one
// encoding path for internal request bodies, and what wireEntry.MarshalJSON
// uses for its line) and writeJSON (the server's response path) — and neither
// can rely on the other to undo escaping already baked into nested bytes.
func TestWireEncodingIsNeverHTMLEscaped(t *testing.T) {
	// The escape sequences below are built from a rune value rather than typed
	// directly as source text, because a JSON-style backslash-u escape typed
	// literally in source would itself just compile down to the escaped rune,
	// not the six literal characters this test needs to search for.
	bs := string(rune(0x5C))
	escaped := []string{bs + "u003c", bs + "u003e", bs + "u0026"}
	assertLiteral := func(t *testing.T, label string, raw []byte) {
		t.Helper()
		for _, esc := range escaped {
			if bytes.Contains(raw, []byte(esc)) {
				t.Fatalf("%s: %s HTML-escaped, got %s", label, esc, raw)
			}
		}
		for _, lit := range []string{"<", ">", "&"} {
			if !bytes.Contains(raw, []byte(lit)) {
				t.Fatalf("%s: missing literal %q, got %s", label, lit, raw)
			}
		}
	}

	entry := wireEntry{T: 7, Line: "a<b>&c"}
	l, err := metrics.NewLabels(map[string]string{"__name__": "m", "note": "<&>"})
	if err != nil {
		t.Fatal(err)
	}
	resp := metricsSelectResponse{Series: seriesToWire([]metrics.SeriesData{{Labels: l}})}

	// marshalJSON: the codec's one encoding path for request bodies (and what a
	// custom MarshalJSON, like wireEntry's, must route through).
	rawEntry, err := marshalJSON(entry)
	if err != nil {
		t.Fatal(err)
	}
	assertLiteral(t, "marshalJSON(wireEntry)", rawEntry)
	var outEntry wireEntry
	if err := json.Unmarshal(rawEntry, &outEntry); err != nil {
		t.Fatal(err)
	}
	if outEntry != entry {
		t.Fatalf("entry round trip = %+v, want %+v", outEntry, entry)
	}

	rawSeries, err := marshalJSON(resp)
	if err != nil {
		t.Fatal(err)
	}
	assertLiteral(t, "marshalJSON(metricsSelectResponse)", rawSeries)
	var outResp metricsSelectResponse
	if err := json.Unmarshal(rawSeries, &outResp); err != nil {
		t.Fatal(err)
	}
	backSeries, err := seriesFromWire(outResp.Series)
	if err != nil {
		t.Fatal(err)
	}
	if len(backSeries) != 1 || backSeries[0].Labels.Hash() != l.Hash() {
		t.Fatalf("series round trip = %+v, want label %q preserved", backSeries, "<&>")
	}

	// writeJSON: the server's own response path must not escape either — an
	// outer SetEscapeHTML(false) cannot undo escaping a nested MarshalJSON
	// already did, so this is a separate assertion, not a consequence of the
	// one above.
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, resp)
	assertLiteral(t, "writeJSON", rec.Body.Bytes())
}
