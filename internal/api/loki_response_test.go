package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteLokiStreams_Shape(t *testing.T) {
	w := httptest.NewRecorder()
	writeLokiStreams(w, []lokiStreamResult{
		{Stream: map[string]string{"service": "api"}, Values: [][2]string{{"100", "hello"}}},
	})
	if w.Code != 200 {
		t.Fatalf("code = %d", w.Code)
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][2]string       `json:"values"`
			} `json:"result"`
			Stats map[string]any `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v; body=%s", err, w.Body.String())
	}
	if resp.Status != "success" || resp.Data.ResultType != "streams" {
		t.Fatalf("envelope = %+v", resp)
	}
	if len(resp.Data.Result) != 1 || resp.Data.Result[0].Values[0][0] != "100" || resp.Data.Result[0].Values[0][1] != "hello" {
		t.Fatalf("result = %+v", resp.Data.Result)
	}
}

func TestWriteLokiStreams_EmptyIsArrayNotNull(t *testing.T) {
	w := httptest.NewRecorder()
	writeLokiStreams(w, nil)
	if !strings.Contains(w.Body.String(), `"result":[]`) {
		t.Fatalf("empty result must serialize as []: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"stats":{}`) {
		t.Fatalf("stats must serialize as {}: %s", w.Body.String())
	}
}

func TestWriteLokiLabels_EmptyIsArray(t *testing.T) {
	w := httptest.NewRecorder()
	writeLokiLabels(w, nil)
	if !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Fatalf("empty labels must serialize as []: %s", w.Body.String())
	}
}

func TestWriteLokiError_PlainText(t *testing.T) {
	w := httptest.NewRecorder()
	writeLokiError(w, 400, "parse error: boom")
	if w.Code != 400 {
		t.Fatalf("code = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q, want text/plain", ct)
	}
	if w.Body.String() != "parse error: boom" {
		t.Fatalf("body = %q", w.Body.String())
	}
}

func TestWriteLokiMatrix(t *testing.T) {
	rec := httptest.NewRecorder()
	writeLokiMatrix(rec, []lokiMatrixSeries{{
		Metric: map[string]string{"level": "info"},
		Values: [][2]any{lokiSampleValue(1_700_000_000_000_000_000, 3), lokiSampleValue(1_700_000_060_500_000_000, 0)},
	}})

	if rec.Code != 200 {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`"resultType":"matrix"`,
		`"metric":{"level":"info"}`,
		`[1700000000,"3"]`,
		`[1700000060.5,"0"]`,
		`"stats":{}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s does not contain %s", body, want)
		}
	}
}

// TestWriteLokiMatrix_EmptyResult pins that no result serializes as [] rather than
// null: Grafana's Loki datasource iterates the array without a nil check.
func TestWriteLokiMatrix_EmptyResult(t *testing.T) {
	rec := httptest.NewRecorder()
	writeLokiMatrix(rec, nil)
	if !strings.Contains(rec.Body.String(), `"result":[]`) {
		t.Errorf("body %s, want an empty result array", rec.Body.String())
	}
}

// TestWriteLokiVectorSamples_Labeled proves the labeled writer carries per-series
// labels, while the label-less health-check writer is unchanged.
func TestWriteLokiVectorSamples_Labeled(t *testing.T) {
	rec := httptest.NewRecorder()
	writeLokiVectorSamples(rec, []lokiVectorSample{
		{Metric: map[string]string{"level": "error"}, Value: lokiSampleValue(1_700_000_000_000_000_000, 7)},
	})
	body := rec.Body.String()
	for _, want := range []string{`"resultType":"vector"`, `"metric":{"level":"error"}`, `[1700000000,"7"]`} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s does not contain %s", body, want)
		}
	}

	rec = httptest.NewRecorder()
	writeLokiVector(rec, 4_000_000_000_000_000_000, 2)
	if body := rec.Body.String(); !strings.Contains(body, `"metric":{}`) || !strings.Contains(body, `,"2"]`) {
		t.Errorf("health-check envelope changed: %s", body)
	}
}
