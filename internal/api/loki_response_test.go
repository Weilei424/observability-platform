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
