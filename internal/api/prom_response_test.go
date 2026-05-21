package api

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPromResponse_SuccessVector(t *testing.T) {
	resp := promResponse{
		Status: "success",
		Data: promVectorData{
			ResultType: "vector",
			Result: []promSample{
				{
					Metric: map[string]string{"__name__": "cpu_usage", "host": "a"},
					Value:  [2]any{1.5, "42"},
				},
			},
		},
		Warnings: []string{},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got["status"] != "success" {
		t.Errorf("status = %v, want success", got["status"])
	}
	warnings, ok := got["warnings"]
	if !ok {
		t.Fatal("warnings field missing from success response")
	}
	wList, ok := warnings.([]any)
	if !ok {
		t.Fatalf("warnings is not []any: %T", warnings)
	}
	if len(wList) != 0 {
		t.Errorf("warnings = %v, want []", wList)
	}
	data, ok := got["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is not map[string]any: %T", got["data"])
	}
	if data["resultType"] != "vector" {
		t.Errorf("resultType = %v, want vector", data["resultType"])
	}
	result, ok := data["result"].([]any)
	if !ok {
		t.Fatalf("result is not []any: %T", data["result"])
	}
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	sample, ok := result[0].(map[string]any)
	if !ok {
		t.Fatalf("sample is not map[string]any: %T", result[0])
	}
	metric, ok := sample["metric"].(map[string]any)
	if !ok {
		t.Fatalf("metric is not map[string]any: %T", sample["metric"])
	}
	if metric["__name__"] != "cpu_usage" {
		t.Errorf("__name__ = %v, want cpu_usage", metric["__name__"])
	}
	value, ok := sample["value"].([]any)
	if !ok {
		t.Fatalf("value is not []any: %T", sample["value"])
	}
	if value[0].(float64) != 1.5 {
		t.Errorf("timestamp = %v, want 1.5", value[0])
	}
	if value[1] != "42" {
		t.Errorf("value = %v, want \"42\"", value[1])
	}
}

func TestPromResponse_SuccessMatrix(t *testing.T) {
	resp := promResponse{
		Status: "success",
		Data: promMatrixData{
			ResultType: "matrix",
			Result: []promSeries{
				{
					Metric: map[string]string{"__name__": "http_requests_total"},
					Values: [][2]any{{1.0, "10"}, {2.0, "20"}},
				},
			},
		},
		Warnings: []string{},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	data, ok := got["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is not map[string]any: %T", got["data"])
	}
	if data["resultType"] != "matrix" {
		t.Errorf("resultType = %v, want matrix", data["resultType"])
	}
	result, ok := data["result"].([]any)
	if !ok {
		t.Fatalf("result is not []any: %T", data["result"])
	}
	if len(result) != 1 {
		t.Fatalf("result len = %d, want 1", len(result))
	}
	series, ok := result[0].(map[string]any)
	if !ok {
		t.Fatalf("series is not map[string]any: %T", result[0])
	}
	values, ok := series["values"].([]any)
	if !ok {
		t.Fatalf("values is not []any: %T", series["values"])
	}
	if len(values) != 2 {
		t.Fatalf("values len = %d, want 2", len(values))
	}
	pair, ok := values[0].([]any)
	if !ok {
		t.Fatalf("pair is not []any: %T", values[0])
	}
	if pair[0].(float64) != 1.0 {
		t.Errorf("ts = %v, want 1.0", pair[0])
	}
	if pair[1] != "10" {
		t.Errorf("val = %v, want \"10\"", pair[1])
	}
}

func TestPromResponse_SuccessScalar(t *testing.T) {
	resp := promResponse{
		Status: "success",
		Data: promScalarData{
			ResultType: "scalar",
			Result:     [2]any{1234567890.0, "3.14"},
		},
		Warnings: []string{},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	data, ok := got["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is not map[string]any: %T", got["data"])
	}
	if data["resultType"] != "scalar" {
		t.Errorf("resultType = %v, want scalar", data["resultType"])
	}
	result, ok := data["result"].([]any)
	if !ok {
		t.Fatalf("result is not []any: %T", data["result"])
	}
	if len(result) != 2 {
		t.Fatalf("result len = %d, want 2", len(result))
	}
	if result[0].(float64) != 1234567890.0 {
		t.Errorf("ts = %v, want 1234567890.0", result[0])
	}
	if result[1] != "3.14" {
		t.Errorf("val = %v, want \"3.14\"", result[1])
	}
}

func TestPromResponse_Error(t *testing.T) {
	resp := promResponse{
		Status:    "error",
		ErrorType: "bad_data",
		Error:     "missing required parameter 'query'",
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got["status"] != "error" {
		t.Errorf("status = %v, want error", got["status"])
	}
	if got["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", got["errorType"])
	}
	if got["error"] != "missing required parameter 'query'" {
		t.Errorf("error = %v", got["error"])
	}
	if _, ok := got["data"]; ok {
		t.Error("data field must be absent on error response")
	}
	if _, ok := got["warnings"]; ok {
		t.Error("warnings field must be absent on error response")
	}
}

func TestPromResponse_SuccessWarnings(t *testing.T) {
	resp := promResponse{
		Status: "success",
		Data: promVectorData{
			ResultType: "vector",
			Result:     []promSample{},
		},
		Warnings: []string{"partial data: replica 2 unavailable"},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got["status"] != "success" {
		t.Errorf("status = %v, want success", got["status"])
	}
	warnings, ok := got["warnings"]
	if !ok {
		t.Fatal("warnings field missing from success response")
	}
	wList, ok := warnings.([]any)
	if !ok {
		t.Fatalf("warnings is not []any: %T", warnings)
	}
	if len(wList) != 1 {
		t.Fatalf("warnings len = %d, want 1", len(wList))
	}
	if wList[0] != "partial data: replica 2 unavailable" {
		t.Errorf("warnings[0] = %v, want \"partial data: replica 2 unavailable\"", wList[0])
	}
}

func TestPromResponse_SuccessNilWarningsNormalized(t *testing.T) {
	// Directly constructed promResponse without going through writePromSuccess —
	// MarshalJSON must still produce "warnings":[] not "warnings":null.
	resp := promResponse{
		Status: "success",
		Data:   promVectorData{ResultType: "vector", Result: []promSample{}},
		// Warnings intentionally left nil
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	warnings, ok := got["warnings"]
	if !ok {
		t.Fatal("warnings field missing from success response with nil Warnings")
	}
	wList, ok := warnings.([]any)
	if !ok {
		t.Fatalf("warnings is not []any (got %T) — nil was not normalized to []", warnings)
	}
	if len(wList) != 0 {
		t.Errorf("warnings = %v, want []", wList)
	}
}

func TestWritePromError_SetsStatusCode(t *testing.T) {
	w := httptest.NewRecorder()
	writePromError(w, http.StatusBadRequest, "bad_data", "missing required parameter 'query'")

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "error" {
		t.Errorf("status = %v, want error", got["status"])
	}
	if got["errorType"] != "bad_data" {
		t.Errorf("errorType = %v, want bad_data", got["errorType"])
	}
	if got["error"] != "missing required parameter 'query'" {
		t.Errorf("error = %v, want \"missing required parameter 'query'\"", got["error"])
	}
}

func TestFormatPromValue_SpecialFloats(t *testing.T) {
	cases := []struct {
		input float64
		want  string
	}{
		{math.NaN(), "NaN"},
		{math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"},
		{1.5, "1.5"},
		{0, "0"},
		{42, "42"},
	}
	for _, tc := range cases {
		got := formatPromValue(tc.input)
		if got != tc.want {
			t.Errorf("formatPromValue(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestWritePromSuccess_SetsStatusAndWarnings(t *testing.T) {
	w := httptest.NewRecorder()
	writePromSuccess(w, promVectorData{ResultType: "vector", Result: []promSample{}})

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "success" {
		t.Errorf("status = %v, want success", got["status"])
	}
	warnings, ok := got["warnings"]
	if !ok {
		t.Fatal("warnings field missing")
	}
	wList, ok := warnings.([]any)
	if !ok {
		t.Fatalf("warnings is not []any: %T", warnings)
	}
	if len(wList) != 0 {
		t.Errorf("warnings = %v, want []", wList)
	}
}
