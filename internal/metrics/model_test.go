package metrics_test

import (
	"errors"
	"math"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// --- Series identity ---

func TestLabels_SameLabels_DifferentOrder_SameFingerprint(t *testing.T) {
	a, err := metrics.NewLabels(map[string]string{
		"__name__": "http_requests",
		"service":  "api",
		"env":      "prod",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, err := metrics.NewLabels(map[string]string{
		"env":      "prod",
		"__name__": "http_requests",
		"service":  "api",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Errorf("expected same fingerprint, got %d vs %d", a.Fingerprint(), b.Fingerprint())
	}
}

func TestLabels_DifferentValues_DifferentFingerprint(t *testing.T) {
	a, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api"})
	b, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "web"})
	if a.Fingerprint() == b.Fingerprint() {
		t.Error("expected different fingerprints for different label values")
	}
}

func TestLabels_DifferentNames_DifferentFingerprint(t *testing.T) {
	a, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api"})
	b, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "env": "api"})
	if a.Fingerprint() == b.Fingerprint() {
		t.Error("expected different fingerprints for different label names")
	}
}

func TestLabels_ExtraLabel_DifferentFingerprint(t *testing.T) {
	a, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api"})
	b, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api", "env": "prod"})
	if a.Fingerprint() == b.Fingerprint() {
		t.Error("expected different fingerprints when extra label is added")
	}
}

// --- __name__ validation ---

func TestLabels_MissingName_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"service": "api"})
	if err == nil {
		t.Fatal("expected error for missing __name__")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_InvalidMetricName_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "123invalid"})
	if err == nil {
		t.Fatal("expected error for invalid metric name")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_EmptyMetricName_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": ""})
	if err == nil {
		t.Fatal("expected error for empty metric name")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_ValidNameOnly_Accepted(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests"})
	if err != nil {
		t.Errorf("unexpected error for label set with only __name__: %v", err)
	}
}

// --- Label name validation ---

func TestLabels_EmptyLabelName_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "": "value"})
	if err == nil {
		t.Fatal("expected error for empty label name")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_InvalidLabelName_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "123bad": "value"})
	if err == nil {
		t.Fatal("expected error for label name starting with a digit")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_LabelNameWithHyphen_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "bad-name": "value"})
	if err == nil {
		t.Fatal("expected error for label name containing a hyphen")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestLabels_ReservedDoubleUnderscorePrefix_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "__job__": "scraper"})
	if err == nil {
		t.Fatal("expected error for label name with __ prefix")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

// --- Label value validation ---

func TestLabels_EmptyLabelValue_Accepted(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": ""})
	if err != nil {
		t.Errorf("unexpected error for empty label value: %v", err)
	}
}

func TestLabels_InvalidUTF8Value_Error(t *testing.T) {
	_, err := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "\xff\xfe"})
	if err == nil {
		t.Fatal("expected error for invalid UTF-8 label value")
	}
	var ve *metrics.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T: %v", err, err)
	}
}

// --- Fingerprint stability ---

func TestLabels_Fingerprint_Stable(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{
		"__name__": "http_requests",
		"service":  "api",
	})
	// Computed from FNV-1a 64-bit with length-prefixed binary encoding.
	// If this fails, the fingerprinting algorithm changed — a breaking change for
	// any persisted SeriesIDs (WAL, blocks, indexes).
	const want metrics.SeriesID = 9696857623413696903
	if got := l.Fingerprint(); got != want {
		t.Errorf("fingerprint = %d, want %d — algorithm changed, update persisted data", got, want)
	}
}

// --- Labels.Get ---

func TestLabels_Get_ExistingLabel(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api"})
	v, ok := l.Get("service")
	if !ok {
		t.Fatal("expected Get to find \"service\"")
	}
	if v != "api" {
		t.Errorf("expected \"api\", got %q", v)
	}
}

func TestLabels_Get_MissingLabel(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests"})
	_, ok := l.Get("missing")
	if ok {
		t.Error("expected Get to return false for missing label")
	}
}

func TestLabels_Get_Name(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests"})
	v, ok := l.Get("__name__")
	if !ok || v != "http_requests" {
		t.Errorf("expected (\"http_requests\", true), got (%q, %v)", v, ok)
	}
}

// --- Labels.Map ---

func TestLabels_Map_RoundTrip(t *testing.T) {
	input := map[string]string{"__name__": "http_requests", "service": "api", "env": "prod"}
	l, _ := metrics.NewLabels(input)
	m := l.Map()
	if len(m) != len(input) {
		t.Fatalf("expected %d labels, got %d", len(input), len(m))
	}
	for k, v := range input {
		if m[k] != v {
			t.Errorf("expected %s=%q, got %q", k, v, m[k])
		}
	}
}

func TestLabels_Map_IsCopy(t *testing.T) {
	l, _ := metrics.NewLabels(map[string]string{"__name__": "http_requests", "service": "api"})
	m := l.Map()
	m["service"] = "mutated"
	v, _ := l.Get("service")
	if v != "api" {
		t.Error("Map() must return a copy — mutating it must not affect Labels")
	}
}

// --- ValidateSample ---

func TestValidateSample_NaN_Accepted(t *testing.T) {
	s := metrics.Sample{TimestampMs: 1000, Value: math.NaN()}
	if err := metrics.ValidateSample(s); err != nil {
		t.Errorf("unexpected error for NaN value: %v", err)
	}
}

func TestValidateSample_PosInf_Accepted(t *testing.T) {
	s := metrics.Sample{TimestampMs: 1000, Value: math.Inf(1)}
	if err := metrics.ValidateSample(s); err != nil {
		t.Errorf("unexpected error for +Inf value: %v", err)
	}
}

func TestValidateSample_NegInf_Accepted(t *testing.T) {
	s := metrics.Sample{TimestampMs: 1000, Value: math.Inf(-1)}
	if err := metrics.ValidateSample(s); err != nil {
		t.Errorf("unexpected error for -Inf value: %v", err)
	}
}

func TestValidateSample_ValidTimestamp_Accepted(t *testing.T) {
	s := metrics.Sample{TimestampMs: 1715865600000, Value: 42.0}
	if err := metrics.ValidateSample(s); err != nil {
		t.Errorf("unexpected error for valid sample: %v", err)
	}
}
