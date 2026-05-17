package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func TestParseSelector_BareMetricName(t *testing.T) {
	sel, err := metrics.ParseSelector("http_requests_total")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sel.MetricName != "http_requests_total" {
		t.Errorf("MetricName = %q, want %q", sel.MetricName, "http_requests_total")
	}
	if len(sel.Matchers) != 0 {
		t.Errorf("Matchers = %v, want empty", sel.Matchers)
	}
}

func TestParseSelector_MetricNameWithSingleMatcher(t *testing.T) {
	sel, err := metrics.ParseSelector(`http_requests_total{service="api"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sel.MetricName != "http_requests_total" {
		t.Errorf("MetricName = %q, want %q", sel.MetricName, "http_requests_total")
	}
	if len(sel.Matchers) != 1 {
		t.Fatalf("len(Matchers) = %d, want 1", len(sel.Matchers))
	}
	if sel.Matchers[0].Name != "service" || sel.Matchers[0].Value != "api" {
		t.Errorf("Matchers[0] = %+v, want {Name:service Value:api}", sel.Matchers[0])
	}
}

func TestParseSelector_MetricNameWithMultipleMatchers(t *testing.T) {
	sel, err := metrics.ParseSelector(`http_requests_total{service="api", env="prod"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sel.MetricName != "http_requests_total" {
		t.Errorf("MetricName = %q, want %q", sel.MetricName, "http_requests_total")
	}
	if len(sel.Matchers) != 2 {
		t.Fatalf("len(Matchers) = %d, want 2", len(sel.Matchers))
	}
	wantMatchers := []struct{ name, value string }{
		{"service", "api"},
		{"env", "prod"},
	}
	for i, want := range wantMatchers {
		if sel.Matchers[i].Name != want.name || sel.Matchers[i].Value != want.value {
			t.Errorf("Matchers[%d] = %+v, want {Name:%s Value:%s}", i, sel.Matchers[i], want.name, want.value)
		}
	}
}

func TestParseSelector_BracesOnlyNoMetricName(t *testing.T) {
	sel, err := metrics.ParseSelector(`{service="api"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sel.MetricName != "" {
		t.Errorf("MetricName = %q, want empty", sel.MetricName)
	}
	if len(sel.Matchers) != 1 || sel.Matchers[0].Name != "service" {
		t.Errorf("unexpected matchers: %v", sel.Matchers)
	}
}

func TestParseSelector_EmptyBraces(t *testing.T) {
	sel, err := metrics.ParseSelector(`http_requests_total{}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sel.MetricName != "http_requests_total" {
		t.Errorf("MetricName = %q, want %q", sel.MetricName, "http_requests_total")
	}
	if len(sel.Matchers) != 0 {
		t.Errorf("Matchers = %v, want empty", sel.Matchers)
	}
}

func TestParseSelector_EmptyString_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector("")
	if err == nil {
		t.Error("expected error for empty selector, got nil")
	}
}

func TestParseSelector_UnsupportedOperatorNotEqual_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service!="api"}`)
	if err == nil {
		t.Error("expected error for != operator, got nil")
	}
}

func TestParseSelector_UnsupportedOperatorRegexMatch_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service=~"api.*"}`)
	if err == nil {
		t.Error("expected error for =~ operator, got nil")
	}
}

func TestParseSelector_UnsupportedOperatorRegexNotMatch_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service!~"api.*"}`)
	if err == nil {
		t.Error("expected error for !~ operator, got nil")
	}
}

func TestParseSelector_UnclosedBrace_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service="api"`)
	if err == nil {
		t.Error("expected error for unclosed brace, got nil")
	}
}

func TestParseSelector_MissingQuote_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service=api}`)
	if err == nil {
		t.Error("expected error for unquoted value, got nil")
	}
}

func TestParseSelector_EmptyBracesNoMetricName_MatchesAll(t *testing.T) {
	sel, err := metrics.ParseSelector(`{}`)
	if err != nil {
		t.Fatalf("unexpected error for {}: %v", err)
	}
	if sel.MetricName != "" {
		t.Errorf("MetricName = %q, want empty", sel.MetricName)
	}
	if len(sel.Matchers) != 0 {
		t.Errorf("Matchers = %v, want empty", sel.Matchers)
	}
}

func TestParseSelector_TrailingContentAfterBrace_ReturnsError(t *testing.T) {
	_, err := metrics.ParseSelector(`http_requests_total{service="api"}garbage`)
	if err == nil {
		t.Error("expected error for trailing content after }, got nil")
	}
}

func TestParseSelector_LabelValueContainingOperatorChars_Accepted(t *testing.T) {
	sel, err := metrics.ParseSelector(`metric{label="foo=~bar"}`)
	if err != nil {
		t.Fatalf("unexpected error for value containing =~: %v", err)
	}
	if len(sel.Matchers) != 1 || sel.Matchers[0].Value != "foo=~bar" {
		t.Errorf("unexpected matcher: %+v", sel.Matchers)
	}
}
