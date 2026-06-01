package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func TestParseExpr_BareMetricName_ReturnsSelectorExpr(t *testing.T) {
	expr, err := metrics.ParseExpr("cpu_usage")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.SelectorExpr)
	if !ok {
		t.Fatalf("got %T, want SelectorExpr", expr)
	}
	if got.Selector.MetricName != "cpu_usage" {
		t.Errorf("MetricName = %q, want cpu_usage", got.Selector.MetricName)
	}
}

func TestParseExpr_SelectorWithLabels_ReturnsSelectorExpr(t *testing.T) {
	expr, err := metrics.ParseExpr(`cpu_usage{service="api"}`)
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.SelectorExpr)
	if !ok {
		t.Fatalf("got %T, want SelectorExpr", expr)
	}
	if got.Selector.MetricName != "cpu_usage" {
		t.Errorf("MetricName = %q, want cpu_usage", got.Selector.MetricName)
	}
	if len(got.Selector.Matchers) != 1 || got.Selector.Matchers[0].Name != "service" || got.Selector.Matchers[0].Value != "api" {
		t.Errorf("Matchers = %v, want [{service api}]", got.Selector.Matchers)
	}
}

func TestParseExpr_Rate_ReturnsRateExpr(t *testing.T) {
	expr, err := metrics.ParseExpr("rate(http_requests_total[5m])")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.RateExpr)
	if !ok {
		t.Fatalf("got %T, want RateExpr", expr)
	}
	if got.Selector.MetricName != "http_requests_total" {
		t.Errorf("Selector.MetricName = %q, want http_requests_total", got.Selector.MetricName)
	}
	const wantWindowMs = 5 * 60 * 1000
	if got.WindowMs != wantWindowMs {
		t.Errorf("WindowMs = %d, want %d", got.WindowMs, wantWindowMs)
	}
}

func TestParseExpr_Rate_WithLabels_ParsesSelector(t *testing.T) {
	expr, err := metrics.ParseExpr(`rate(http_requests_total{service="api"}[1m])`)
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.RateExpr)
	if !ok {
		t.Fatalf("got %T, want RateExpr", expr)
	}
	if got.Selector.MetricName != "http_requests_total" {
		t.Errorf("Selector.MetricName = %q, want http_requests_total", got.Selector.MetricName)
	}
	if len(got.Selector.Matchers) != 1 || got.Selector.Matchers[0].Name != "service" {
		t.Errorf("Matchers = %v, want [{service api}]", got.Selector.Matchers)
	}
	if got.WindowMs != 60*1000 {
		t.Errorf("WindowMs = %d, want 60000", got.WindowMs)
	}
}

func TestParseExpr_Sum_ReturnsSumExprWithNilBy(t *testing.T) {
	expr, err := metrics.ParseExpr("sum(http_requests_total)")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.SumExpr)
	if !ok {
		t.Fatalf("got %T, want SumExpr", expr)
	}
	if got.By != nil {
		t.Errorf("By = %v, want nil", got.By)
	}
	inner, ok := got.Inner.(metrics.SelectorExpr)
	if !ok {
		t.Fatalf("Inner: got %T, want SelectorExpr", got.Inner)
	}
	if inner.Selector.MetricName != "http_requests_total" {
		t.Errorf("Inner MetricName = %q, want http_requests_total", inner.Selector.MetricName)
	}
}

func TestParseExpr_SumBySingleLabel_SetsBy(t *testing.T) {
	expr, err := metrics.ParseExpr("sum by (service)(http_requests_total)")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.SumExpr)
	if !ok {
		t.Fatalf("got %T, want SumExpr", expr)
	}
	if len(got.By) != 1 || got.By[0] != "service" {
		t.Errorf("By = %v, want [service]", got.By)
	}
}

func TestParseExpr_SumByMultipleLabels_SetsAllLabels(t *testing.T) {
	expr, err := metrics.ParseExpr("sum by (service, env)(http_requests_total)")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	got, ok := expr.(metrics.SumExpr)
	if !ok {
		t.Fatalf("got %T, want SumExpr", expr)
	}
	if len(got.By) != 2 || got.By[0] != "service" || got.By[1] != "env" {
		t.Errorf("By = %v, want [service env]", got.By)
	}
}

func TestParseExpr_SumOfRate_ReturnsNestedExpr(t *testing.T) {
	expr, err := metrics.ParseExpr("sum(rate(http_requests_total[5m]))")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	sum, ok := expr.(metrics.SumExpr)
	if !ok {
		t.Fatalf("got %T, want SumExpr", expr)
	}
	if sum.By != nil {
		t.Errorf("By = %v, want nil", sum.By)
	}
	rate, ok := sum.Inner.(metrics.RateExpr)
	if !ok {
		t.Fatalf("Inner: got %T, want RateExpr", sum.Inner)
	}
	if rate.Selector.MetricName != "http_requests_total" {
		t.Errorf("rate.Selector.MetricName = %q, want http_requests_total", rate.Selector.MetricName)
	}
	if rate.WindowMs != 5*60*1000 {
		t.Errorf("rate.WindowMs = %d, want %d", rate.WindowMs, 5*60*1000)
	}
}

func TestParseExpr_UnknownFunction_ReturnsError(t *testing.T) {
	_, err := metrics.ParseExpr("avg(cpu_usage)")
	if err == nil {
		t.Fatal("expected error for avg(), got nil")
	}
}

func TestParseExpr_Empty_ReturnsError(t *testing.T) {
	_, err := metrics.ParseExpr("")
	if err == nil {
		t.Fatal("expected error for empty string, got nil")
	}
}

func TestParseExpr_Rate_MissingDuration_ReturnsError(t *testing.T) {
	_, err := metrics.ParseExpr("rate(http_requests_total)")
	if err == nil {
		t.Fatal("expected error for rate without duration window, got nil")
	}
}

func TestParseExpr_Rate_UnclosedBracket_ReturnsError(t *testing.T) {
	_, err := metrics.ParseExpr("rate(http_requests_total[5m)")
	if err == nil {
		t.Fatal("expected error for unclosed bracket, got nil")
	}
}

func TestParseExpr_Sum_UnclosedParen_ReturnsError(t *testing.T) {
	_, err := metrics.ParseExpr("sum(http_requests_total")
	if err == nil {
		t.Fatal("expected error for unclosed paren, got nil")
	}
}

func TestParseExpr_RateAsMetricName_ReturnsSelectorExpr(t *testing.T) {
	// "rate" without parentheses must be treated as a metric selector, not a function call.
	for _, input := range []string{"rate", `rate{job="api"}`} {
		expr, err := metrics.ParseExpr(input)
		if err != nil {
			t.Fatalf("ParseExpr(%q): unexpected error: %v", input, err)
		}
		if _, ok := expr.(metrics.SelectorExpr); !ok {
			t.Errorf("ParseExpr(%q): got %T, want SelectorExpr", input, expr)
		}
	}
}

func TestParseExpr_SumAsMetricName_ReturnsSelectorExpr(t *testing.T) {
	// "sum" without parentheses or "by" must be treated as a metric selector, not a function call.
	for _, input := range []string{"sum", `sum{job="api"}`} {
		expr, err := metrics.ParseExpr(input)
		if err != nil {
			t.Fatalf("ParseExpr(%q): unexpected error: %v", input, err)
		}
		if _, ok := expr.(metrics.SelectorExpr); !ok {
			t.Errorf("ParseExpr(%q): got %T, want SelectorExpr", input, expr)
		}
	}
}
