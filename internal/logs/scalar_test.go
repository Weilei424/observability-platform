package logs

import (
	"errors"
	"math"
	"testing"
)

// TestParseScalarQuery_GrafanaHealthCheck pins the exact expression Grafana's
// Loki datasource health check sends (Grafana 11.1.0 pkg/tsdb/loki/healthcheck.go
// issues `vector(1)+vector(1)` as an instant query and requires the value 2).
func TestParseScalarQuery_GrafanaHealthCheck(t *testing.T) {
	got, err := ParseScalarQuery("vector(1)+vector(1)")
	if err != nil {
		t.Fatalf("health-check query rejected: %v", err)
	}
	if got != 2 {
		t.Fatalf("vector(1)+vector(1) = %v, want 2", got)
	}
}

func TestParseScalarQuery_Arithmetic(t *testing.T) {
	cases := []struct {
		q    string
		want float64
	}{
		{"vector(1)", 1},
		{"  vector( 1 ) + vector( 1 )  ", 2},
		{"vector(1)+vector(2)*vector(3)", 7},   // precedence, not 9
		{"(vector(1)+vector(2))*vector(3)", 9}, // parentheses override
		{"vector(10)/vector(4)", 2.5},
		{"vector(3)-vector(5)", -2},
		{"-vector(2)+vector(5)", 3},
		{"vector(1.5e1)", 15},
		{"1+1", 2}, // bare literals, a harmless superset
		{"vector(2) * -vector(3)", -6},
	}
	for _, c := range cases {
		got, err := ParseScalarQuery(c.q)
		if err != nil {
			t.Errorf("ParseScalarQuery(%q) error: %v", c.q, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseScalarQuery(%q) = %v, want %v", c.q, got, c.want)
		}
	}
}

// TestParseScalarQuery_DivideByZero matches Prometheus/Loki, which return an
// infinity rather than an error.
func TestParseScalarQuery_DivideByZero(t *testing.T) {
	got, err := ParseScalarQuery("vector(1)/vector(0)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !math.IsInf(got, 1) {
		t.Fatalf("vector(1)/vector(0) = %v, want +Inf", got)
	}
}

// TestParseScalarQuery_RealMetricQueriesStayUnsupported guards the boundary of
// the shim: anything that would have to read stored samples must still return the
// explicit unsupported error, not a made-up number.
func TestParseScalarQuery_RealMetricQueriesStayUnsupported(t *testing.T) {
	cases := []string{
		`rate({service="api"}[5m])`,
		`count_over_time({a="b"}[1m])`,
		`sum(rate({a="b"}[5m]))`,
		`sum by (level) (count_over_time({a="b"}[1m]))`,
		`bytes_over_time({a="b"}[1m])`,
		`avg_over_time({a="b"} | unwrap dur [1m])`,
	}
	for _, q := range cases {
		_, err := ParseScalarQuery(q)
		if !errors.Is(err, errUnsupportedMetricQuery) {
			t.Errorf("ParseScalarQuery(%q) error = %v, want errUnsupportedMetricQuery", q, err)
		}
	}
}

// TestParseScalarQuery_VectorTakesANumber pins the shim to upstream LogQL's
// production `vector OPEN_PARENTHESIS NUMBER CLOSE_PARENTHESIS`. An operand that
// is anything other than a bare numeric literal is a shape Loki itself rejects,
// so accepting it would make the shim wrong in a way that is invisible until
// someone relies on it.
func TestParseScalarQuery_VectorTakesANumber(t *testing.T) {
	for _, q := range []string{
		`vector(vector(1))`, // nested vector
		`vector(1+1)`,       // arithmetic operand
		`vector((1))`,       // parenthesized operand
		`vector(-1)`,        // signed operand: negate outside instead
		`vector(x)`,
	} {
		if got, err := ParseScalarQuery(q); err == nil {
			t.Errorf("ParseScalarQuery(%q) = %v, nil error; want error (vector() takes a number)", q, got)
		}
	}
	// Arithmetic still composes outside the parentheses, so nothing is lost.
	for _, c := range []struct {
		q    string
		want float64
	}{
		{`-vector(1)`, -1},
		{`vector(1)+vector(1)`, 2},
	} {
		got, err := ParseScalarQuery(c.q)
		if err != nil || got != c.want {
			t.Errorf("ParseScalarQuery(%q) = %v, %v; want %v, nil", c.q, got, err, c.want)
		}
	}
}

func TestParseScalarQuery_Rejections(t *testing.T) {
	cases := []string{
		``,
		`vector(`,
		`vector()`,
		`vector(1))`,
		`vector(1) +`,
		`vector(1) vector(2)`,
		`vector(1.2.3)`,
		`)`,
		`{service="api"}`, // a log query is not a scalar query
	}
	for _, q := range cases {
		if got, err := ParseScalarQuery(q); err == nil {
			t.Errorf("ParseScalarQuery(%q) = %v, nil error; want error", q, got)
		}
	}
}
