package logs

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseMetricQuery_Accepts(t *testing.T) {
	cases := []struct {
		q            string
		wantOp       RangeOp
		wantRange    time.Duration
		wantAgg      AggKind
		wantWithout  bool
		wantLabels   []string
		wantFilters  int
		wantMatchers int
	}{
		{`count_over_time({service="api"}[5m])`, OpCountOverTime, 5 * time.Minute, AggNone, false, nil, 0, 1},
		{`rate({service="api"}[30s])`, OpRate, 30 * time.Second, AggNone, false, nil, 0, 1},
		{`bytes_over_time({service="api"}[1h])`, OpBytesOverTime, time.Hour, AggNone, false, nil, 0, 1},
		{`bytes_rate({service="api"}[1h30m])`, OpBytesRate, 90 * time.Minute, AggNone, false, nil, 0, 1},
		{`count_over_time({service="api", level="error"} |= "boom" [5m])`, OpCountOverTime, 5 * time.Minute, AggNone, false, nil, 1, 2},
		{`count_over_time({a="b"} |= "x" != "y" |~ "z" [5m])`, OpCountOverTime, 5 * time.Minute, AggNone, false, nil, 3, 1},
		{`sum(count_over_time({a="b"}[5m]))`, OpCountOverTime, 5 * time.Minute, AggSum, false, nil, 0, 1},
		{`sum by (level) (count_over_time({a="b"}[5m]))`, OpCountOverTime, 5 * time.Minute, AggSum, false, []string{"level"}, 0, 1},
		{`sum by (level, service) (rate({a="b"}[5m]))`, OpRate, 5 * time.Minute, AggSum, false, []string{"level", "service"}, 0, 1},
		{`sum without (level) (count_over_time({a="b"}[5m]))`, OpCountOverTime, 5 * time.Minute, AggSum, true, []string{"level"}, 0, 1},
		{`sum by(level)(count_over_time({a="b"}[5m]))`, OpCountOverTime, 5 * time.Minute, AggSum, false, []string{"level"}, 0, 1},
		{"  sum by ( level )  (  count_over_time( {a=\"b\"} [5m] )  )  ", OpCountOverTime, 5 * time.Minute, AggSum, false, []string{"level"}, 0, 1},
		// Prometheus grammar (1d, 1w) and the Go fallback (1.5h, 150ns), in that
		// order, exactly as upstream LogQL's parseDuration tries them.
		{`count_over_time({a="b"}[1d])`, OpCountOverTime, 24 * time.Hour, AggNone, false, nil, 0, 1},
		{`count_over_time({a="b"}[1w])`, OpCountOverTime, 7 * 24 * time.Hour, AggNone, false, nil, 0, 1},
		{`count_over_time({a="b"}[1.5h])`, OpCountOverTime, 90 * time.Minute, AggNone, false, nil, 0, 1},
		{`count_over_time({a="b"}[150ns])`, OpCountOverTime, 150 * time.Nanosecond, AggNone, false, nil, 0, 1},
	}
	for _, tc := range cases {
		got, err := ParseMetricQuery(tc.q)
		if err != nil {
			t.Errorf("ParseMetricQuery(%q) error: %v", tc.q, err)
			continue
		}
		if got.Op != tc.wantOp {
			t.Errorf("%q: op = %v, want %v", tc.q, got.Op, tc.wantOp)
		}
		if got.RangeNs != tc.wantRange.Nanoseconds() {
			t.Errorf("%q: range = %d, want %d", tc.q, got.RangeNs, tc.wantRange.Nanoseconds())
		}
		if got.Agg != tc.wantAgg {
			t.Errorf("%q: agg = %v, want %v", tc.q, got.Agg, tc.wantAgg)
		}
		if got.Grouping.Without != tc.wantWithout {
			t.Errorf("%q: without = %v, want %v", tc.q, got.Grouping.Without, tc.wantWithout)
		}
		if len(got.Grouping.Labels) != len(tc.wantLabels) {
			t.Errorf("%q: grouping labels = %v, want %v", tc.q, got.Grouping.Labels, tc.wantLabels)
		} else {
			for i, l := range tc.wantLabels {
				if got.Grouping.Labels[i] != l {
					t.Errorf("%q: grouping label %d = %q, want %q", tc.q, i, got.Grouping.Labels[i], l)
				}
			}
		}
		if len(got.Selector.LineFilters) != tc.wantFilters {
			t.Errorf("%q: %d line filters, want %d", tc.q, len(got.Selector.LineFilters), tc.wantFilters)
		}
		if len(got.Selector.Matchers) != tc.wantMatchers {
			t.Errorf("%q: %d matchers, want %d", tc.q, len(got.Selector.Matchers), tc.wantMatchers)
		}
	}
}

func TestParseMetricQuery_Rejects(t *testing.T) {
	cases := []struct {
		q       string
		wantMsg string // substring the message must name
	}{
		{`avg_over_time({a="b"}[5m])`, "avg_over_time"},
		{`quantile_over_time(0.99, {a="b"}[5m])`, "quantile_over_time"},
		{`count(count_over_time({a="b"}[5m]))`, "sum"},
		{`topk(5, count_over_time({a="b"}[5m]))`, "topk"},
		{`sum(sum(count_over_time({a="b"}[5m])))`, "nested"},
		{`sum({a="b"})`, "range aggregation"},
		{`sum by () (count_over_time({a="b"}[5m]))`, "empty label list"},
		{`sum(count_over_time({a="b"}[5m])) by (level)`, "must precede"},
		{`count_over_time({a="b"})`, "[5m]"},
		{`count_over_time({a="b"}[0s])`, "greater than 0"},
		{`count_over_time({a="b"}[-5m])`, "greater than 0"}, // Go's parser accepts the sign; the positivity check rejects it
		{`count_over_time({a="b"}[bogus])`, "invalid range"},
		{`count_over_time({a="b"}[5m] offset 10m)`, "offset"},
		{`count_over_time({a="b"}[5m] offsetting)`, "')'"}, // word-boundary: "offsetting" is not the offset keyword
		{`count_over_time({a="b"} | json [5m])`, "pipeline"},
		{`count_over_time({a="b"} | unwrap dur [5m])`, "pipeline"},
		{`count_over_time({a=~"b"}[5m])`, "only '=' is supported"},
		{`count_over_time({}[5m])`, "at least one label matcher"},
		{`count_over_time([5m])`, "stream selector"},
		{`count_over_time({a="b"}[5m]) * 2`, "binary operations"},
		{`sum by (level) (count_over_time({a="b"}[5m])) / sum(count_over_time({a="c"}[5m]))`, "binary operations"},
		{`count_over_time({a="b"}[5m]`, "')'"},
		{`sum by (level) (count_over_time({a="b"}[5m])`, "')'"},
		{`sum by (1bad) (count_over_time({a="b"}[5m]))`, "invalid label name"},
	}
	for _, tc := range cases {
		got, err := ParseMetricQuery(tc.q)
		if err == nil {
			t.Errorf("ParseMetricQuery(%q) = %+v, want error", tc.q, got)
			continue
		}
		if errors.Is(err, ErrNotMetricQuery) {
			t.Errorf("ParseMetricQuery(%q) = ErrNotMetricQuery, want a parse error", tc.q)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantMsg) {
			t.Errorf("ParseMetricQuery(%q) error %q does not mention %q", tc.q, err, tc.wantMsg)
		}
	}
}

// TestParseMetricQuery_DropStage covers the drop stage reaching MetricQuery
// through parseRangeAgg — bare, after a line filter, and wrapped in sum() as
// Grafana's real Explore log-volume expression is (see task-1-brief.md).
func TestParseMetricQuery_DropStage(t *testing.T) {
	cases := []struct {
		q    string
		want []string
	}{
		{`sum by (level) (count_over_time({service="api"} | drop __error__[5m]))`, []string{"__error__"}},
		{`count_over_time({a="b"} |= "x" | drop __error__[5m])`, []string{"__error__"}},
	}
	for _, tc := range cases {
		got, err := ParseMetricQuery(tc.q)
		if err != nil {
			t.Errorf("ParseMetricQuery(%q) error: %v", tc.q, err)
			continue
		}
		if len(got.Selector.DropLabels) != len(tc.want) {
			t.Errorf("ParseMetricQuery(%q) DropLabels = %v, want %v", tc.q, got.Selector.DropLabels, tc.want)
			continue
		}
		for i, n := range tc.want {
			if got.Selector.DropLabels[i] != n {
				t.Errorf("ParseMetricQuery(%q) DropLabels[%d] = %q, want %q", tc.q, i, got.Selector.DropLabels[i], n)
			}
		}
	}
}

// TestParseMetricQuery_DropStage_Rejections covers the same "drop must be last,
// non-empty" rules on the metric-query path, where the drop stage is parsed by
// the same parseDropStagePrefix helper inside parseRangeAgg.
func TestParseMetricQuery_DropStage_Rejections(t *testing.T) {
	cases := []struct {
		q       string
		wantMsg string
	}{
		{`count_over_time({a="b"} | drop [5m])`, "label name"},
		{`count_over_time({a="b"} | drop x | json [5m])`, "pipeline"},
	}
	for _, tc := range cases {
		got, err := ParseMetricQuery(tc.q)
		if err == nil {
			t.Errorf("ParseMetricQuery(%q) = %+v, want error", tc.q, got)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantMsg) {
			t.Errorf("ParseMetricQuery(%q) error %q does not mention %q", tc.q, err, tc.wantMsg)
		}
	}
}

// TestParseMetricQuery_NotMetricQuery covers the sentinel the handlers' fallbacks
// depend on: these expressions belong to another parser, not to this one.
func TestParseMetricQuery_NotMetricQuery(t *testing.T) {
	for _, q := range []string{
		`vector(1)`,
		`vector(1)+vector(1)`,
		`1+1`,
		`(1+1)`,
		`42`,
		`-1`,
		`{service="api"}`,
		`{service="api"} |= "x"`,
	} {
		if _, err := ParseMetricQuery(q); !errors.Is(err, ErrNotMetricQuery) {
			t.Errorf("ParseMetricQuery(%q) error = %v, want ErrNotMetricQuery", q, err)
		}
	}
}
