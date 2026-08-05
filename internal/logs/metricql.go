package logs

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// RangeOp is a range aggregation over log lines. Every supported op reads only
// the line and its timestamp; the label-extraction family (which needs `unwrap`)
// is out of the subset.
type RangeOp int

const (
	OpCountOverTime RangeOp = iota
	OpRate
	OpBytesOverTime
	OpBytesRate
)

var rangeOpNames = [...]string{
	OpCountOverTime: "count_over_time",
	OpRate:          "rate",
	OpBytesOverTime: "bytes_over_time",
	OpBytesRate:     "bytes_rate",
}

func (o RangeOp) String() string {
	if int(o) < 0 || int(o) >= len(rangeOpNames) {
		return "unknown"
	}
	return rangeOpNames[o]
}

func lookupRangeOp(name string) (RangeOp, bool) {
	for i, n := range rangeOpNames {
		if n == name {
			return RangeOp(i), true
		}
	}
	return 0, false
}

// supportedOps is quoted in errors so a rejection names the alternatives.
const supportedOps = "count_over_time, rate, bytes_over_time, bytes_rate"

// AggKind is the optional vector aggregation wrapping a range aggregation.
type AggKind int

const (
	AggNone AggKind = iota
	AggSum
)

// Grouping is a by/without clause. Labels is non-empty whenever a clause is
// present; a bare sum() and a bare range aggregation both carry the zero value.
type Grouping struct {
	Without bool
	Labels  []string
}

// MetricQuery is a parsed metric query from the supported subset:
//
//	[ sum [by|without (labels)] ( ] range_op( selector [line filters] [range] ) [ ) ]
type MetricQuery struct {
	Op       RangeOp
	Selector LogSelector
	RangeNs  int64
	Agg      AggKind
	Grouping Grouping
}

// ErrNotMetricQuery reports that the expression is not in the metric subset at
// all — a stream selector, a numeric literal, or vector(). Callers fall back to
// another parser on this sentinel; it is never written to a client.
var ErrNotMetricQuery = errors.New("not a metric query")

// unsupportedAggs are vector aggregations LogQL has and this subset does not, so
// they get a message naming the one that is supported instead of being reported
// as an unknown range aggregation.
var unsupportedAggs = map[string]bool{
	"count": true, "avg": true, "min": true, "max": true, "stddev": true,
	"stdvar": true, "topk": true, "bottomk": true, "sort": true, "sort_desc": true,
}

var (
	errPipelineUnsupported = errors.New("parse error: unsupported LogQL feature: log pipelines such as | json, | logfmt, | unwrap and | label_format are not supported")
	errOffsetUnsupported   = errors.New("parse error: unsupported LogQL feature: the offset modifier is not supported")
)

// IsLogExpression reports whether q is a log query rather than a metric or
// constant expression — the discriminator both query endpoints dispatch on. A
// log query opens with a stream selector; everything else opens with a function
// name, a number, or a sign. An empty expression counts as a log query so the
// log parser owns the "empty query" error, as it did before metric queries
// existed.
func IsLogExpression(q string) bool {
	trimmed := strings.TrimSpace(q)
	return trimmed == "" || trimmed[0] == '{'
}

// ParseMetricQuery parses the supported metric subset. It returns
// ErrNotMetricQuery — not a parse error — when the expression belongs to another
// parser, so the caller can fall through to the log or constant-expression path.
func ParseMetricQuery(q string) (MetricQuery, error) {
	s := strings.TrimSpace(q)
	if s == "" {
		return MetricQuery{}, fmt.Errorf("parse error: empty query")
	}
	if s[0] == '{' {
		return MetricQuery{}, ErrNotMetricQuery // a log query
	}
	// A metric query always opens with a function name. Anything else — a digit,
	// a sign, a parenthesis — belongs to the constant-expression parser. This
	// check cannot be left to scanIdent: its charset includes digits, so it would
	// happily return "42" from `42` and "1" from `1+1`, and those would be
	// reported as unknown functions instead of falling through to the shim.
	if c := s[0]; c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') {
		return MetricQuery{}, ErrNotMetricQuery
	}
	name, next, err := scanIdent(s, 0)
	if err != nil {
		return MetricQuery{}, ErrNotMetricQuery
	}
	if name == "vector" {
		return MetricQuery{}, ErrNotMetricQuery // the constant-expression shim
	}

	var mq MetricQuery
	i := next
	if name == "sum" {
		mq.Agg = AggSum
		i, err = parseGrouping(s, i, &mq.Grouping)
		if err != nil {
			return MetricQuery{}, err
		}
		i = skipSpace(s, i)
		if i >= len(s) || s[i] != '(' {
			return MetricQuery{}, fmt.Errorf("parse error: expected '(' after sum, got %q", s[i:])
		}
		i = skipSpace(s, i+1)
		inner, n, err := parseRangeAgg(s[i:])
		if err != nil {
			return MetricQuery{}, err
		}
		mq.Op, mq.Selector, mq.RangeNs = inner.Op, inner.Selector, inner.RangeNs
		i = skipSpace(s, i+n)
		if i >= len(s) || s[i] != ')' {
			return MetricQuery{}, fmt.Errorf("parse error: expected ')' closing sum, got %q", s[i:])
		}
		i++
	} else {
		inner, n, err := parseRangeAgg(s)
		if err != nil {
			return MetricQuery{}, err
		}
		mq.Op, mq.Selector, mq.RangeNs = inner.Op, inner.Selector, inner.RangeNs
		i = n
	}

	if rest := strings.TrimSpace(s[i:]); rest != "" {
		return MetricQuery{}, trailingError(rest)
	}
	return mq, nil
}

// rangeAgg is one parsed range aggregation, the only thing sum() may wrap.
type rangeAgg struct {
	Op       RangeOp
	Selector LogSelector
	RangeNs  int64
}

// parseRangeAgg parses `op( {selector} [filters] [duration] )` at the start of s
// and returns it with the bytes consumed through the closing ')'.
func parseRangeAgg(s string) (rangeAgg, int, error) {
	name, i, err := scanIdent(s, 0)
	if err != nil {
		return rangeAgg{}, 0, fmt.Errorf("parse error: expected a range aggregation (%s), got %q", supportedOps, s)
	}
	op, ok := lookupRangeOp(name)
	if !ok {
		switch {
		case name == "sum":
			return rangeAgg{}, 0, fmt.Errorf("parse error: nested aggregations are not supported; sum must wrap a range aggregation such as count_over_time(...)")
		case unsupportedAggs[name]:
			return rangeAgg{}, 0, fmt.Errorf("parse error: unsupported LogQL feature: aggregation %q; only sum is supported", name)
		default:
			return rangeAgg{}, 0, fmt.Errorf("parse error: unsupported LogQL feature: %q; supported range aggregations are %s", name, supportedOps)
		}
	}
	i = skipSpace(s, i)
	if i >= len(s) || s[i] != '(' {
		return rangeAgg{}, 0, fmt.Errorf("parse error: expected '(' after %s", name)
	}
	i = skipSpace(s, i+1)
	if i >= len(s) || s[i] != '{' {
		return rangeAgg{}, 0, fmt.Errorf("parse error: %s requires a stream selector '{...}', got %q", name, s[i:])
	}
	matchers, n, err := parseStreamSelector(s[i:])
	if err != nil {
		return rangeAgg{}, 0, err
	}
	if len(matchers) == 0 {
		return rangeAgg{}, 0, fmt.Errorf("parse error: stream selector must contain at least one label matcher")
	}
	i += n

	filters, n, err := parseLineFiltersPrefix(s[i:])
	if err != nil {
		return rangeAgg{}, 0, err
	}
	i = skipSpace(s, i+n)
	if i < len(s) && s[i] == '|' {
		return rangeAgg{}, 0, errPipelineUnsupported
	}
	if i >= len(s) || s[i] != '[' {
		return rangeAgg{}, 0, fmt.Errorf("parse error: expected a range such as [5m] after the stream selector in %s(...), got %q", name, s[i:])
	}
	end := strings.IndexByte(s[i:], ']')
	if end < 0 {
		return rangeAgg{}, 0, fmt.Errorf("parse error: unclosed '[' in the range of %s(...)", name)
	}
	rangeNs, err := parseLogQLRange(strings.TrimSpace(s[i+1 : i+end]))
	if err != nil {
		return rangeAgg{}, 0, err
	}
	i = skipSpace(s, i+end+1)
	if strings.HasPrefix(s[i:], "offset") {
		return rangeAgg{}, 0, errOffsetUnsupported
	}
	if i >= len(s) || s[i] != ')' {
		return rangeAgg{}, 0, fmt.Errorf("parse error: expected ')' closing %s, got %q", name, s[i:])
	}
	return rangeAgg{
		Op:       op,
		Selector: LogSelector{Matchers: matchers, LineFilters: filters},
		RangeNs:  rangeNs,
	}, i + 1, nil
}

// parseLogQLRange parses a [range] duration the way upstream LogQL's parseDuration
// does: the Prometheus grammar first, so 1d and 1w work, then Go's own parser as a
// fallback, so 1.5h and 150ns work. Unlike `since`, which is Prometheus-only, the
// range selector accepts both.
func parseLogQLRange(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("parse error: empty range; want a duration such as [5m]")
	}
	ns, err := metrics.ParsePromDurationNanos(s)
	if err != nil {
		d, goErr := time.ParseDuration(s)
		if goErr != nil {
			return 0, fmt.Errorf("parse error: invalid range %q: want a duration such as 5m, 1h30m, 1d, or 1.5h", s)
		}
		ns = d.Nanoseconds()
	}
	if ns <= 0 {
		return 0, fmt.Errorf("parse error: range %q must be greater than 0", s)
	}
	return ns, nil
}

// parseGrouping parses an optional `by (l1, l2)` / `without (l1, l2)` clause at
// s[i] and returns the index after it. Upstream also accepts the clause after the
// argument (`sum(x) by (l)`); this parser is prefix-only, matching
// internal/metrics/expr_parser.go, and trailingError names the accepted form.
func parseGrouping(s string, i int, g *Grouping) (int, error) {
	i = skipSpace(s, i)
	var without bool
	switch {
	case hasKeyword(s, i, "by"):
		i += len("by")
	case hasKeyword(s, i, "without"):
		without = true
		i += len("without")
	default:
		return i, nil
	}
	i = skipSpace(s, i)
	if i >= len(s) || s[i] != '(' {
		return 0, fmt.Errorf("parse error: expected '(' after the grouping keyword, got %q", s[i:])
	}
	end := strings.IndexByte(s[i:], ')')
	if end < 0 {
		return 0, fmt.Errorf("parse error: unclosed '(' in the grouping label list")
	}
	labels, err := parseGroupingLabels(s[i+1 : i+end])
	if err != nil {
		return 0, err
	}
	g.Without, g.Labels = without, labels
	return i + end + 1, nil
}

// hasKeyword reports whether kw appears at s[i] as a whole word, so `byte_total`
// is not read as the `by` keyword.
func hasKeyword(s string, i int, kw string) bool {
	if !strings.HasPrefix(s[i:], kw) {
		return false
	}
	j := i + len(kw)
	if j >= len(s) {
		return true
	}
	c := s[j]
	return c == '(' || c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func parseGroupingLabels(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("parse error: empty label list in the grouping clause")
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if !logqlLabelNameRe.MatchString(name) {
			return nil, fmt.Errorf("parse error: invalid label name %q in the grouping clause", name)
		}
		out = append(out, name)
	}
	return out, nil
}

// trailingError reports content after an otherwise complete metric expression.
// Both causes are common enough to name: the trailing grouping form upstream
// accepts, and binary operations such as the `sum(...) / sum(...)` ratio idiom.
func trailingError(rest string) error {
	if hasKeyword(rest, 0, "by") || hasKeyword(rest, 0, "without") {
		return fmt.Errorf("parse error: the grouping clause must precede the argument: write sum by (level) (count_over_time(...)), not sum(count_over_time(...)) by (level)")
	}
	return fmt.Errorf("parse error: unexpected %q after the metric expression; binary operations and chained expressions are not supported", rest)
}
