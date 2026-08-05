package logs

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// logqlLabelNameRe validates a stream-selector label name. Defined locally
// because internal/labels keeps its equivalent unexported; matches that charset.
var logqlLabelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// metricQueryRe recognizes a leading "func(" so metric/aggregation queries get a
// targeted "unsupported" error instead of a generic one.
var metricQueryRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\s*\(`)

// FilterOp is a line-filter operator.
type FilterOp int

const (
	FilterContains    FilterOp = iota // |=
	FilterNotContains                 // !=
	FilterMatch                       // |~  (RE2)
	FilterNotMatch                    // !~  (RE2)
)

// LineFilter filters a log line by substring (|=, !=) or regexp (|~, !~).
type LineFilter struct {
	Op    FilterOp
	Value string
	re    *regexp.Regexp // set for FilterMatch/FilterNotMatch
}

// Keep reports whether line passes this filter.
func (f LineFilter) Keep(line string) bool {
	switch f.Op {
	case FilterContains:
		return strings.Contains(line, f.Value)
	case FilterNotContains:
		return !strings.Contains(line, f.Value)
	case FilterMatch:
		return f.re.MatchString(line)
	case FilterNotMatch:
		return !f.re.MatchString(line)
	default:
		return false
	}
}

// LogSelector is a parsed LogQL query: an equality stream selector, zero or more
// chained line filters applied left to right, and the label names named by a
// trailing `| drop` stage, if any.
type LogSelector struct {
	Matchers    []index.Pair
	LineFilters []LineFilter
	DropLabels  []string
}

// ParseLogQL parses the supported LogQL subset:
//
//	{label="value", ...} [ |= "x" | != "x" | |~ "re" | !~ "re" ]...
//
// Label matchers are equality-only and at least one is required. Pipelines,
// formatters, metric/aggregation queries, and regex/negative label matchers are
// rejected with an explicit error.
func ParseLogQL(q string) (LogSelector, error) {
	s := strings.TrimSpace(q)
	if s == "" {
		return LogSelector{}, fmt.Errorf("parse error: empty query")
	}
	if s[0] != '{' {
		if metricQueryRe.MatchString(s) {
			return LogSelector{}, errUnsupportedMetricQuery
		}
		return LogSelector{}, fmt.Errorf("parse error: query must start with a stream selector '{...}'")
	}
	matchers, n, err := parseStreamSelector(s)
	if err != nil {
		return LogSelector{}, err
	}
	if len(matchers) == 0 {
		return LogSelector{}, fmt.Errorf("parse error: stream selector must contain at least one label matcher")
	}
	rest := s[n:]
	filters, consumed, err := parseLineFiltersPrefix(rest)
	if err != nil {
		return LogSelector{}, err
	}
	dropLabels, dn, err := parseDropStagePrefix(rest[consumed:])
	if err != nil {
		return LogSelector{}, err
	}
	consumed += dn
	// The metric parser reads the remainder as a [range]; a log query has no
	// remainder, so anything left is unsupported syntax. Line filters and the one
	// supported pipeline stage (drop) are already consumed above, so a leading '|'
	// here is always some other pipeline stage — it gets the same
	// errPipelineUnsupported message a metric query's unsupported stage does,
	// rather than the generic message below.
	if trailing := strings.TrimSpace(rest[consumed:]); trailing != "" {
		if strings.HasPrefix(trailing, "|") {
			return LogSelector{}, errPipelineUnsupported
		}
		if len(trailing) < 2 {
			return LogSelector{}, fmt.Errorf("parse error: unsupported LogQL feature near %q", trailing)
		}
		return LogSelector{}, fmt.Errorf("parse error: unsupported LogQL feature near %q; only line filters |=, !=, |~, !~ are supported", trailing)
	}
	return LogSelector{Matchers: matchers, LineFilters: filters, DropLabels: dropLabels}, nil
}

// errUnsupportedMetricQuery is returned for real metric/aggregation LogQL. The
// constant-expression subset accepted by ParseScalarQuery (see scalar.go) is the
// one exception, and only on the instant-query path.
var errUnsupportedMetricQuery = errors.New("parse error: unsupported LogQL feature: metric and aggregation queries (e.g. rate(), count_over_time()) are not supported")

// scanString scans a LogQL string literal at the start of s and returns its
// unescaped value plus the bytes consumed including both delimiters. Two forms
// are recognized, matching LogQL (which lexes strings with Go's rules):
//
//	"a\"b"  double-quoted, backslash escapes processed
//	`a"b`   backtick-quoted raw string, no escape processing
//
// Escape-aware scanning is what lets a '"' appear inside a label value or a
// line-filter operand — e.g. |= "\"event\"" for JSON lines — without the
// literal terminating early.
func scanString(s string) (value string, n int, err error) {
	if s == "" {
		return "", 0, fmt.Errorf("expected a quoted string")
	}
	switch s[0] {
	case '`':
		end := strings.IndexByte(s[1:], '`')
		if end == -1 {
			return "", 0, fmt.Errorf("unterminated raw string %s", s)
		}
		return s[1 : 1+end], end + 2, nil
	case '"':
		for i := 1; i < len(s); i++ {
			switch s[i] {
			case '\\':
				i++ // skip the escaped byte; strconv.Unquote validates the sequence
			case '"':
				v, err := strconv.Unquote(s[:i+1])
				if err != nil {
					return "", 0, fmt.Errorf("invalid string literal %s: %w", s[:i+1], err)
				}
				return v, i + 1, nil
			}
		}
		return "", 0, fmt.Errorf("unterminated string %s", s)
	default:
		return "", 0, fmt.Errorf("expected a double-quoted or backtick-quoted string near %q", s)
	}
}

func skipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// parseStreamSelector parses `{name="value", ...}` starting at s[0]=='{' and
// returns the matchers plus the bytes consumed through the closing '}'.
//
// It scans the selector rather than splitting on delimiters, so anything that is
// not exactly a matcher list is a parse error — never a value that silently
// absorbs the junk around it.
func parseStreamSelector(s string) ([]index.Pair, int, error) {
	var matchers []index.Pair
	i := 1 // past '{'
	for {
		i = skipSpace(s, i)
		if i >= len(s) {
			return nil, 0, fmt.Errorf("parse error: unclosed '{' in stream selector")
		}
		if s[i] == '}' {
			return matchers, i + 1, nil
		}

		name, next, err := scanLabelName(s, i)
		if err != nil {
			return nil, 0, err
		}
		i = skipSpace(s, next)
		if i >= len(s) {
			return nil, 0, fmt.Errorf("parse error: unclosed '{' in stream selector")
		}
		switch {
		case strings.HasPrefix(s[i:], "=~"), strings.HasPrefix(s[i:], "!="), strings.HasPrefix(s[i:], "!~"):
			return nil, 0, fmt.Errorf("parse error: unsupported label matcher operator %q on label %q; only '=' is supported", s[i:i+2], name)
		case s[i] == '=':
			i++
		default:
			return nil, 0, fmt.Errorf("parse error: expected '=' after label name %q in stream selector, got %q", name, s[i:])
		}

		i = skipSpace(s, i)
		value, n, err := scanString(s[i:])
		if err != nil {
			return nil, 0, fmt.Errorf("parse error: label %q: %w", name, err)
		}
		matchers = append(matchers, index.Pair{Name: name, Value: value})

		i = skipSpace(s, i+n)
		if i >= len(s) {
			return nil, 0, fmt.Errorf("parse error: unclosed '{' in stream selector")
		}
		switch s[i] {
		case ',':
			i++
		case '}':
			return matchers, i + 1, nil
		default:
			return nil, 0, fmt.Errorf("parse error: expected ',' or '}' after the value of label %q, got %q", name, s[i:])
		}
	}
}

// scanLabelName reads a label name at s[i] and returns it with the next index.
// The loop takes the identifier charset; logqlLabelNameRe enforces the rest of
// the rule (non-empty, no leading digit).
func scanLabelName(s string, i int) (string, int, error) {
	start := i
	for i < len(s) && (s[i] == '_' ||
		(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	name := s[start:i]
	if !logqlLabelNameRe.MatchString(name) {
		return "", 0, fmt.Errorf("parse error: expected a label name in stream selector, got %q", s[start:])
	}
	return name, i, nil
}

// parseLineFiltersPrefix consumes as many chained line filters as it can from the
// start of s and returns them with the number of bytes consumed. It stops at the
// first token that does not begin a line filter, leaving the caller to decide
// whether the remainder is an error (ParseLogQL) or the [range] that follows a
// metric query's log expression (ParseMetricQuery).
//
// Stopping is only for an operator this parser does not recognize. Once an
// operator matches, a malformed operand is an error — otherwise `{a="b"} |= error`
// would silently parse as a bare selector.
func parseLineFiltersPrefix(s string) ([]LineFilter, int, error) {
	var filters []LineFilter
	i := skipSpace(s, 0)
	for {
		if len(s)-i < 2 {
			return filters, i, nil
		}
		var op FilterOp
		switch s[i : i+2] {
		case "|=":
			op = FilterContains
		case "!=":
			op = FilterNotContains
		case "|~":
			op = FilterMatch
		case "!~":
			op = FilterNotMatch
		default:
			return filters, i, nil
		}
		j := skipSpace(s, i+2)
		value, n, err := scanString(s[j:])
		if err != nil {
			return nil, 0, fmt.Errorf("parse error: line filter %s: %w", s[i:i+2], err)
		}
		f := LineFilter{Op: op, Value: value}
		if op == FilterMatch || op == FilterNotMatch {
			re, err := regexp.Compile(value)
			if err != nil {
				return nil, 0, fmt.Errorf("parse error: invalid regular expression %q: %w", value, err)
			}
			f.re = re
		}
		filters = append(filters, f)
		i = skipSpace(s, j+n)
	}
}

// parseDropStagePrefix consumes a `| drop a, b` stage at the start of s and returns the
// dropped label names with the bytes consumed. It returns (nil, 0, nil) when s does not
// begin one, leaving the caller to reject whatever is there.
//
// Only this one stage is supported, and only in last position, because it is the only one
// Grafana emits: its Loki datasource appends `| drop __error__` to every Explore
// log-volume query. Dropping __error__ is exactly a no-op here — no parser stage runs, so
// the label is never set — but the stage is implemented rather than ignored so that
// dropping a real label does what LogQL says it does.
func parseDropStagePrefix(s string) ([]string, int, error) {
	i := skipSpace(s, 0)
	if i >= len(s) || s[i] != '|' {
		return nil, 0, nil
	}
	j := skipSpace(s, i+1)
	if !hasKeyword(s, j, "drop") {
		return nil, 0, nil
	}
	j = skipSpace(s, j+len("drop"))

	var names []string
	for {
		name, next, identErr := scanIdent(s, j)
		if identErr != nil {
			return nil, 0, fmt.Errorf("parse error: drop requires a label name, got %q", s[j:])
		}
		if !logqlLabelNameRe.MatchString(name) {
			return nil, 0, fmt.Errorf("parse error: invalid label name %q in drop", name)
		}
		j = skipSpace(s, next)
		if j < len(s) && s[j] == '=' {
			return nil, 0, fmt.Errorf("parse error: drop takes plain label names, not a matcher; label %q is followed by %q", name, s[j:])
		}
		names = append(names, name)
		if j < len(s) && s[j] == ',' {
			j = skipSpace(s, j+1)
			continue
		}
		break
	}
	return names, j, nil
}
