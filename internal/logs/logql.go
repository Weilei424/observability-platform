package logs

import (
	"fmt"
	"regexp"
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

// LogSelector is a parsed LogQL query: an equality stream selector plus zero or
// more chained line filters applied left to right.
type LogSelector struct {
	Matchers    []index.Pair
	LineFilters []LineFilter
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
			return LogSelector{}, fmt.Errorf("parse error: unsupported LogQL feature: metric and aggregation queries (e.g. rate(), count_over_time()) are not supported")
		}
		return LogSelector{}, fmt.Errorf("parse error: query must start with a stream selector '{...}'")
	}
	closeIdx := findSelectorEnd(s)
	if closeIdx == -1 {
		return LogSelector{}, fmt.Errorf("parse error: unclosed '{' in stream selector")
	}
	matchers, err := parseStreamMatchers(s[1:closeIdx])
	if err != nil {
		return LogSelector{}, err
	}
	if len(matchers) == 0 {
		return LogSelector{}, fmt.Errorf("parse error: stream selector must contain at least one label matcher")
	}
	filters, err := parseLineFilters(strings.TrimSpace(s[closeIdx+1:]))
	if err != nil {
		return LogSelector{}, err
	}
	return LogSelector{Matchers: matchers, LineFilters: filters}, nil
}

// findSelectorEnd returns the index of the '}' closing the selector that starts at
// s[0]=='{', ignoring braces inside quoted strings so a '}' in a label value (or a
// later line-filter operand) does not end it early.
func findSelectorEnd(s string) int {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case '}':
			if !inQuote {
				return i
			}
		}
	}
	return -1
}

func parseStreamMatchers(inner string) ([]index.Pair, error) {
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return nil, nil
	}
	var matchers []index.Pair
	for _, part := range splitLogQLComma(inner) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		m, err := parseLabelMatcher(part)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

// splitLogQLComma splits on commas that are not inside double-quoted strings.
func splitLogQLComma(s string) []string {
	var parts []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// parseLabelMatcher parses a single `name="value"` equality matcher, rejecting
// =~, !=, and !~ operators explicitly.
func parseLabelMatcher(s string) (index.Pair, error) {
	eq := strings.IndexByte(s, '=')
	if eq == -1 {
		return index.Pair{}, fmt.Errorf("parse error: label matcher must use '=': %q", s)
	}
	if eq > 0 && (s[eq-1] == '!' || s[eq-1] == '~') {
		return index.Pair{}, fmt.Errorf("parse error: unsupported label matcher operator in %q; only '=' is supported", s)
	}
	if eq+1 < len(s) && s[eq+1] == '~' {
		return index.Pair{}, fmt.Errorf("parse error: unsupported label matcher operator '=~' in %q; only '=' is supported", s)
	}
	name := strings.TrimSpace(s[:eq])
	if name == "" {
		return index.Pair{}, fmt.Errorf("parse error: empty label name in %q", s)
	}
	if !logqlLabelNameRe.MatchString(name) {
		return index.Pair{}, fmt.Errorf("parse error: invalid label name %q", name)
	}
	raw := strings.TrimSpace(s[eq+1:])
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return index.Pair{}, fmt.Errorf("parse error: label value must be double-quoted in %q", s)
	}
	return index.Pair{Name: name, Value: raw[1 : len(raw)-1]}, nil
}

// parseLineFilters parses the chained line filters that follow a selector.
func parseLineFilters(s string) ([]LineFilter, error) {
	var filters []LineFilter
	s = strings.TrimSpace(s)
	for s != "" {
		if len(s) < 2 {
			return nil, fmt.Errorf("parse error: unsupported LogQL feature near %q", s)
		}
		var op FilterOp
		switch s[:2] {
		case "|=":
			op = FilterContains
		case "!=":
			op = FilterNotContains
		case "|~":
			op = FilterMatch
		case "!~":
			op = FilterNotMatch
		default:
			return nil, fmt.Errorf("parse error: unsupported LogQL feature near %q; only line filters |=, !=, |~, !~ are supported", s)
		}
		rest := strings.TrimSpace(s[2:])
		if rest == "" || rest[0] != '"' {
			return nil, fmt.Errorf("parse error: line filter operand must be a double-quoted string near %q", s)
		}
		end := strings.IndexByte(rest[1:], '"')
		if end == -1 {
			return nil, fmt.Errorf("parse error: unterminated line filter operand near %q", s)
		}
		value := rest[1 : 1+end]
		f := LineFilter{Op: op, Value: value}
		if op == FilterMatch || op == FilterNotMatch {
			re, err := regexp.Compile(value)
			if err != nil {
				return nil, fmt.Errorf("parse error: invalid regular expression %q: %w", value, err)
			}
			f.re = re
		}
		filters = append(filters, f)
		s = strings.TrimSpace(rest[1+end+1:])
	}
	return filters, nil
}
