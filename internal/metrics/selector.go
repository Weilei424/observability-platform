package metrics

import (
	"fmt"
	"strings"
)

// Matcher is a single equality label filter.
type Matcher struct {
	Name  string
	Value string
}

// Selector describes which series to match: by metric name and zero or more
// equality label matchers. MetricName == "" matches all metric names.
type Selector struct {
	MetricName string
	Matchers   []Matcher
}

// ParseSelector parses a PromQL-subset selector string.
//
// Supported forms:
//
//	metric_name
//	metric_name{label="value", label2="value2"}
//	{label="value"}
//
// Only equality (=) matchers are supported. !=, =~, and !~ return an error.
func ParseSelector(s string) (Selector, error) {
	if strings.TrimSpace(s) == "" {
		return Selector{}, fmt.Errorf("selector must not be empty")
	}

	braceIdx := strings.IndexByte(s, '{')
	if braceIdx == -1 {
		name := strings.TrimSpace(s)
		if err := checkUnsupportedPromQL(name); err != nil {
			return Selector{}, err
		}
		return Selector{MetricName: name}, nil
	}

	sel := Selector{MetricName: strings.TrimSpace(s[:braceIdx])}
	if err := checkUnsupportedPromQL(sel.MetricName); err != nil {
		return Selector{}, err
	}

	rest := s[braceIdx+1:]
	closeIdx := strings.LastIndexByte(rest, '}')
	if closeIdx == -1 {
		return Selector{}, fmt.Errorf("unclosed brace in selector")
	}

	if strings.TrimSpace(rest[closeIdx+1:]) != "" {
		return Selector{}, fmt.Errorf("unexpected content after closing brace in selector")
	}

	inner := strings.TrimSpace(rest[:closeIdx])
	if inner == "" {
		return sel, nil
	}

	for _, part := range splitOnComma(inner) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		m, err := parseMatcher(part)
		if err != nil {
			return Selector{}, err
		}
		sel.Matchers = append(sel.Matchers, m)
	}

	return sel, nil
}

// splitOnComma splits s on commas that are not inside double-quoted strings.
func splitOnComma(s string) []string {
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

// checkUnsupportedPromQL returns an error if the metric name contains
// characters that indicate a PromQL function call or range selector.
// These are not supported in Phase 1.3; callers must pass a plain metric name.
func checkUnsupportedPromQL(name string) error {
	if strings.ContainsAny(name, "([") {
		return fmt.Errorf("unsupported query: PromQL functions and range selectors are not supported")
	}
	return nil
}

// parseMatcher parses a single `label="value"` expression.
func parseMatcher(s string) (Matcher, error) {
	eqIdx := strings.IndexByte(s, '=')
	if eqIdx == -1 {
		return Matcher{}, fmt.Errorf("missing '=' in matcher: %s", s)
	}

	// Check for unsupported operators by looking at the character before '='
	if eqIdx > 0 {
		prev := s[eqIdx-1]
		if prev == '!' || prev == '~' {
			return Matcher{}, fmt.Errorf("unsupported matcher operator at: %s", s)
		}
	}
	// Also check for =~ (regex match)
	if eqIdx+1 < len(s) && s[eqIdx+1] == '~' {
		return Matcher{}, fmt.Errorf("unsupported matcher operator at: %s", s)
	}

	name := strings.TrimSpace(s[:eqIdx])
	if name == "" {
		return Matcher{}, fmt.Errorf("empty label name in matcher: %s", s)
	}

	raw := strings.TrimSpace(s[eqIdx+1:])
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return Matcher{}, fmt.Errorf("matcher value must be double-quoted: %s", s)
	}

	// Note: backslash escape sequences inside quoted values are not supported.
	// Values are stored verbatim between the outer quotes.
	return Matcher{Name: name, Value: raw[1 : len(raw)-1]}, nil
}
