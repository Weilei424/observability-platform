package metrics

import (
	"fmt"
	"strings"
)

// ParseExpr parses a PromQL-subset expression into an Expr tree.
//
// Supported forms:
//
//	metric_name                        → SelectorExpr
//	metric_name{label="value"}         → SelectorExpr
//	rate(selector[duration])           → RateExpr
//	sum(expr)                          → SumExpr{By: nil}
//	sum by (l1, l2)(expr)              → SumExpr{By: [...]}
//
// Any other function name returns an explicit error.
func ParseExpr(s string) (Expr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty expression")
	}

	// Extract the leading word (stops at '(', '{', or whitespace).
	// This identifies the function name without consuming the argument list.
	i := 0
	for i < len(s) && s[i] != '(' && s[i] != '{' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	word := s[:i]

	switch word {
	case "rate":
		return parseRateExpr(s)
	case "sum":
		return parseSumExpr(s)
	case "":
		// Starts with '(' or '{' — treat as selector
		sel, err := ParseSelector(s)
		if err != nil {
			return nil, err
		}
		return SelectorExpr{Selector: sel}, nil
	default:
		// Check whether this is a function call (next non-space char is '(').
		rest := strings.TrimSpace(s[i:])
		if len(rest) > 0 && rest[0] == '(' {
			return nil, fmt.Errorf("unsupported query function: %q", word)
		}
		// Otherwise it's a selector (metric name with optional label set).
		sel, err := ParseSelector(s)
		if err != nil {
			return nil, err
		}
		return SelectorExpr{Selector: sel}, nil
	}
}

// parseRateExpr parses rate(selector[duration]).
func parseRateExpr(s string) (Expr, error) {
	// s is "rate(...)"
	inner, trailing, err := extractFirstParen(s[len("rate"):])
	if err != nil {
		return nil, fmt.Errorf("rate: %w", err)
	}
	if strings.TrimSpace(trailing) != "" {
		return nil, fmt.Errorf("rate: unexpected content after closing ')'")
	}
	inner = strings.TrimSpace(inner)

	// Locate the [duration] suffix: expect selector[duration] with ] at end.
	bracketOpen := strings.LastIndexByte(inner, '[')
	if bracketOpen == -1 {
		return nil, fmt.Errorf("rate: missing duration window, expected selector[duration]")
	}
	bracketClose := strings.LastIndexByte(inner, ']')
	if bracketClose == -1 || bracketClose < bracketOpen {
		return nil, fmt.Errorf("rate: unclosed '[' in duration window")
	}
	if bracketClose != len(inner)-1 {
		return nil, fmt.Errorf("rate: unexpected content after ']'")
	}

	durStr := strings.TrimSpace(inner[bracketOpen+1 : bracketClose])
	windowMs, err := ParsePromDuration(durStr)
	if err != nil {
		return nil, fmt.Errorf("rate: invalid duration %q: %w", durStr, err)
	}
	if windowMs <= 0 {
		return nil, fmt.Errorf("rate: duration must be positive")
	}

	selStr := strings.TrimSpace(inner[:bracketOpen])
	sel, err := ParseSelector(selStr)
	if err != nil {
		return nil, fmt.Errorf("rate: invalid selector: %w", err)
	}

	return RateExpr{Selector: sel, WindowMs: windowMs}, nil
}

// parseSumExpr parses sum(expr) or sum by (l1, l2)(expr).
func parseSumExpr(s string) (Expr, error) {
	// s starts with "sum"
	rest := strings.TrimSpace(s[len("sum"):])

	var by []string
	if strings.HasPrefix(rest, "by") {
		byRest := rest[len("by"):]
		// 'by' must be followed by whitespace or '(' to avoid matching "byfoo"
		if len(byRest) == 0 || (byRest[0] != '(' && byRest[0] != ' ' && byRest[0] != '\t') {
			return nil, fmt.Errorf("sum by: expected '(' or space after 'by'")
		}
		byRest = strings.TrimSpace(byRest)
		labelList, remaining, err := extractFirstParen(byRest)
		if err != nil {
			return nil, fmt.Errorf("sum by: label list: %w", err)
		}
		by, err = parseLabelList(labelList)
		if err != nil {
			return nil, fmt.Errorf("sum by: %w", err)
		}
		rest = strings.TrimSpace(remaining)
	}

	if !strings.HasPrefix(rest, "(") {
		return nil, fmt.Errorf("sum: expected '(' before inner expression, got %q", rest)
	}

	innerStr, trailing, err := extractFirstParen(rest)
	if err != nil {
		return nil, fmt.Errorf("sum: %w", err)
	}
	if strings.TrimSpace(trailing) != "" {
		return nil, fmt.Errorf("sum: unexpected content after closing ')'")
	}

	inner, err := ParseExpr(strings.TrimSpace(innerStr))
	if err != nil {
		return nil, fmt.Errorf("sum inner: %w", err)
	}

	return SumExpr{Inner: inner, By: by}, nil
}

// extractFirstParen extracts the content of the first balanced parenthesized group
// starting at s[0] (which must be '('). Returns the content and the remaining string
// after the closing ')'.
func extractFirstParen(s string) (content, remaining string, err error) {
	if len(s) == 0 || s[0] != '(' {
		return "", "", fmt.Errorf("expected '('")
	}
	depth := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[1:i], s[i+1:], nil
			}
		}
	}
	return "", "", fmt.Errorf("unclosed '('")
}

// parseLabelList parses a comma-separated list of label names.
func parseLabelList(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty label list")
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			return nil, fmt.Errorf("empty label name in list")
		}
		if !labelNameRe.MatchString(name) {
			return nil, fmt.Errorf("invalid label name %q", name)
		}
		result = append(result, name)
	}
	return result, nil
}
