package logs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Constant metric queries exist for one reason: Grafana's Loki datasource health
// check issues the instant query `vector(1)+vector(1)` and requires the result to
// be 2 (Grafana 11.1.0, pkg/tsdb/loki/healthcheck.go). Evaluating constant
// expressions is the smallest honest way to satisfy it — a query that touches
// stored data (rate(), count_over_time(), sum() over a stream selector) still
// returns errUnsupportedMetricQuery.
//
// Grammar:
//
//	expr    = term { ("+" | "-") term }
//	term    = unary { ("*" | "/") unary }
//	unary   = [ "+" | "-" ] primary
//	primary = number | "(" expr ")" | "vector" "(" number ")"
//
// vector() takes a bare number, matching upstream LogQL's
// `vector OPEN_PARENTHESIS NUMBER CLOSE_PARENTHESIS`.

// ParseScalarQuery evaluates a constant metric query and returns its value. Any
// identifier other than vector() — i.e. every query that would have to read
// stored samples — returns errUnsupportedMetricQuery.
func ParseScalarQuery(q string) (float64, error) {
	p := &scalarParser{s: strings.TrimSpace(q)}
	if p.s == "" {
		return 0, fmt.Errorf("parse error: empty query")
	}
	v, err := p.expr()
	if err != nil {
		return 0, err
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return 0, fmt.Errorf("parse error: unexpected %q after expression", p.s[p.i:])
	}
	return v, nil
}

type scalarParser struct {
	s string
	i int
}

func (p *scalarParser) skipSpace() { p.i = skipSpace(p.s, p.i) }

func (p *scalarParser) expr() (float64, error) {
	v, err := p.term()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) || (p.s[p.i] != '+' && p.s[p.i] != '-') {
			return v, nil
		}
		op := p.s[p.i]
		p.i++
		rhs, err := p.term()
		if err != nil {
			return 0, err
		}
		if op == '+' {
			v += rhs
		} else {
			v -= rhs
		}
	}
}

func (p *scalarParser) term() (float64, error) {
	v, err := p.unary()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) || (p.s[p.i] != '*' && p.s[p.i] != '/') {
			return v, nil
		}
		op := p.s[p.i]
		p.i++
		rhs, err := p.unary()
		if err != nil {
			return 0, err
		}
		// Division by zero yields ±Inf, which formats as "+Inf"/"-Inf" — the same
		// thing Prometheus and Loki return rather than an error.
		if op == '*' {
			v *= rhs
		} else {
			v /= rhs
		}
	}
}

func (p *scalarParser) unary() (float64, error) {
	p.skipSpace()
	if p.i < len(p.s) && (p.s[p.i] == '-' || p.s[p.i] == '+') {
		op := p.s[p.i]
		p.i++
		v, err := p.unary()
		if err != nil {
			return 0, err
		}
		if op == '-' {
			return -v, nil
		}
		return v, nil
	}
	return p.primary()
}

func (p *scalarParser) primary() (float64, error) {
	p.skipSpace()
	if p.i >= len(p.s) {
		return 0, fmt.Errorf("parse error: unexpected end of query; want a number or vector(...)")
	}
	c := p.s[p.i]
	switch {
	case c == '(':
		p.i++
		v, err := p.expr()
		if err != nil {
			return 0, err
		}
		return v, p.expectClose()
	case (c >= '0' && c <= '9') || c == '.':
		return p.number()
	case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		return p.call()
	default:
		return 0, fmt.Errorf("parse error: unexpected %q in query", p.s[p.i:])
	}
}

// call parses `vector(<number>)`. Every other function name is a real metric
// query.
func (p *scalarParser) call() (float64, error) {
	name, next, err := scanIdent(p.s, p.i)
	if err != nil {
		return 0, err
	}
	if name != "vector" {
		return 0, errUnsupportedMetricQuery
	}
	p.i = skipSpace(p.s, next)
	if p.i >= len(p.s) || p.s[p.i] != '(' {
		return 0, fmt.Errorf("parse error: expected '(' after %q", name)
	}
	p.i++
	p.skipSpace()
	// Upstream LogQL's production is `vector OPEN_PARENTHESIS NUMBER
	// CLOSE_PARENTHESIS`: the operand is a bare numeric literal, not an
	// expression. Matching that shape exactly means vector(vector(1)),
	// vector(1+1) and vector((1)) are rejected here just as Loki rejects them,
	// rather than being quietly evaluated. Nothing is lost — arithmetic composes
	// outside the parentheses, as in vector(1)+vector(1).
	operand := p.i
	if p.i >= len(p.s) || !isNumberStart(p.s[p.i]) {
		return 0, errVectorOperand(p.s[operand:])
	}
	v, err := p.number()
	if err != nil {
		return 0, err
	}
	// A trailing operator is an operand-shape error, not a missing paren — report
	// the rule that was actually violated. Running out of input really is a
	// missing paren.
	p.skipSpace()
	if p.i >= len(p.s) {
		return 0, errMissingCloseParen
	}
	if p.s[p.i] != ')' {
		return 0, errVectorOperand(p.s[operand:])
	}
	p.i++
	return v, nil
}

func errVectorOperand(got string) error {
	return fmt.Errorf("parse error: vector() takes a number, got %q", got)
}

func isNumberStart(c byte) bool { return (c >= '0' && c <= '9') || c == '.' }

var errMissingCloseParen = errors.New("parse error: missing ')' in query")

func (p *scalarParser) expectClose() error {
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != ')' {
		return errMissingCloseParen
	}
	p.i++
	return nil
}

func (p *scalarParser) number() (float64, error) {
	start := p.i
	for p.i < len(p.s) {
		c := p.s[p.i]
		if (c >= '0' && c <= '9') || c == '.' {
			p.i++
			continue
		}
		// An exponent sign belongs to the literal, not to the surrounding
		// expression: 1e+5 is one number, 1+5 is two.
		if c == 'e' || c == 'E' {
			p.i++
			if p.i < len(p.s) && (p.s[p.i] == '+' || p.s[p.i] == '-') {
				p.i++
			}
			continue
		}
		break
	}
	v, err := strconv.ParseFloat(p.s[start:p.i], 64)
	if err != nil {
		return 0, fmt.Errorf("parse error: invalid number %q", p.s[start:p.i])
	}
	return v, nil
}

// scanIdent reads an identifier at s[i] and returns it with the next index.
func scanIdent(s string, i int) (string, int, error) {
	start := i
	for i < len(s) && (s[i] == '_' ||
		(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	if i == start {
		return "", 0, fmt.Errorf("parse error: expected an identifier, got %q", s[start:])
	}
	return s[start:i], i, nil
}
