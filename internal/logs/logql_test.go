package logs

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

func TestParseLogQL_SelectorOnly(t *testing.T) {
	sel, err := ParseLogQL(`{service="api", level="error"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []index.Pair{{Name: "level", Value: "error"}, {Name: "service", Value: "api"}}
	got := append([]index.Pair(nil), sel.Matchers...)
	// order-independent compare
	if len(got) != len(want) {
		t.Fatalf("matchers = %v, want %v", got, want)
	}
	seen := map[index.Pair]bool{}
	for _, p := range got {
		seen[p] = true
	}
	for _, p := range want {
		if !seen[p] {
			t.Fatalf("missing matcher %v in %v", p, got)
		}
	}
	if len(sel.LineFilters) != 0 {
		t.Fatalf("expected no line filters, got %d", len(sel.LineFilters))
	}
}

func TestParseLogQL_LineFilters(t *testing.T) {
	sel, err := ParseLogQL(`{app="x"} |= "error" != "debug" |~ "id=[0-9]+" !~ "healthz"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sel.LineFilters) != 4 {
		t.Fatalf("got %d filters, want 4", len(sel.LineFilters))
	}
	ops := []FilterOp{FilterContains, FilterNotContains, FilterMatch, FilterNotMatch}
	for i, op := range ops {
		if sel.LineFilters[i].Op != op {
			t.Fatalf("filter %d op = %d, want %d", i, sel.LineFilters[i].Op, op)
		}
	}
	if !sel.LineFilters[0].Keep("an error line") || sel.LineFilters[0].Keep("clean line") {
		t.Fatalf("FilterContains Keep wrong")
	}
	if sel.LineFilters[1].Keep("has debug") || !sel.LineFilters[1].Keep("clean") {
		t.Fatalf("FilterNotContains Keep wrong")
	}
	if !sel.LineFilters[2].Keep("id=42") || sel.LineFilters[2].Keep("no id") {
		t.Fatalf("FilterMatch Keep wrong")
	}
	if sel.LineFilters[3].Keep("GET /healthz") || !sel.LineFilters[3].Keep("GET /api") {
		t.Fatalf("FilterNotMatch Keep wrong")
	}
}

func TestParseLogQL_ValueWithBraceAndComma(t *testing.T) {
	sel, err := ParseLogQL(`{path="/a,b}c"} |= "x}y"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sel.Matchers) != 1 || sel.Matchers[0].Value != "/a,b}c" {
		t.Fatalf("matcher = %v, want path=/a,b}c", sel.Matchers)
	}
	if len(sel.LineFilters) != 1 || sel.LineFilters[0].Value != "x}y" {
		t.Fatalf("filter = %v, want x}y", sel.LineFilters)
	}
}

func TestParseLogQL_Rejections(t *testing.T) {
	cases := []string{
		``,
		`{}`,
		`{service="api"`,            // unclosed brace
		`{service=~"api"}`,          // regex label matcher
		`{service!="api"}`,          // negative label matcher
		`{service="api"} | json`,    // pipeline
		`{service="api"} | logfmt`,  // pipeline
		`rate({service="api"}[5m])`, // metric query
		`count_over_time({a="b"}[1m])`,
		`{service="api"} |~ "("`,   // invalid regex
		`{service="api"} |= error`, // unquoted operand
		`{service="api"} |= "err`,  // unterminated operand
		`{service="api"} |# "x"`,   // unknown operator
	}
	for _, q := range cases {
		if _, err := ParseLogQL(q); err == nil {
			t.Fatalf("ParseLogQL(%q) = nil error, want error", q)
		}
	}
}
