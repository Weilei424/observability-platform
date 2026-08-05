package logs

import (
	"errors"
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

// TestParseLogQL_EscapedStrings covers LogQL's two string forms. Stream label
// values only have to be valid UTF-8 at ingest, so a value containing a quote is
// pushable and therefore has to be queryable.
func TestParseLogQL_EscapedStrings(t *testing.T) {
	cases := []struct {
		q         string
		wantValue string
	}{
		{`{service="a\"b"}`, `a"b`},            // escaped quote in a label value
		{"{service=`a\"b`}", `a"b`},            // raw string, no escape processing
		{`{service="a\\b"}`, `a\b`},            // escaped backslash
		{`{service="tab\there"}`, "tab\there"}, // standard Go escape
		{`{path="/a,b}c"}`, `/a,b}c`},          // delimiters inside a value
		{`{ service = "api" }`, `api`},         // spaces around the operator
		{`{service="api",}`, `api`},            // tolerated trailing comma
	}
	for _, c := range cases {
		sel, err := ParseLogQL(c.q)
		if err != nil {
			t.Errorf("ParseLogQL(%s) unexpected error: %v", c.q, err)
			continue
		}
		if len(sel.Matchers) == 0 || sel.Matchers[0].Value != c.wantValue {
			t.Errorf("ParseLogQL(%s) value = %+v, want %q", c.q, sel.Matchers, c.wantValue)
		}
	}
}

// TestParseLogQL_EscapedLineFilterOperands covers the JSON-search case: finding
// a quoted key in a log line requires escaped quotes in the operand.
func TestParseLogQL_EscapedLineFilterOperands(t *testing.T) {
	sel, err := ParseLogQL(`{app="x"} |= "\"event\":\"login\"" |~ ` + "`" + `id=\d+` + "`")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sel.LineFilters) != 2 {
		t.Fatalf("got %d filters, want 2", len(sel.LineFilters))
	}
	if got, want := sel.LineFilters[0].Value, `"event":"login"`; got != want {
		t.Errorf("filter operand = %q, want %q", got, want)
	}
	if !sel.LineFilters[0].Keep(`{"event":"login","user":"a"}`) {
		t.Error("escaped operand should match the JSON line")
	}
	if sel.LineFilters[0].Keep(`{"event":"logout"}`) {
		t.Error("escaped operand should not match a different event")
	}
	// The raw string reaches the regexp engine with its backslash intact.
	if !sel.LineFilters[1].Keep("req id=42") || sel.LineFilters[1].Keep("req id=x") {
		t.Error("raw-string regex operand wrong")
	}
}

// TestParseLineFiltersPrefix_StopsAtUnknownToken pins the contract the metric
// parser depends on: consume the filters, stop at whatever follows, and report
// how many bytes were consumed so the caller can keep parsing from there.
func TestParseLineFiltersPrefix_StopsAtUnknownToken(t *testing.T) {
	cases := []struct {
		in        string
		wantCount int
		wantRest  string
	}{
		{`[5m])`, 0, `[5m])`},
		{` |= "x" [5m])`, 1, `[5m])`},
		{` |= "x" != "y" [5m])`, 2, `[5m])`},
		{` |~ "5\\d\\d"`, 1, ``},
		{` | json`, 0, `| json`},
		{``, 0, ``},
	}
	for _, tc := range cases {
		filters, n, err := parseLineFiltersPrefix(tc.in)
		if err != nil {
			t.Fatalf("parseLineFiltersPrefix(%q) error: %v", tc.in, err)
		}
		if len(filters) != tc.wantCount {
			t.Errorf("parseLineFiltersPrefix(%q) = %d filters, want %d", tc.in, len(filters), tc.wantCount)
		}
		if got := tc.in[n:]; got != tc.wantRest {
			t.Errorf("parseLineFiltersPrefix(%q) left %q, want %q", tc.in, got, tc.wantRest)
		}
	}
}

// TestParseLineFiltersPrefix_OperandErrorsStillFail proves stopping is only for an
// unmatched operator: once an operator matches, a broken operand is an error, not
// a place to stop.
func TestParseLineFiltersPrefix_OperandErrorsStillFail(t *testing.T) {
	for _, in := range []string{` |= error`, ` |= "unterminated`, ` |~ "("`} {
		if _, _, err := parseLineFiltersPrefix(in); err == nil {
			t.Errorf("parseLineFiltersPrefix(%q) = nil error, want error", in)
		}
	}
}

// TestParseLogQL_NeverSilentlyMisparses is the counterpart to the accept cases:
// input that is not a well-formed selector must error rather than quietly become
// a different, valid-looking query.
func TestParseLogQL_NeverSilentlyMisparses(t *testing.T) {
	cases := []string{
		`{service="a"junk"b"}`,    // trailing junk once absorbed into the value
		`{service="a" "b"}`,       // two adjacent strings
		`{service="a" level="b"}`, // missing comma
		`{service}`,               // no operator
		`{="x"}`,                  // no label name
		`{9bad="x"}`,              // label name starting with a digit
		`{service="bad\q"}`,       // invalid escape sequence
		`{service="unterminated}`,
		"{service=`unterminated}",
		`{service="a"`, // unclosed brace after a complete matcher
	}
	for _, q := range cases {
		if sel, err := ParseLogQL(q); err == nil {
			t.Errorf("ParseLogQL(%s) = %+v, nil error; want error", q, sel)
		}
	}
}

// TestParseLogQL_DropStage covers the one supported pipeline stage: a trailing
// `| drop a, b`, optionally after line filters, with whitespace variants.
func TestParseLogQL_DropStage(t *testing.T) {
	cases := []struct {
		q    string
		want []string
	}{
		{`{a="b"} | drop __error__`, []string{"__error__"}},
		{`{a="b"} |= "x" | drop level, service`, []string{"level", "service"}},
		{`{a="b"}   |   drop   __error__   ,   level   `, []string{"__error__", "level"}},
		{`{a="b"}|drop __error__`, []string{"__error__"}},
	}
	for _, tc := range cases {
		sel, err := ParseLogQL(tc.q)
		if err != nil {
			t.Errorf("ParseLogQL(%q) unexpected error: %v", tc.q, err)
			continue
		}
		if len(sel.DropLabels) != len(tc.want) {
			t.Errorf("ParseLogQL(%q) DropLabels = %v, want %v", tc.q, sel.DropLabels, tc.want)
			continue
		}
		for i, n := range tc.want {
			if sel.DropLabels[i] != n {
				t.Errorf("ParseLogQL(%q) DropLabels[%d] = %q, want %q", tc.q, i, sel.DropLabels[i], n)
			}
		}
	}

	// The line filter ahead of the drop stage must still be parsed, not swallowed.
	sel, err := ParseLogQL(`{a="b"} |= "x" | drop level, service`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sel.LineFilters) != 1 || sel.LineFilters[0].Op != FilterContains {
		t.Fatalf("LineFilters = %+v, want one FilterContains", sel.LineFilters)
	}
	if len(sel.Matchers) != 1 || sel.Matchers[0].Value != "b" {
		t.Fatalf("Matchers = %+v, want a=b", sel.Matchers)
	}
}

// TestParseLogQL_DropStage_Rejections covers the grammar's restrictions: at least
// one label name, plain names only (no matcher), valid names only, and last
// position only — anything after `drop` is rejected the same as any other
// unsupported pipeline stage.
func TestParseLogQL_DropStage_Rejections(t *testing.T) {
	cases := []string{
		`{a="b"} | drop`,               // empty list
		`{a="b"} | drop __error__="x"`, // drop takes plain names, not a matcher
		`{a="b"} | drop 1bad`,          // invalid label name
		`{a="b"} | drop x | json`,      // drop must be last
		`{a="b"} | drop x |= "y"`,      // drop must be last
	}
	for _, q := range cases {
		if sel, err := ParseLogQL(q); err == nil {
			t.Errorf("ParseLogQL(%q) = %+v, nil error; want error", q, sel)
		}
	}
}

// TestParseLogQL_PipelineStagesStillRejected pins that adding `drop` did not
// loosen the rejection of every other pipeline stage, and that the message
// naming them (errPipelineUnsupported) is what callers still get.
func TestParseLogQL_PipelineStagesStillRejected(t *testing.T) {
	cases := []string{
		`{a="b"} | json`,
		`{a="b"} | logfmt`,
		`{a="b"} | line_format "{{.msg}}"`,
		`{a="b"} | unwrap bytes`,
	}
	for _, q := range cases {
		_, err := ParseLogQL(q)
		if err == nil {
			t.Errorf("ParseLogQL(%q) = nil error, want error", q)
			continue
		}
		if !errors.Is(err, errPipelineUnsupported) {
			t.Errorf("ParseLogQL(%q) error = %v, want errPipelineUnsupported", q, err)
		}
	}
}
