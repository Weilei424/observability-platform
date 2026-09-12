package e2e_test

// These tests hold the project's documentation to the same standard as its
// Grafana provisioning and Helm charts: a claim a document makes about the
// system is checked against the system. They read checked-in files and compare
// against in-process code — no Docker, no backend — so they run in
// `go test ./...` alongside everything else.
//
// Documentation rot is silent. A wrong dashboard breaks a panel; a wrong
// sentence breaks a reader who never files a bug.

import (
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

const docsRoot = "../.."

type docFile struct {
	Path string // repo-relative, for failure messages
	Body string
}

// docFiles reads every Markdown file matching a repo-relative glob. It fails the
// test when the glob matches nothing: a pattern that matches no files makes every
// check over it vacuously pass.
func docFiles(t *testing.T, glob string) []docFile {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(docsRoot, glob))
	if err != nil {
		t.Fatalf("glob %s: %v", glob, err)
	}
	if len(matches) == 0 {
		t.Fatalf("glob %s matched no files; a check over zero files passes without checking anything", glob)
	}
	slices.Sort(matches)
	out := make([]docFile, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		rel, err := filepath.Rel(docsRoot, m)
		if err != nil {
			rel = m
		}
		out = append(out, docFile{Path: filepath.ToSlash(rel), Body: string(b)})
	}
	return out
}

// repoFile reads one known repo-relative file. Later checks use it for source
// files (Makefile, internal/config/config.go) as well as documents.
func repoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(docsRoot, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

// lineOf converts a byte offset into a 1-based line number so failures point at
// a line a writer can open.
func lineOf(body string, idx int) int {
	return 1 + strings.Count(body[:idx], "\n")
}

// Endpoint documentation opens with a fenced http block listing every method the
// router registers for that path, one "METHOD /path" per line.
var httpFenceRe = regexp.MustCompile("(?s)```http\n(.*?)```")

type documentedRoute struct {
	Method string
	Path   string
	File   string
	Line   int
}

// documentedRoutes parses every endpoint fence under docs/api/.
func documentedRoutes(t *testing.T) []documentedRoute {
	t.Helper()
	var out []documentedRoute
	for _, f := range docFiles(t, "docs/api/*.md") {
		for _, m := range httpFenceRe.FindAllStringSubmatchIndex(f.Body, -1) {
			block := f.Body[m[2]:m[3]]
			for _, line := range strings.Split(block, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				parts := strings.Fields(line)
				if len(parts) != 2 {
					t.Errorf("%s:%d: %q is not \"METHOD /path\"", f.Path, lineOf(f.Body, m[2]), line)
					continue
				}
				out = append(out, documentedRoute{
					Method: parts[0],
					Path:   parts[1],
					File:   f.Path,
					Line:   lineOf(f.Body, m[2]),
				})
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no endpoint fences found under docs/api/; the fence convention or the docs changed shape")
	}
	return out
}

// chi expands a Handle registration (one that answers any method) into a separate
// walk entry per method. /metrics is the only such route in this router: promhttp
// answers whatever it is asked, and a scraper asks GET. A path registered this way
// is considered documented once, rather than demanding nine fences for one
// endpoint. Detection is by the full set, so a change in chi's method list fails
// closed — the route is reported as undocumented rather than quietly excused.
var allChiMethods = []string{
	http.MethodConnect, http.MethodDelete, http.MethodGet, http.MethodHead,
	http.MethodOptions, http.MethodPatch, http.MethodPost, http.MethodPut,
	http.MethodTrace,
}

func isCatchAll(methods map[string]bool) bool {
	for _, m := range allChiMethods {
		if !methods[m] {
			return false
		}
	}
	return true
}

// servedRoutes enumerates what a production-wired server actually serves, as
// path -> set of methods.
func servedRoutes(t *testing.T) map[string]map[string]bool {
	t.Helper()
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}
	srv, w := newTestServer(t, dataDir, walDir)
	defer w.Close()

	served := map[string]map[string]bool{}
	err := chi.Walk(srv.Router(), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		path := strings.TrimSuffix(route, "/")
		if served[path] == nil {
			served[path] = map[string]bool{}
		}
		served[path][method] = true
		return nil
	})
	if err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}
	if len(served) == 0 {
		t.Fatal("chi.Walk found no routes; the router changed shape")
	}
	return served
}

func TestEveryDocumentedRouteExists(t *testing.T) {
	served := servedRoutes(t)
	for _, d := range documentedRoutes(t) {
		methods, ok := served[d.Path]
		if !ok {
			t.Errorf("%s:%d documents %s %s, but the server serves no route at that path. Fix the fence, or add the route to internal/api/router.go.",
				d.File, d.Line, d.Method, d.Path)
			continue
		}
		if !methods[d.Method] {
			t.Errorf("%s:%d documents %s %s, but that path answers only %v",
				d.File, d.Line, d.Method, d.Path, slices.Sorted(maps.Keys(methods)))
		}
	}
}

func TestEveryRouteIsDocumented(t *testing.T) {
	documented := map[string]map[string]bool{} // path -> documented methods
	for _, d := range documentedRoutes(t) {
		if documented[d.Path] == nil {
			documented[d.Path] = map[string]bool{}
		}
		documented[d.Path][d.Method] = true
	}
	for path, methods := range servedRoutes(t) {
		docMethods, ok := documented[path]
		if !ok {
			t.Errorf("%s is served but documented nowhere under docs/api/. Add a fenced http block for it; the server's API is its contract.\nDocumented paths: %v",
				path, slices.Sorted(maps.Keys(documented)))
			continue
		}
		if isCatchAll(methods) {
			continue // answers every method; one documented fence is enough
		}
		for m := range methods {
			if !docMethods[m] {
				t.Errorf("%s %s is served but not documented; docs/api/ lists only %v for that path",
					m, path, slices.Sorted(maps.Keys(docMethods)))
			}
		}
	}
}

// Support tables are parsed, not read. Each row's Example cell is handed to the
// same parser the HTTP handler uses, and the Status cell must agree with what
// comes back. A row can otherwise be wrong for years: nothing else connects the
// documented query forms to metrics.ParseExpr.
const (
	limitationsDoc = "docs/api/limitations.md"
	promQLHeading  = "## PromQL subset"
	logQLHeading   = "## LogQL subset"

	// Row counts are asserted so that a formatting change which silently drops
	// rows fails instead of passing with fewer checks. Update deliberately when
	// adding a row.
	wantPromQLRows = 10
	wantLogQLRows  = 17

	// A pipe inside a LogQL expression is escaped for Markdown. Splitting cells
	// on "|" would split those too and silently skip every line-filter row, so
	// escaped pipes are parked on this placeholder first.
	escapedPipe = "\x00"
)


// sectionAfter returns the body of a Markdown section and its start offset. The
// section ends at the next heading of the same or higher level — stopping only at
// "## " would make a "### Metrics" section swallow the "### Logs" table that
// follows it.
func sectionAfter(body, heading string) (string, int) {
	start := strings.Index(body, heading)
	if start < 0 {
		return "", -1
	}
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	rest := body[start+len(heading):]
	next := regexp.MustCompile(`(?m)^#{1,` + strconv.Itoa(level) + `} `).FindStringIndex(rest)
	if next == nil {
		return body[start:], start
	}
	return body[start : start+len(heading)+next[0]], start
}

type supportRow struct {
	Form    string
	Example string
	Status  string
	File    string
	Line    int
}

var tableRowRe = regexp.MustCompile(`(?m)^\|(.+)\|\s*$`)

// supportRows returns the rows of the first Markdown table under heading.
func supportRows(t *testing.T, file, heading string) []supportRow {
	t.Helper()
	body := repoFile(t, file)
	section, start := sectionAfter(body, heading)
	if start < 0 {
		t.Fatalf("%s has no %q section", file, heading)
	}
	section = strings.ReplaceAll(section, `\|`, escapedPipe)

	var out []supportRow
	for _, m := range tableRowRe.FindAllStringSubmatchIndex(section, -1) {
		cells := strings.Split(section[m[2]:m[3]], "|")
		if len(cells) != 3 {
			continue // not a three-column support row
		}
		for i := range cells {
			cells[i] = strings.ReplaceAll(strings.TrimSpace(cells[i]), escapedPipe, "|")
		}
		form, example, status := cells[0], cells[1], cells[2]
		if form == "Form" || strings.HasPrefix(form, "---") {
			continue // header or separator
		}
		out = append(out, supportRow{
			Form: form, Example: strings.Trim(example, "`"), Status: status,
			File: file, Line: lineOf(body, start+m[0]),
		})
	}
	if len(out) == 0 {
		t.Fatalf("%s: no support rows parsed under %q; the table shape changed", file, heading)
	}
	return out
}

// parseLogQLAsHandlerDoes mirrors handleLokiQuery's dispatch exactly. Calling
// ParseLogQL alone would reject every count_over_time row and report correct
// documentation as broken.
func parseLogQLAsHandlerDoes(q string) error {
	if logs.IsLogExpression(q) {
		_, err := logs.ParseLogQL(q)
		return err
	}
	_, err := logs.ParseMetricQuery(q)
	if err == nil {
		return nil
	}
	if !errors.Is(err, logs.ErrNotMetricQuery) {
		return err
	}
	// A constant expression such as vector(1): supported on the instant endpoint.
	_, err = logs.ParseScalarQuery(q)
	return err
}

func checkSupportRows(t *testing.T, rows []supportRow, wantRows int, parse func(string) error) {
	t.Helper()
	if len(rows) != wantRows {
		t.Errorf("%s: parsed %d support rows, want %d. If you added or removed a row, update the constant in docs_test.go.",
			rows[0].File, len(rows), wantRows)
	}
	for _, r := range rows {
		err := parse(r.Example)
		switch {
		case strings.HasPrefix(r.Status, "Supported"):
			if err != nil {
				t.Errorf("%s:%d row %q documents %q as %s, but the parser rejects it: %v",
					r.File, r.Line, r.Form, r.Example, r.Status, err)
			}
		case r.Status == "Returns 400":
			if err == nil {
				t.Errorf("%s:%d row %q documents %q as Returns 400, but the parser accepts it. Either the doc is stale or an unsupported form silently started working.",
					r.File, r.Line, r.Form, r.Example)
			}
		default:
			t.Errorf("%s:%d row %q has Status %q; it must begin with \"Supported\" or be exactly \"Returns 400\"",
				r.File, r.Line, r.Form, r.Status)
		}
	}
}

func TestDocumentedQueryFormsMatchTheParser(t *testing.T) {
	t.Run("promql", func(t *testing.T) {
		checkSupportRows(t, supportRows(t, limitationsDoc, promQLHeading), wantPromQLRows, func(q string) error {
			_, err := metrics.ParseExpr(q)
			return err
		})
	})
	t.Run("logql", func(t *testing.T) {
		checkSupportRows(t, supportRows(t, limitationsDoc, logQLHeading), wantLogQLRows, parseLogQLAsHandlerDoes)
	})
}

// The README keeps a short table so the front door answers "can it do X" without
// a click. It must not be able to contradict the canonical list: every row it
// shows has to appear there verbatim.
func TestREADMESupportTableIsASubsetOfLimitations(t *testing.T) {
	canonical := map[string]string{} // example -> status
	for _, heading := range []string{promQLHeading, logQLHeading} {
		for _, r := range supportRows(t, limitationsDoc, heading) {
			canonical[r.Example] = r.Status
		}
	}

	var checked int
	for _, heading := range []string{"### Metrics", "### Logs"} {
		for _, r := range supportRows(t, "README.md", heading) {
			checked++
			status, ok := canonical[r.Example]
			if !ok {
				t.Errorf("README.md:%d shows %q, which is in no table in %s. The README summarises the canonical list; it does not extend it.",
					r.Line, r.Example, limitationsDoc)
				continue
			}
			if status != r.Status {
				t.Errorf("README.md:%d shows %q as %q; %s says %q",
					r.Line, r.Example, r.Status, limitationsDoc, status)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no README support rows were checked; the README table headings or shape changed")
	}
}

// allDocs is every Markdown file a reader might follow: the README and
// everything under docs/, plus the Helm values reference.
func allDocs(t *testing.T) []docFile {
	t.Helper()
	var out []docFile
	for _, glob := range []string{
		"README.md", "PERFORMANCE.md",
		"docs/api/*.md", "docs/architecture/*.md",
		"docs/runbooks/*.md", "docs/planning/*.md",
		"deployments/helm/README.md",
	} {
		if matches, _ := filepath.Glob(filepath.Join(docsRoot, glob)); len(matches) == 0 {
			continue
		}
		out = append(out, docFiles(t, glob)...)
	}
	if len(out) < 10 {
		t.Fatalf("allDocs found only %d documents; the globs no longer match the tree", len(out))
	}
	return out
}

// Markdown inline links. Image links share this shape and are checked the same
// way — a broken image path is a broken link too.
var linkRe = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

// headingSlug mirrors GitHub's anchor generation closely enough for this repo:
// lowercase, drop everything that is not alphanumeric, space, or hyphen, then
// turn runs of spaces into single hyphens.
var (
	nonAnchorRe = regexp.MustCompile(`[^a-z0-9 -]`)
	spaceRunRe  = regexp.MustCompile(`\s+`)
)

func headingSlug(text string) string {
	s := nonAnchorRe.ReplaceAllString(strings.ToLower(text), "")
	return strings.Trim(spaceRunRe.ReplaceAllString(strings.TrimSpace(s), "-"), "-")
}

func hasHeading(body, anchor string) bool {
	want := strings.ToLower(anchor)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "#") {
			continue
		}
		if headingSlug(strings.TrimSpace(strings.TrimLeft(line, "#"))) == want {
			return true
		}
	}
	return false
}

func TestDocsLinksResolve(t *testing.T) {
	var checked int
	for _, f := range allDocs(t) {
		dir := filepath.Dir(filepath.Join(docsRoot, f.Path))
		for _, m := range linkRe.FindAllStringSubmatchIndex(f.Body, -1) {
			target := f.Body[m[2]:m[3]]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			path, anchor, _ := strings.Cut(target, "#")
			line := lineOf(f.Body, m[0])
			checked++

			if path == "" { // same-file anchor
				if !hasHeading(f.Body, anchor) {
					t.Errorf("%s:%d links to #%s, which is not a heading in this file", f.Path, line, anchor)
				}
				continue
			}
			resolved := filepath.Join(dir, path)
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s:%d links to %q, which does not exist", f.Path, line, target)
				continue
			}
			if anchor == "" || !strings.HasSuffix(path, ".md") {
				continue
			}
			b, err := os.ReadFile(resolved)
			if err != nil {
				t.Errorf("%s:%d links into %q, which cannot be read: %v", f.Path, line, path, err)
				continue
			}
			if !hasHeading(string(b), anchor) {
				t.Errorf("%s:%d links to %q, but %q has no heading with that anchor", f.Path, line, target, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no relative links were checked; the link regexp or the docs changed shape")
	}
}

// commandSpans returns the text of every inline code span and fenced code block.
// Commands are only looked for here: scanning prose would match "make sure" and
// report `sure` as a missing target.
var (
	fenceRe     = regexp.MustCompile("(?s)```[a-z]*\n(.*?)```")
	codeSpanRe  = regexp.MustCompile("`([^`\n]+)`")
	makeUsageRe = regexp.MustCompile(`(?m)^\s*make ([a-z][a-z0-9-]*)`)
)

func commandSpans(body string) []string {
	var out []string
	for _, m := range fenceRe.FindAllStringSubmatch(body, -1) {
		out = append(out, m[1])
	}
	for _, m := range codeSpanRe.FindAllStringSubmatch(body, -1) {
		out = append(out, m[1])
	}
	return out
}

// Every `make <target>` a document tells a reader to run must exist. A runbook
// naming a target that was renamed sends the reader to "No rule to make target".
func TestDocumentedMakeTargetsExist(t *testing.T) {
	makefile := repoFile(t, "Makefile")
	var checked int
	for _, f := range allDocs(t) {
		for _, span := range commandSpans(f.Body) {
			for _, m := range makeUsageRe.FindAllStringSubmatch(span, -1) {
				target := m[1]
				checked++
				if !strings.Contains(makefile, "\n"+target+":") {
					t.Errorf("%s tells the reader to run `make %s`, which is not a target in the Makefile",
						f.Path, target)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no make targets were found in any document; the regexp changed shape")
	}
}

// Viper ignores unknown env vars silently, so a typo'd OBS_* key in a runbook is
// a setting the reader believes they changed and did not. Phase 5.2 enforces this
// on the Helm ConfigMap; prose was the remaining place a key could be wrong for
// free.
var obsEnvRe = regexp.MustCompile(`OBS_[A-Z0-9_]+`)

// OBS_* names that are correctly documented but are not backend Viper config.
// Each carries its reason, because an unexplained exclusion is how a real typo
// gets waved through later.
var nonBackendEnvKeys = map[string]string{
	"OBS_BACKEND_ADDR":    "the producers' target address, read by the sample app and load generator",
	"OBS_COMPOSE_PROJECT": "tests/e2e/compose_smoke.sh",
	"OBS_COMPOSE_KEEP_UP": "tests/e2e/compose_smoke.sh",
	"OBS_INSTANCE":        "producers chart, set from the downward API (see TestProducersCarryPodInstanceLabel)",
	"OBS_LOG_LEVLE":       "a deliberate misspelling in deployments/helm/README.md, showing that Viper ignores unknown keys; helm_test.go asserts that install succeeds with it",
}

func TestDocumentedConfigKeysAreReal(t *testing.T) {
	configSrc := repoFile(t, "internal/config/config.go")
	var checked int
	for _, f := range allDocs(t) {
		for _, m := range obsEnvRe.FindAllStringSubmatchIndex(f.Body, -1) {
			key := f.Body[m[0]:m[1]]
			if _, ok := nonBackendEnvKeys[key]; ok {
				continue
			}
			checked++
			want := `v.SetDefault("` + strings.ToLower(strings.TrimPrefix(key, "OBS_")) + `"`
			if !strings.Contains(configSrc, want) {
				t.Errorf("%s:%d documents %s, which has no %s) in internal/config/config.go. Viper ignores unknown env vars, so this setting would do nothing.",
					f.Path, lineOf(f.Body, m[0]), key, want)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no OBS_* keys found in any document; the regexp or the docs changed shape")
	}
}

// A document naming a metric the registry does not expose sends a reader to an
// empty query. The dashboard is already held to this rule; prose was not.
var obsMetricRe = regexp.MustCompile(`obs_[a-z0-9_]+`)

// Planning documents record what was considered, including metrics that were
// renamed or never built. They are excluded from this check for that reason, and
// no reference or runbook is.
var historicalMetricDocs = map[string]bool{
	"docs/planning/BACKLOG.md":            true,
	"docs/planning/ARCHITECTURE_NOTES.md": true,
}

// documentableMetricNames extends the queryable series names with histogram
// family base names. registeredMetricNames deliberately omits those, because a
// dashboard panel querying a bare histogram name renders empty — but prose
// naming the metric family is correct, and a runbook should say
// obs_http_request_duration_seconds, not obs_http_request_duration_seconds_bucket.
func documentableMetricNames(t *testing.T) map[string]bool {
	t.Helper()
	names := registeredMetricNames(t)
	out := maps.Clone(names)
	for n := range names {
		if base, ok := strings.CutSuffix(n, "_bucket"); ok {
			out[base] = true
		}
	}
	return out
}

func TestDocumentedMetricNamesAreRegistered(t *testing.T) {
	registered := documentableMetricNames(t)
	var checked int
	for _, f := range allDocs(t) {
		if historicalMetricDocs[f.Path] {
			continue
		}
		for _, m := range obsMetricRe.FindAllStringSubmatchIndex(f.Body, -1) {
			name := f.Body[m[0]:m[1]]
			checked++
			if !registered[name] {
				t.Errorf("%s:%d names the metric %q, which the registry does not expose. A reader querying it gets nothing back.",
					f.Path, lineOf(f.Body, m[0]), name)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no obs_* metric names found in any document; the regexp or the docs changed shape")
	}
}

// Example URLs are the lines a reader pastes first. This checks each one against
// the real router by asking whether it routes at all.
var curlURLRe = regexp.MustCompile(`https?://localhost:8080(/[^\s'"` + "`" + `\\]*)`)

func TestDocumentedCurlExamplesTargetRealRoutes(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}
	srv, w := newTestServer(t, dataDir, walDir)
	defer w.Close()

	// routes reports whether the router answers this path at all. A handler may
	// legitimately reply 400 (a missing query parameter); only 404 means the path
	// reaches nothing.
	routes := func(path string) bool {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
			if rr.Code != http.StatusNotFound {
				return true
			}
		}
		return false
	}

	var checked int
	for _, f := range allDocs(t) {
		for _, m := range curlURLRe.FindAllStringSubmatchIndex(f.Body, -1) {
			raw := f.Body[m[2]:m[3]]
			path, _, _ := strings.Cut(raw, "?")
			if path == "" || strings.Contains(path, "$") {
				continue // shell-interpolated path; nothing stable to check
			}
			checked++
			if !routes(path) {
				t.Errorf("%s:%d shows an example against %q, which the server does not route (404 on both GET and POST)",
					f.Path, lineOf(f.Body, m[0]), path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no localhost:8080 example URLs found; the regexp or the docs changed shape")
	}
}
