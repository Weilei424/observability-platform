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
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
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
