package integration_test

// docs/architecture/storage-layout.md claims to describe what the backend writes
// to disk. This drives the real write paths, walks what appears, and requires the
// document and the directory to agree in both directions.
//
// The Phase-0 layout section drifted precisely because nothing did this: it was
// headed "Recommended local data layout" and written before any of it existed.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/logs"
)

const storageLayoutDoc = "../../docs/architecture/storage-layout.md"

// Documented paths appear in the document as inline code spans opening a table
// row, rooted at data/. A variable segment is written <like-this>.
var layoutPathRe = regexp.MustCompile("(?m)^\\|\\s*`(data/[^`]+)`\\s*\\|")

// layoutPattern turns a documented path into an anchored regexp: <...> matches
// one path segment, everything else is literal.
func layoutPattern(t *testing.T, documented string) *regexp.Regexp {
	t.Helper()
	parts := regexp.MustCompile("<[^>]+>").Split(documented, -1)
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = regexp.QuoteMeta(p)
	}
	pattern := "^" + strings.Join(quoted, "[^/]+") + "$"
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("documented path %q compiles to an invalid pattern %q: %v", documented, pattern, err)
	}
	return re
}

func TestStorageLayoutDocMatchesDisk(t *testing.T) {
	docBytes, err := os.ReadFile(storageLayoutDoc)
	if err != nil {
		t.Fatalf("read %s: %v", storageLayoutDoc, err)
	}
	matches := layoutPathRe.FindAllStringSubmatch(string(docBytes), -1)
	if len(matches) == 0 {
		t.Fatalf("%s documents no data/ paths; the table shape changed", storageLayoutDoc)
	}
	documented := make(map[string]*regexp.Regexp, len(matches))
	for _, m := range matches {
		documented[m[1]] = layoutPattern(t, m[1])
	}

	dataDir := produceStorageTree(t)

	produced := walkTree(t, dataDir)
	if len(produced) == 0 {
		t.Fatal("the exercised write paths produced no files at all")
	}

	diffLayout(t, documented, produced, "the backend")
}

// walkTree walks dataDir and returns every path it finds, relative to dataDir
// and rooted at "data/" the way the storage-layout doc's rows are written.
// Directory entries end in "/".
func walkTree(t *testing.T, dataDir string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	err := filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(dataDir, path)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		entry := "data/" + filepath.ToSlash(rel)
		if d.IsDir() {
			entry += "/"
		}
		out[entry] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dataDir, err)
	}
	return out
}

// diffLayout fails t unless every path in produced matches some pattern in
// documented, and every pattern in documented matches at least one path in
// produced — the two-direction check every storage-layout test needs. label
// names whoever wrote produced (e.g. "the backend", "the ingester"), and each
// documented key is used exactly as given in the error text, so a caller that
// wants a component prefix in the message (e.g. "ingester:data/metrics/")
// passes documented keyed that way while still matching plain "data/..."
// entries: the key only decides what gets printed, the regexp decides what
// matches.
func diffLayout(t *testing.T, documented map[string]*regexp.Regexp, produced map[string]bool, label string) {
	t.Helper()
	matched := map[string]bool{}
	for entry := range produced {
		var ok bool
		for doc, re := range documented {
			if re.MatchString(entry) {
				ok, matched[doc] = true, true
			}
		}
		if !ok {
			t.Errorf("%s wrote %q, which %s does not document. Add a row for it.", label, entry, storageLayoutDoc)
		}
	}
	for doc := range documented {
		if !matched[doc] {
			t.Errorf("%s documents %q, which %s never produced. Either the path is gone, or the test does not exercise the code that writes it — say which in the row.", storageLayoutDoc, doc, label)
		}
	}
}

// produceStorageTree exercises the metrics and logs write paths against a fresh
// data directory and returns it. Anything it does not exercise cannot be
// documented as produced.
func produceStorageTree(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}

	// --- metrics: ingest over HTTP, then flush a block ---
	srv, w := newWALServer(t, dataDir, walDir)
	payload := buildIngestPayload(t, "layout_counter", map[string]string{"env": "test"}, 120)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/metrics", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ingest status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
	ws := buildWALStore(t, dataDir, walDir)
	if _, err := ws.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// --- logs: append and flush a chunk ---
	logsDir := filepath.Join(dataDir, "logs")
	store, err := logs.NewStore(
		filepath.Join(logsDir, "wal"),
		filepath.Join(logsDir, "chunks"),
		filepath.Join(logsDir, "index"),
		128<<20, 1, 8<<20,
	)
	if err != nil {
		t.Fatalf("logs.NewStore: %v", err)
	}
	defer store.Close()
	labels, err := logs.NewStreamLabels(map[string]string{"service": "layout-test"})
	if err != nil {
		t.Fatalf("NewStreamLabels: %v", err)
	}
	for i := range 50 {
		if err := store.Append(labels, int64(i+1)*1_000_000, "storage layout line"); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("logs Flush: %v", err)
	}
	return dataDir
}
