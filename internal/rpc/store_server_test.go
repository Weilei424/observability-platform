package rpc_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

type storeFixture struct {
	srv    *httptest.Server
	blocks *metrics.BlockStore
	chunks *logs.ChunkStore
}

func newStoreFixture(t *testing.T) storeFixture {
	t.Helper()
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	cs, err := logs.OpenChunkStore(filepath.Join(dir, "logs", "chunks"), filepath.Join(dir, "logs", "index"))
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountReads(r, bs, logs.AsSource(cs))
		rpc.MountStore(r, bs, cs)
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return storeFixture{srv: srv, blocks: bs, chunks: cs}
}

func sealedChunkBytes(t *testing.T, base int64) string {
	t.Helper()
	c := chunk.NewChunk()
	for i := range 120 {
		if err := c.Append(base+int64(i)*1000, float64(i), int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	return base64.StdEncoding.EncodeToString(c.Bytes())
}

func TestMetricsFlushRouteIngestsABlock(t *testing.T) {
	f := newStoreFixture(t)
	body := fmt.Sprintf(`{"series":[{"labels":{"__name__":"flushed"},"chunks":[%q]}]}`, sealedChunkBytes(t, 0))
	resp, out := post(t, f.srv.URL+"/internal/v1/metrics/flush", body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(out, `"samples":120`) {
		t.Fatalf("status %d body %s, want 200 with 120 samples", resp.StatusCode, out)
	}
	l, _ := metrics.NewLabels(map[string]string{"__name__": "flushed"})
	if got, _ := f.blocks.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000); len(got) != 120 {
		t.Fatalf("store holds %d samples after flush-in, want 120", len(got))
	}
}

func TestMetricsFlushRouteRefusesBadInput(t *testing.T) {
	f := newStoreFixture(t)
	good := sealedChunkBytes(t, 0)
	for name, body := range map[string]string{
		"no series":      `{"series":[]}`,
		"no chunks":      `{"series":[{"labels":{"__name__":"x"},"chunks":[]}]}`,
		"corrupt chunk":  `{"series":[{"labels":{"__name__":"x"},"chunks":["AAAA"]}]}`,
		"invalid labels": fmt.Sprintf(`{"series":[{"labels":{"not a name":"x"},"chunks":[%q]}]}`, good),
		"unknown field":  `{"series":[],"extra":1}`,
	} {
		if resp, out := post(t, f.srv.URL+"/internal/v1/metrics/flush", body); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s), want 400", name, resp.StatusCode, out)
		}
	}
	huge := `{"series":[{"labels":{"__name__":"x"},"chunks":["` + strings.Repeat("A", rpc.FlushBodyLimit) + `"]}]}`
	if resp, _ := post(t, f.srv.URL+"/internal/v1/metrics/flush", huge); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized flush = %d, want 413", resp.StatusCode)
	}
}

// TestMetricsFlushRouteRefusesDuplicateSeriesInOneFlush pins the controller
// ruling carried from Task 6: IngestSeriesChunks wraps a caller error (here,
// the same series appearing twice in one flush) in ErrInvalidSeriesChunks, and
// the handler must answer 400 for that — not 500 — and must register no block.
func TestMetricsFlushRouteRefusesDuplicateSeriesInOneFlush(t *testing.T) {
	f := newStoreFixture(t)
	good := sealedChunkBytes(t, 0)
	body := fmt.Sprintf(`{"series":[{"labels":{"__name__":"dup"},"chunks":[%q]},{"labels":{"__name__":"dup"},"chunks":[%q]}]}`, good, good)
	resp, out := post(t, f.srv.URL+"/internal/v1/metrics/flush", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d body %s, want 400 for a flush carrying the same series twice", resp.StatusCode, out)
	}

	listResp, err := http.Get(f.srv.URL + "/internal/v1/metrics/blocks")
	if err != nil {
		t.Fatal(err)
	}
	defer listResp.Body.Close()
	var list struct {
		Blocks []struct {
			ID string `json:"id"`
		} `json:"blocks"`
	}
	_ = json.NewDecoder(listResp.Body).Decode(&list)
	if len(list.Blocks) != 0 {
		t.Fatalf("blocks = %+v, want none registered after a refused flush", list.Blocks)
	}
}

func TestLogsFlushRoute(t *testing.T) {
	f := newStoreFixture(t)
	resp, out := post(t, f.srv.URL+"/internal/v1/logs/flush",
		`{"streams":[{"labels":{"service":"api"},"entries":[[1,"a"],[2,"b"]]}]}`)
	if resp.StatusCode != http.StatusOK || !strings.Contains(out, `"entries":2`) {
		t.Fatalf("status %d body %s", resp.StatusCode, out)
	}
	l, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	if got, _ := f.chunks.StreamEntries(t.Context(), logs.StreamIDOf(l), 0, 10); len(got) != 2 {
		t.Fatalf("store holds %d entries, want 2", len(got))
	}

	invalidUTF8 := "{\"streams\":[{\"labels\":{\"service\":\"api\"},\"entries\":[[1,\"\xff\"]]}]}"
	if resp, _ := post(t, f.srv.URL+"/internal/v1/logs/flush", invalidUTF8); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid UTF-8 = %d, want 400 (never a silent U+FFFD)", resp.StatusCode)
	}
	if resp, _ := post(t, f.srv.URL+"/internal/v1/logs/flush", `{"streams":[{"labels":{"service":"api"},"entries":[[0,"a"]]}]}`); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("non-positive timestamp = %d, want 400", resp.StatusCode)
	}
}

func TestBlockMaintenanceRoutes(t *testing.T) {
	f := newStoreFixture(t)
	for _, base := range []int64{0, 200_000} {
		body := fmt.Sprintf(`{"series":[{"labels":{"__name__":"m"},"chunks":[%q]}]}`, sealedChunkBytes(t, base))
		if resp, out := post(t, f.srv.URL+"/internal/v1/metrics/flush", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("flush: %d %s", resp.StatusCode, out)
		}
	}
	resp, err := http.Get(f.srv.URL + "/internal/v1/metrics/blocks")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Blocks []struct {
			ID string `json:"id"`
		} `json:"blocks"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list.Blocks) != 2 {
		t.Fatalf("blocks = %+v, want 2", list.Blocks)
	}
	group := fmt.Sprintf(`{"groups":[[%q,%q]]}`, list.Blocks[0].ID, list.Blocks[1].ID)
	if resp, out := post(t, f.srv.URL+"/internal/v1/metrics/compact", group); resp.StatusCode != http.StatusOK || !strings.Contains(out, `"compacted":1`) {
		t.Fatalf("compact: %d %s", resp.StatusCode, out)
	}
	// A group naming a block that no longer exists is skipped, not an error.
	if resp, out := post(t, f.srv.URL+"/internal/v1/metrics/compact", group); resp.StatusCode != http.StatusOK || !strings.Contains(out, `"compacted":0`) {
		t.Fatalf("stale compact: %d %s", resp.StatusCode, out)
	}
	if resp, out := post(t, f.srv.URL+"/internal/v1/metrics/retention", `{"now_ms":10000000000000,"retention_ms":1}`); resp.StatusCode != http.StatusOK || !strings.Contains(out, `"deleted":1`) {
		t.Fatalf("retention: %d %s", resp.StatusCode, out)
	}
	if resp, _ := post(t, f.srv.URL+"/internal/v1/metrics/retention", `{"now_ms":0,"retention_ms":-1}`); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative retention = %d, want 400", resp.StatusCode)
	}
}
