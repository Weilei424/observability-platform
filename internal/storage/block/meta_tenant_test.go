package block_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

func TestMeta_PreTenantBlockReadsAsDefaultTenant(t *testing.T) {
	dir := t.TempDir()
	old := `{"block_id":"b1","min_time":1,"max_time":2,"num_series":1,"num_samples":2,"created_at":"2026-01-01T00:00:00Z","level":1,"max_gen":0}`
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := block.ReadMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Tenant != "" {
		t.Fatalf("Tenant = %q, want empty", m.Tenant)
	}
}

func TestMeta_TenantRoundTrips(t *testing.T) {
	w, blocks, _ := makeWriter(t)
	w.SetTenant("team-b")
	if err := w.AddSeries(1, []block.LabelPair{{Name: "__name__", Value: "x"}},
		[]*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatal(err)
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Tenant != "team-b" {
		t.Fatalf("commit meta Tenant = %q", meta.Tenant)
	}
	got, err := block.ReadMeta(filepath.Join(blocks, meta.BlockID))
	if err != nil {
		t.Fatal(err)
	}
	if got.Tenant != "team-b" {
		t.Fatalf("read Tenant = %q, want team-b", got.Tenant)
	}
}
