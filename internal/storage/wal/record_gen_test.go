package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTypeTwoRecordRoundTrips(t *testing.T) {
	labels := []LabelPair{{"__name__", "m"}, {"job", "x"}}
	body := encodeRecordGen(labels, 1234, 5.5, 1<<61)[4:]
	if body[0] != recordTypeSampleGen {
		t.Fatalf("type byte = %#x, want %#x", body[0], recordTypeSampleGen)
	}
	got, ts, v, gen, ok := decodeRecord(body)
	if !ok || ts != 1234 || v != 5.5 || gen != 1<<61 || len(got) != 2 || got[1].Value != "x" {
		t.Fatalf("decode = %v %d %v %d %v", got, ts, v, gen, ok)
	}
}

func TestTypeOneRecordDecodesWithGenerationZero(t *testing.T) {
	body := encodeRecord([]LabelPair{{"__name__", "m"}}, 7, 1)[4:] // the pre-6.2 encoder
	_, ts, _, gen, ok := decodeRecord(body)
	if !ok || ts != 7 || gen != 0 {
		t.Fatalf("type-1 decode: ts %d gen %d ok %v; want 7, 0, true", ts, gen, ok)
	}
}

func TestTypeTwoTruncatedOrPaddedIsCorrupt(t *testing.T) {
	body := encodeRecordGen([]LabelPair{{"__name__", "m"}}, 1, 1, 9)[4:]
	if _, _, _, _, ok := decodeRecord(body[:len(body)-1]); ok {
		t.Error("truncated type-2 record decoded")
	}
	if _, _, _, _, ok := decodeRecord(append(body, 0)); ok {
		t.Error("padded type-2 record decoded")
	}
}

// A WAL written before 6.2 (type 1) and continued after (type 2) replays both,
// in order, with the type-1 records' generation reported as 0.
func TestReplayMixedRecordTypes(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "000001.wal")
	data := append(encodeRecord([]LabelPair{{"__name__", "old"}}, 1, 1),
		encodeRecordGen([]LabelPair{{"__name__", "new"}}, 2, 2, 42)...)
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}
	type rec struct {
		name string
		gen  int64
	}
	var got []rec
	if err := ReplayFromGen(dir, 0, func(l []LabelPair, _ int64, _ float64, gen int64) {
		got = append(got, rec{l[0].Value, gen})
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (rec{"old", 0}) || got[1] != (rec{"new", 42}) {
		t.Fatalf("replayed %v, want [{old 0} {new 42}]", got)
	}
}

func TestWriteRecordGenIsReplayedExactly(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRecordGen([]LabelPair{{"__name__", "m"}}, 10, 3, 1_700_000_000_000_000); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRecord([]LabelPair{{"__name__", "m"}}, 11, 4); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	var gens []int64
	if err := ReplayFromGen(dir, 0, func(_ []LabelPair, _ int64, _ float64, gen int64) { gens = append(gens, gen) }); err != nil {
		t.Fatal(err)
	}
	if len(gens) != 2 || gens[0] != 1_700_000_000_000_000 || gens[1] != 0 {
		t.Fatalf("gens = %v, want [1700000000000000 0]", gens)
	}
}
