// internal/storage/index/postings_test.go
package index

import (
	"reflect"
	"testing"
)

func pairs(kv ...string) []Pair {
	var out []Pair
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, Pair{Name: kv[i], Value: kv[i+1]})
	}
	return out
}

func TestMemPostings_AddAndPostingsSorted(t *testing.T) {
	p := NewMemPostings()
	p.Add(30, pairs("__name__", "http", "job", "api"))
	p.Add(10, pairs("__name__", "http", "job", "api"))
	p.Add(20, pairs("__name__", "http", "job", "web"))

	if got := p.Postings("__name__", "http"); !reflect.DeepEqual(got, []uint64{10, 20, 30}) {
		t.Fatalf("__name__=http postings = %v, want [10 20 30]", got)
	}
	if got := p.Postings("job", "api"); !reflect.DeepEqual(got, []uint64{10, 30}) {
		t.Fatalf("job=api postings = %v, want [10 30]", got)
	}
}

func TestMemPostings_AddIdempotent(t *testing.T) {
	p := NewMemPostings()
	p.Add(5, pairs("job", "api"))
	p.Add(5, pairs("job", "api"))
	if got := p.Postings("job", "api"); !reflect.DeepEqual(got, []uint64{5}) {
		t.Fatalf("idempotent add = %v, want [5]", got)
	}
}

func TestMemPostings_SelectIntersection(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("__name__", "http", "job", "api", "env", "prod"))
	p.Add(2, pairs("__name__", "http", "job", "api", "env", "dev"))
	p.Add(3, pairs("__name__", "http", "job", "web", "env", "prod"))

	got := p.Select(pairs("__name__", "http", "job", "api"))
	if !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("select = %v, want [1 2]", got)
	}
}

func TestMemPostings_SelectEmptyMatchersReturnsAll(t *testing.T) {
	p := NewMemPostings()
	p.Add(7, pairs("job", "api"))
	p.Add(3, pairs("job", "web"))
	if got := p.Select(nil); !reflect.DeepEqual(got, []uint64{3, 7}) {
		t.Fatalf("select(nil) = %v, want [3 7]", got)
	}
}

func TestMemPostings_SelectUnknownPairEmpty(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("job", "api"))
	if got := p.Select(pairs("job", "nope")); len(got) != 0 {
		t.Fatalf("select unknown = %v, want empty", got)
	}
}

func TestMemPostings_ReturnedSlicesAreCopies(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("job", "api"))
	p.Add(2, pairs("job", "api"))
	p.Add(3, pairs("job", "web"))

	// Test Select(nil) returns a copy
	firstSelect := p.Select(nil)
	if !reflect.DeepEqual(firstSelect, []uint64{1, 2, 3}) {
		t.Fatalf("select(nil) = %v, want [1 2 3]", firstSelect)
	}
	// Mutate the returned slice
	firstSelect = append(firstSelect, 999)
	// Re-fetch and verify the internal state is unaffected
	secondSelect := p.Select(nil)
	if !reflect.DeepEqual(secondSelect, []uint64{1, 2, 3}) {
		t.Fatalf("select(nil) after mutation = %v, want [1 2 3]", secondSelect)
	}

	// Test Postings returns a copy
	firstPostings := p.Postings("job", "api")
	if !reflect.DeepEqual(firstPostings, []uint64{1, 2}) {
		t.Fatalf("postings(job,api) = %v, want [1 2]", firstPostings)
	}
	// Mutate the returned slice
	firstPostings = append(firstPostings, 888)
	// Re-fetch and verify the internal state is unaffected
	secondPostings := p.Postings("job", "api")
	if !reflect.DeepEqual(secondPostings, []uint64{1, 2}) {
		t.Fatalf("postings(job,api) after mutation = %v, want [1 2]", secondPostings)
	}

	// Test empty pair returns a copy
	firstEmptyPair := p.Postings("", "")
	if !reflect.DeepEqual(firstEmptyPair, []uint64{1, 2, 3}) {
		t.Fatalf("postings(empty,empty) = %v, want [1 2 3]", firstEmptyPair)
	}
	// Mutate the returned slice
	firstEmptyPair = append(firstEmptyPair, 777)
	// Re-fetch and verify the internal state is unaffected
	secondEmptyPair := p.Postings("", "")
	if !reflect.DeepEqual(secondEmptyPair, []uint64{1, 2, 3}) {
		t.Fatalf("postings(empty,empty) after mutation = %v, want [1 2 3]", secondEmptyPair)
	}
}

func TestMemPostings_Delete(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("job", "api"))
	p.Add(2, pairs("job", "api"))
	p.Delete(1, pairs("job", "api"))
	if got := p.Postings("job", "api"); len(got) != 1 || got[0] != 2 {
		t.Fatalf("after delete = %v, want [2]", got)
	}
	if got := p.Select(nil); len(got) != 1 || got[0] != 2 {
		t.Fatalf("allRefs after delete = %v, want [2]", got)
	}
}

func TestMemPostings_Delete_RemovesStaleLabelMetadata(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("__name__", "http", "job", "api"))
	p.Delete(1, pairs("__name__", "http", "job", "api"))

	if got := p.LabelNames(); len(got) != 0 {
		t.Errorf("LabelNames after deleting all series = %v, want empty", got)
	}
	if got := p.LabelNameCount(); got != 0 {
		t.Errorf("LabelNameCount after deleting all series = %d, want 0", got)
	}
	if got := p.LabelPairCount(); got != 0 {
		t.Errorf("LabelPairCount after deleting all series = %d, want 0", got)
	}
	if got := p.SeriesCount(); got != 0 {
		t.Errorf("SeriesCount after deleting all series = %d, want 0", got)
	}
	if got := p.LabelValues("job"); len(got) != 0 {
		t.Errorf("LabelValues(job) after deleting all series = %v, want empty", got)
	}
}

func TestMemPostings_LabelNamesAndValuesSorted(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("__name__", "http", "job", "web"))
	p.Add(2, pairs("__name__", "http", "job", "api"))
	if got := p.LabelNames(); !reflect.DeepEqual(got, []string{"__name__", "job"}) {
		t.Fatalf("LabelNames = %v, want [__name__ job]", got)
	}
	if got := p.LabelValues("job"); !reflect.DeepEqual(got, []string{"api", "web"}) {
		t.Fatalf("LabelValues(job) = %v, want [api web]", got)
	}
	if got := p.LabelValues("missing"); len(got) != 0 {
		t.Fatalf("LabelValues(missing) = %v, want empty", got)
	}
}

func TestMemPostings_Cardinality(t *testing.T) {
	p := NewMemPostings()
	p.Add(1, pairs("__name__", "http", "job", "api"))
	p.Add(2, pairs("__name__", "http", "job", "web"))
	if got := p.SeriesCount(); got != 2 {
		t.Fatalf("SeriesCount = %d, want 2", got)
	}
	if got := p.LabelNameCount(); got != 2 { // __name__, job
		t.Fatalf("LabelNameCount = %d, want 2", got)
	}
	if got := p.LabelPairCount(); got != 3 { // __name__=http, job=api, job=web
		t.Fatalf("LabelPairCount = %d, want 3", got)
	}
}
