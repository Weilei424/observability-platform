package logs

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

type tenantAppender interface {
	Append(l StreamLabels, tsNs int64, line string) error
}

// fillTwoTenants holds {service="api"} for the default tenant ("default") and
// for team-b ("team-b"), plus a team-b-only {secret="x"} stream.
func fillTwoTenants(t *testing.T, s tenantAppender) {
	t.Helper()
	add := func(m map[string]string, id, line string) {
		l, err := NewStreamLabels(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Append(labels.WithTenant(l, id), 1000, line); err != nil {
			t.Fatal(err)
		}
	}
	add(map[string]string{"service": "api"}, tenant.Default, "default")
	add(map[string]string{"service": "api"}, "team-b", "team-b")
	add(map[string]string{"secret": "x"}, "team-b", "hidden")
}

// tenantReaders builds each local Reader the wrapper is deployed over, holding
// the same two-tenant data: a head, a chunk store fed by a flush, and a Store
// both flushed and holding fresh head data.
func tenantReaders(t *testing.T) map[string]Reader {
	t.Helper()
	out := map[string]Reader{}

	head, _ := openPolicyHead(t, nil, HeadOptions{}, 1<<30)
	fillTwoTenants(t, head)
	out["head"] = head

	cs, err := OpenChunkStore(filepath.Join(t.TempDir(), "chunks"), filepath.Join(t.TempDir(), "index"))
	if err != nil {
		t.Fatal(err)
	}
	src, _ := openPolicyHead(t, cs, HeadOptions{}, 1<<30)
	fillTwoTenants(t, src)
	if err := src.Flush(); err != nil {
		t.Fatal(err)
	}
	out["chunkstore"] = cs

	newStore := func() *Store {
		d := t.TempDir()
		s, err := NewStore(filepath.Join(d, "wal"), filepath.Join(d, "chunks"), filepath.Join(d, "index"), 1<<20, 1, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	flushed := newStore()
	fillTwoTenants(t, flushed)
	if err := flushed.Flush(); err != nil {
		t.Fatal(err)
	}
	out["store-flushed"] = flushed

	mixed := newStore()
	fillTwoTenants(t, mixed)
	if err := mixed.Flush(); err != nil {
		t.Fatal(err)
	}
	// Later timestamp so the head holds entries the chunks do not.
	for _, id := range []string{tenant.Default, "team-b"} {
		l, _ := NewStreamLabels(map[string]string{"service": "api"})
		if err := mixed.Append(labels.WithTenant(l, id), 2000, id+"-fresh"); err != nil {
			t.Fatal(err)
		}
	}
	out["store-mixed"] = mixed
	return out
}

func entryLines(sd StreamData) []string {
	var out []string
	for _, e := range sd.Entries {
		out = append(out, e.Line)
	}
	return out
}

func TestTenantSourceSeesOnlyItsOwnStreams(t *testing.T) {
	api := []index.Pair{{Name: "service", Value: "api"}}
	for name, r := range tenantReaders(t) {
		t.Run(name, func(t *testing.T) {
			src := TenantSource(r)
			for id, own := range map[string]string{tenant.Default: "default", "team-b": "team-b"} {
				got, err := src.SelectStreams(tenant.Inject(context.Background(), id), api, 0, 3000)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != 1 {
					t.Fatalf("%s: got %d streams, want 1: %+v", id, len(got), got)
				}
				if _, ok := got[0].Labels.Get(tenant.Label); ok {
					t.Fatalf("%s: the tenant label leaked into %v", id, got[0].Labels.Map())
				}
				for _, line := range entryLines(got[0]) {
					if line != own && line != id+"-fresh" {
						t.Fatalf("%s saw another tenant's line %q", id, line)
					}
				}
				if len(got[0].Entries) == 0 {
					t.Fatalf("%s: no entries", id)
				}
			}
		})
	}
}

func TestTenantSourceLabelNamesAndValuesArePerTenant(t *testing.T) {
	for name, r := range tenantReaders(t) {
		t.Run(name, func(t *testing.T) {
			src := TenantSource(r)
			def := tenant.Inject(context.Background(), tenant.Default)
			names, err := src.SelectLabelNames(def)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(names, []string{"service"}) {
				t.Fatalf("default tenant label names = %v", names)
			}
			b := tenant.Inject(context.Background(), "team-b")
			names, _ = src.SelectLabelNames(b)
			if !slices.Equal(names, []string{"secret", "service"}) {
				t.Fatalf("team-b label names = %v", names)
			}
			vals, _ := src.SelectLabelValues(def, "secret")
			if len(vals) != 0 {
				t.Fatalf("default tenant secret values = %v", vals)
			}
			vals, _ = src.SelectLabelValues(b, "secret")
			if !slices.Equal(vals, []string{"x"}) {
				t.Fatalf("team-b secret values = %v", vals)
			}
			if v, _ := src.SelectLabelValues(b, tenant.Label); len(v) != 0 {
				t.Fatalf("label values of __tenant__ = %v, want none", v)
			}
		})
	}
}

func TestTenantSourceSelectorNamingTheTenantLabelMatchesNothing(t *testing.T) {
	for name, r := range tenantReaders(t) {
		t.Run(name, func(t *testing.T) {
			src := TenantSource(r)
			m := []index.Pair{{Name: tenant.Label, Value: "team-b"}}
			// team-b too: its own label is internal, so naming it matches nothing
			// even when the value is its own tenant.
			for _, id := range []string{tenant.Default, "other", "team-b"} {
				got, err := src.SelectStreams(tenant.Inject(context.Background(), id), m, 0, 3000)
				if err != nil || len(got) != 0 {
					t.Fatalf("%s reading {__tenant__=\"team-b\"}: %v, %v", id, got, err)
				}
			}
		})
	}
}

// The label reads rest on MatchingStreamIDs(nil) returning every stream.
func TestEmptyMatchersMatchEveryStream(t *testing.T) {
	for name, r := range tenantReaders(t) {
		t.Run(name, func(t *testing.T) {
			if n := len(r.MatchingStreamIDs(nil)); n != 3 {
				t.Fatalf("MatchingStreamIDs(nil) = %d streams, want 3", n)
			}
		})
	}
}
