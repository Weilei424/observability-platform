package labels

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/tenant"
)

func TestWithTenantAndTenantOf(t *testing.T) {
	l, err := New(map[string]string{"__name__": "up", "job": "a"})
	if err != nil {
		t.Fatal(err)
	}
	if got := WithTenant(l, tenant.Default); got.Hash() != l.Hash() || TenantOf(got) != tenant.Default {
		t.Fatalf("default tenant must leave the labels unchanged")
	}
	b := WithTenant(l, "team-b")
	if v, _ := b.Get(tenant.Label); v != "team-b" || TenantOf(b) != "team-b" {
		t.Fatalf("WithTenant(team-b) = %v", b.Map())
	}
	if b.Hash() == l.Hash() {
		t.Fatal("a tenant's series must have its own identity")
	}
	if got := Without(b, tenant.Label); got.Hash() != l.Hash() {
		t.Fatalf("Without = %v, want %v", got.Map(), l.Map())
	}
}

func TestPublicNewRefusesTheTenantLabel(t *testing.T) {
	if _, err := New(map[string]string{"__name__": "up", tenant.Label: "team-b"}); err == nil {
		t.Fatal("a client must not send __tenant__")
	}
}

func TestNewStored(t *testing.T) {
	if _, err := NewStored(map[string]string{"__name__": "up", tenant.Label: "team-b"}); err != nil {
		t.Fatalf("stored labels with a tenant: %v", err)
	}
	for _, m := range []map[string]string{
		{"__name__": "up", tenant.Label: "anonymous"}, // the default tenant has no label
		{"__name__": "up", tenant.Label: "a b"},       // invalid ID
		{"__name__": "up", "__other__": "x"},          // other reserved names stay refused
	} {
		if _, err := NewStored(m); err == nil {
			t.Errorf("NewStored(%v) = nil error, want refusal", m)
		}
	}
}
