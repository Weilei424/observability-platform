package tenant

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := []string{"team-b", "a", "A_b.c*d'e(f)g!h", strings.Repeat("x", 150), Default}
	for _, id := range ok {
		if err := Validate(id); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", id, err)
		}
	}
	bad := []string{"", ".", "..", strings.Repeat("x", 151), "a b", "a/b", "é", "a|b"}
	for _, id := range bad {
		if err := Validate(id); err == nil {
			t.Errorf("Validate(%q) = nil, want an error", id)
		}
	}
}

func TestFromDefaultsAndInjectRoundTrips(t *testing.T) {
	if got := From(context.Background()); got != Default {
		t.Fatalf("From(empty) = %q, want %q", got, Default)
	}
	if got := From(Inject(context.Background(), "team-b")); got != "team-b" {
		t.Fatalf("From = %q", got)
	}
}

func TestResolve(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    *string
		required bool
		wantID   string
		wantCode int
	}{
		{"absent, optional", nil, false, Default, 0},
		{"absent, required", nil, true, "", http.StatusUnauthorized},
		{"empty is absent", ptr(""), false, Default, 0},
		{"empty is absent when required", ptr(""), true, "", http.StatusUnauthorized},
		{"named", ptr("team-b"), true, "team-b", 0},
		{"padded", ptr("  team-b "), false, "team-b", 0},
		{"explicit default", ptr("anonymous"), true, Default, 0},
		{"invalid", ptr("a b"), false, "", http.StatusBadRequest},
		{"federation", ptr("a|b"), false, "", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.value != nil {
				h.Set(Header, *tc.value)
			}
			id, code, _ := Resolve(h, tc.required)
			if id != tc.wantID || code != tc.wantCode {
				t.Fatalf("Resolve = (%q, %d), want (%q, %d)", id, code, tc.wantID, tc.wantCode)
			}
		})
	}
}

func ptr(s string) *string { return &s }
