package labels_test

import (
	"errors"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/labels"
)

func TestNew_SameLabelsDifferentOrder_SameHash(t *testing.T) {
	a, err := labels.New(map[string]string{"__name__": "http_requests", "service": "api", "env": "prod"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, err := labels.New(map[string]string{"env": "prod", "service": "api", "__name__": "http_requests"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Hash() != b.Hash() {
		t.Errorf("expected same hash, got %d vs %d", a.Hash(), b.Hash())
	}
}

func TestNew_DifferentValueNameOrExtra_DifferentHash(t *testing.T) {
	base, _ := labels.New(map[string]string{"__name__": "http_requests", "service": "api"})
	value, _ := labels.New(map[string]string{"__name__": "http_requests", "service": "web"})
	name, _ := labels.New(map[string]string{"__name__": "http_requests", "env": "api"})
	extra, _ := labels.New(map[string]string{"__name__": "http_requests", "service": "api", "env": "prod"})
	for _, other := range []labels.Labels{value, name, extra} {
		if base.Hash() == other.Hash() {
			t.Error("expected different hash for a differing label set")
		}
	}
}

func TestNew_GoldenHash(t *testing.T) {
	// Migration guard: must match the value the metrics package produced before
	// the algorithm moved here. Any change breaks persisted SeriesIDs.
	l, err := labels.New(map[string]string{"__name__": "http_requests", "service": "api"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want uint64 = 9696857623413696903
	if l.Hash() != want {
		t.Errorf("golden hash changed: got %d, want %d", l.Hash(), want)
	}
}

func TestNew_NameLabelNotRequired(t *testing.T) {
	// A log-style stream label set with no __name__ must be accepted.
	if _, err := labels.New(map[string]string{"service": "api", "level": "error"}); err != nil {
		t.Errorf("unexpected error for label set without __name__: %v", err)
	}
}

func TestNew_NameLabelAllowed(t *testing.T) {
	if _, err := labels.New(map[string]string{"__name__": "anything", "service": "api"}); err != nil {
		t.Errorf("__name__ must be permitted as an ordinary label: %v", err)
	}
}

func TestNew_ValidationErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"empty name":      {"service": "api", "": "v"},
		"invalid name":    {"service": "api", "123bad": "v"},
		"hyphen name":     {"service": "api", "bad-name": "v"},
		"reserved prefix": {"service": "api", "__job__": "v"},
		"invalid utf8":    {"service": string([]byte{0xff, 0xfe})},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := labels.New(m)
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			var ve *labels.ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("expected *ValidationError, got %T", err)
			}
		})
	}
}

func TestNew_EmptyValueAccepted(t *testing.T) {
	if _, err := labels.New(map[string]string{"service": ""}); err != nil {
		t.Errorf("empty label value must be accepted: %v", err)
	}
}

func TestNew_TooManyLabels_Rejected(t *testing.T) {
	m := make(map[string]string, 300)
	for i := 0; i < 300; i++ {
		m["label_"+itoa(i)] = "v"
	}
	if _, err := labels.New(m); err == nil {
		t.Fatal("expected error for >255 labels")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestGetAndMap(t *testing.T) {
	l, _ := labels.New(map[string]string{"__name__": "http_requests", "service": "api"})
	if v, ok := l.Get("service"); !ok || v != "api" {
		t.Errorf("Get(service) = (%q, %v)", v, ok)
	}
	if _, ok := l.Get("missing"); ok {
		t.Error("Get(missing) should be false")
	}
	m := l.Map()
	m["service"] = "mutated"
	if v, _ := l.Get("service"); v != "api" {
		t.Error("Map() must return a copy")
	}
	if l.Len() != 2 {
		t.Errorf("Len() = %d, want 2", l.Len())
	}
}

func TestNewUnvalidated_NoNameRequired(t *testing.T) {
	l := labels.NewUnvalidated(map[string]string{"service": "api"})
	if v, ok := l.Get("service"); !ok || v != "api" {
		t.Errorf("NewUnvalidated Get(service) = (%q, %v)", v, ok)
	}
}
