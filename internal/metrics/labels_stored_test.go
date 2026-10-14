package metrics

import "testing"

func TestNewStoredLabels(t *testing.T) {
	if _, err := NewStoredLabels(map[string]string{"__name__": "up", "__tenant__": "team-b"}); err != nil {
		t.Fatalf("tenant label: %v", err)
	}
	if _, err := NewStoredLabels(map[string]string{"__tenant__": "team-b"}); err == nil {
		t.Fatal("missing __name__ must still be refused")
	}
	if _, err := NewLabels(map[string]string{"__name__": "up", "__tenant__": "team-b"}); err == nil {
		t.Fatal("public constructor must refuse __tenant__")
	}
}
