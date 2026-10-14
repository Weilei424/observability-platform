package logs

import "testing"

func TestNewStoredStreamLabels(t *testing.T) {
	if _, err := NewStoredStreamLabels(map[string]string{"service": "api", "__tenant__": "team-b"}); err != nil {
		t.Fatalf("tenant label: %v", err)
	}
	if _, err := NewStoredStreamLabels(map[string]string{}); err == nil {
		t.Fatal("empty set must still be refused")
	}
	if _, err := NewStreamLabels(map[string]string{"service": "api", "__tenant__": "team-b"}); err == nil {
		t.Fatal("public constructor must refuse __tenant__")
	}
}
