package config

import (
	"strings"
	"testing"
)

func TestCanonicalPeerURLNormalizes(t *testing.T) {
	for raw, want := range map[string]string{
		"HTTP://Store-2.:8080/":      "http://store-2:8080",
		"http://store:80":            "http://store",
		"https://store:443/":         "https://store",
		"http://[0:0:0:0:0:0:0:1]:9": "http://[::1]:9",
		"http://[::ffff:10.0.0.1]:9": "http://10.0.0.1:9",
	} {
		got, err := CanonicalPeerURL(raw)
		if err != nil || got != want {
			t.Errorf("CanonicalPeerURL(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
}

func TestCanonicalPeerURLRefuses(t *testing.T) {
	for _, raw := range []string{
		"", "ftp://a:1", "http://", "http://a:1/x", "http://a:1?q=1", "http://a:1?", "http://a:1#", "a:1", "http://u:p@a:1",
	} {
		if got, err := CanonicalPeerURL(raw); err == nil {
			t.Errorf("CanonicalPeerURL(%q) = %q, want an error", raw, got)
		}
	}
}

func TestCanonicalPeerURLWithholdsCredentials(t *testing.T) {
	for _, raw := range []string{"http://user:s3cret@a:1", "http://user:s3cret pass@a:1", "http://user:%zz@a:1"} {
		_, err := CanonicalPeerURL(raw)
		if err == nil {
			t.Fatalf("CanonicalPeerURL(%q) accepted credentials", raw)
		}
		if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "%zz") {
			t.Errorf("error for %q leaks the credentials: %v", raw, err)
		}
	}
}
