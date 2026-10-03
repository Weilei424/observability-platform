package config

import (
	"strings"
	"testing"
)

func topo(target Target, ingester, store, querier string) *Config {
	return &Config{Target: target, IngesterURL: ingester, StoreURL: store, QuerierURL: querier}
}

func TestIngesterURLListNormalized(t *testing.T) {
	c := topo(TargetQuerier, "http://i-0:8080/, http://i-1:8080 ,http://i-2:8080", "http://store:8080", "")
	if err := c.validateTopology(); err != nil {
		t.Fatal(err)
	}
	want := []string{"http://i-0:8080", "http://i-1:8080", "http://i-2:8080"}
	if strings.Join(c.IngesterURLs, " ") != strings.Join(want, " ") {
		t.Errorf("IngesterURLs = %v, want %v", c.IngesterURLs, want)
	}
	if c.IngesterURL != strings.Join(want, ",") {
		t.Errorf("IngesterURL = %q", c.IngesterURL)
	}
}

func TestIngesterURLSingleIsAOneMemberList(t *testing.T) {
	c := topo(TargetGateway, "http://ingester:8080", "", "http://querier:8080")
	if err := c.validateTopology(); err != nil {
		t.Fatal(err)
	}
	if len(c.IngesterURLs) != 1 || c.IngesterURLs[0] != "http://ingester:8080" {
		t.Errorf("IngesterURLs = %v", c.IngesterURLs)
	}
}

func TestIngesterURLListRefusals(t *testing.T) {
	for _, tc := range []struct{ list, want string }{
		{"http://a:1,,http://b:1", "empty element"},
		{"http://a:1,", "empty element"},
		{"http://a:1,http://a:1/", "duplicate"},
		{"http://a:1,ftp://b:1", "http or https"},
		{"http://a:1,http://user:secret@b:1", "credentials"},
		{"http://a:1,ftp://u:secret@b:1", "credentials"},
	} {
		c := topo(TargetQuerier, tc.list, "http://store:8080", "")
		err := c.validateTopology()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: err = %v, want one containing %q", tc.list, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%q: error echoes a credential: %v", tc.list, err)
		}
	}
}

func TestSingleURLPeersRefuseAComma(t *testing.T) {
	for _, c := range []*Config{
		topo(TargetQuerier, "http://i:1", "http://s:1,http://s2:1", ""),
		topo(TargetGateway, "http://i:1", "", "http://q:1,http://q2:1"),
	} {
		err := c.validateTopology()
		if err == nil || !strings.Contains(err.Error(), "single URL") {
			t.Errorf("%+v: err = %v, want a single-URL refusal", c, err)
		}
	}
}
