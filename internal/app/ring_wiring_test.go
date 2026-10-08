package app

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/config"
)

// The gateway and querier log the same ring hash for the same member set in any
// order, so an operator can compare the two (Review Focus 1).
func TestGatewayAndQuerierLogTheSameRing(t *testing.T) {
	var lines []string
	hashOf := func(target config.Target, urls []string) string {
		var buf bytes.Buffer
		cfg := &config.Config{Target: target, DataDir: t.TempDir(), IngesterURLs: urls,
			IngesterURL: strings.Join(urls, ","), ReplicationFactor: 3, QuerierURL: "http://q:1", StoreURL: "http://s:1"}
		if target == config.TargetGateway {
			cfg.StoreURL = ""
		} else {
			cfg.QuerierURL = ""
		}
		if _, err := Build(cfg, slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, `msg="ring ready"`) {
				lines = append(lines, line)
				_, after, _ := strings.Cut(line, "ring=")
				hash, _, _ := strings.Cut(after, " ")
				return hash
			}
		}
		t.Fatalf("%s logged no ring line:\n%s", target, buf.String())
		return ""
	}
	a := hashOf(config.TargetGateway, []string{"http://i-0:1", "http://i-1:1", "http://i-2:1"})
	b := hashOf(config.TargetQuerier, []string{"http://i-2:1", "http://i-1:1", "http://i-0:1"})
	c := hashOf(config.TargetQuerier, []string{"http://i-0:1"})
	for _, l := range lines {
		if !strings.Contains(l, "replication_factor=3") || !strings.Contains(l, "quorum=2") {
			t.Errorf("ring line lacks replication_factor=3 quorum=2: %s", l)
		}
	}
	if a != b || a == c || len(a) != 8 {
		t.Errorf("hashes: gateway %q, querier same set %q, querier other set %q", a, b, c)
	}
}
