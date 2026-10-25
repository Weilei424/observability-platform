package tenant

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "overrides.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadOverridesAndLookups(t *testing.T) {
	p := writeFile(t, `overrides:
  team-b:
    retention: 24h
    max_active_series: 200
  anonymous:
    max_active_streams: 7
  keep-forever:
    retention: 0s
`)
	o, err := LoadOverrides(p)
	if err != nil {
		t.Fatal(err)
	}
	l := NewLimits(Defaults{Retention: 72 * time.Hour, MaxActiveSeries: 1000, MaxMetricsPushBytes: 1 << 20, MaxLogsPushBytes: 4 << 20}, o)
	if l.Retention("team-b") != 24*time.Hour || l.MaxActiveSeries("team-b") != 200 {
		t.Errorf("team-b = %v / %d", l.Retention("team-b"), l.MaxActiveSeries("team-b"))
	}
	if l.Retention("other") != 72*time.Hour || l.MaxActiveSeries("other") != 1000 {
		t.Errorf("an unlisted tenant takes the defaults")
	}
	if l.MaxActiveStreams(Default) != 7 {
		t.Errorf("the anonymous entry applies to the default tenant")
	}
	p2 := l.RetentionPolicy()
	if p2.For("keep-forever") != 0 || p2.For("team-b") != 24*time.Hour || p2.For("x") != 72*time.Hour {
		t.Errorf("policy = %+v", p2)
	}
}

func TestLoadOverridesRefusesBadInput(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"unknown key", "overrides:\n  a:\n    max_series: 1\n", "max_series"},
		{"bad tenant", "overrides:\n  'a b':\n    max_active_series: 1\n", "invalid tenant ID"},
		{"negative limit", "overrides:\n  a:\n    max_active_series: -1\n", "max_active_series"},
		{"bad duration", "overrides:\n  a:\n    retention: soon\n", "retention"},
		{"negative retention", "overrides:\n  a:\n    retention: -1h\n", "retention"},
		{"zero push cap", "overrides:\n  a:\n    max_logs_push_bytes: 0\n", "max_logs_push_bytes"},
		{"push cap too big", "overrides:\n  a:\n    max_metrics_push_bytes: 16777217\n", "max_metrics_push_bytes"},
		{"top-level typo", "override:\n  a: {}\n", "override"},
		{"second document", "overrides:\n  a:\n    max_active_series: 1\n---\noverrides:\n  b:\n    max_active_series: 2\n", "one YAML document"},
		{"second document with an unknown key", "overrides: {}\n---\nmax_series: 1\n", "one YAML document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadOverrides(writeFile(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
	if _, err := LoadOverrides(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("a missing file must refuse to start")
	}
}

func TestLoadOverridesEmptyFileMeansNone(t *testing.T) {
	for _, body := range []string{"", "  \n\n", "# nothing yet\n"} {
		m, err := LoadOverrides(writeFile(t, body))
		if err != nil || len(m) != 0 {
			t.Fatalf("%q: %v %v", body, m, err)
		}
	}
}
