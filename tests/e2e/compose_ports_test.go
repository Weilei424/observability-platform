package e2e_test

// The Compose demo publishes three things with no access control worth the
// name: an unauthenticated backend, a Prometheus that will answer anyone, and a
// Grafana whose password is `admin`. Written as a bare "8080:8080", Docker binds
// each of those to 0.0.0.0 — every network the host can route to — and nothing
// in this suite noticed, because the file is valid either way and every test,
// runbook and Make target reaches the stack over localhost regardless of how it
// is published. A reviewer found it instead.
//
// So this pins the host binding itself, and pins the sentence in
// docs/api/limitations.md that states it to a reader, since that sentence is
// the only reason anyone would believe one way or the other.
//
// Checked-in files only — no Docker — so it runs inside `go test ./...`.

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

// loopbackHosts is what counts as "not published to the network". The empty
// host is deliberately absent: a mapping with no host part is the 0.0.0.0 case
// this test exists to catch.
var loopbackHosts = []string{"127.0.0.1", "localhost", "::1"}

// composePublishingServices is the complete set of services allowed to publish
// a host port at all. Pinned as a set rather than a count so that a service
// which starts publishing one has to be considered here deliberately.
var composePublishingServices = []string{"backend", "grafana", "prometheus"}

// publishedPort is one entry of a service's `ports:` list.
type publishedPort struct {
	Service string
	Raw     string // the mapping as written in the file
	HostIP  string // "" when the entry names no interface, i.e. 0.0.0.0
}

// splitHostIP pulls the host-interface half out of a short-syntax port mapping.
//
// Compose's short syntax is `[[HOST_IP:]HOST_PORT:]CONTAINER_PORT[/PROTO]`, so
// the host IP is present only in the three-field form — and an IPv6 literal is
// bracketed, which is why this cannot simply count colons.
func splitHostIP(raw string) string {
	s, _, _ := strings.Cut(raw, "/")
	if after, ok := strings.CutPrefix(s, "["); ok {
		if host, _, found := strings.Cut(after, "]"); found {
			return host
		}
		return "" // unterminated bracket: malformed, and certainly not loopback
	}
	if parts := strings.Split(s, ":"); len(parts) == 3 {
		return parts[0]
	}
	return ""
}

func composePublishedPorts(t *testing.T) []publishedPort {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(composePath))
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose struct {
		Services map[string]struct {
			Ports []any `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &compose); err != nil {
		t.Fatalf("%s is not valid YAML: %v", composePath, err)
	}

	var out []publishedPort
	for name, svc := range compose.Services {
		for _, entry := range svc.Ports {
			switch v := entry.(type) {
			case string:
				out = append(out, publishedPort{Service: name, Raw: v, HostIP: splitHostIP(v)})
			case map[string]any:
				// Long syntax: host_ip is an explicit field, and absent means
				// every interface, exactly as in the short form.
				host, _ := v["host_ip"].(string)
				out = append(out, publishedPort{
					Service: name,
					Raw:     fmt.Sprintf("%v", v),
					HostIP:  host,
				})
			default:
				t.Errorf("%s: service %q has a port entry this test cannot read (%T); it may be published to the network without being checked",
					composePath, name, entry)
			}
		}
	}
	slices.SortFunc(out, func(a, b publishedPort) int { return strings.Compare(a.Raw, b.Raw) })
	return out
}

// TestComposePublishedPortsAreLoopbackOnly is the assertion itself.
func TestComposePublishedPortsAreLoopbackOnly(t *testing.T) {
	ports := composePublishedPorts(t)
	if len(ports) == 0 {
		t.Fatalf("%s publishes no host ports at all; the `ports:` shape must have changed, and a check over zero mappings passes without checking anything", composePath)
	}

	var services []string
	for _, p := range ports {
		if !slices.Contains(services, p.Service) {
			services = append(services, p.Service)
		}
		if !slices.Contains(loopbackHosts, p.HostIP) {
			where := "no host interface, so Docker publishes it on 0.0.0.0"
			if p.HostIP != "" {
				where = fmt.Sprintf("host interface %q", p.HostIP)
			}
			t.Errorf("%s: service %q publishes %q with %s. The demo has no authentication worth the name; bind it to loopback (\"127.0.0.1:<host>:<container>\").",
				composePath, p.Service, p.Raw, where)
		}
	}

	slices.Sort(services)
	want := slices.Clone(composePublishingServices)
	slices.Sort(want)
	if !slices.Equal(services, want) {
		t.Errorf("%s publishes host ports for %v, want exactly %v. A service that newly publishes a port needs a deliberate decision about its host binding, not a default.",
			composePath, services, want)
	}
}

// TestLimitationsDocMatchesComposePortBindings keeps the prose and the file in
// step. docs/api/limitations.md tells the reader what the demo is reachable
// from; that sentence is worth nothing if it is not derived from the mappings.
func TestLimitationsDocMatchesComposePortBindings(t *testing.T) {
	doc := repoFile(t, limitationsDoc)
	ports := composePublishedPorts(t)
	if len(ports) == 0 {
		t.Fatalf("%s publishes no host ports; nothing to cross-reference", composePath)
	}
	for _, p := range ports {
		if !strings.Contains(doc, `"`+p.Raw+`"`) {
			t.Errorf("%s does not quote the mapping %q that %s actually publishes for %q. A reader trusting that section would be told the wrong thing about what the demo exposes.",
				limitationsDoc, p.Raw, composePath, p.Service)
		}
	}
}
