package e2e_test

// These tests validate the Grafana provisioning assets behind the logs demo.
// They read the checked-in files only — no Docker, no Grafana, no backend — so
// they run in `go test ./...` alongside everything else.
//
// This is the automated stand-in for clicking through Grafana. Without it, a
// wrong datasource URL or an *All* option on a variable leaves every live API
// check green while Grafana itself shows "datasource not found" or sends a
// regex label matcher the backend rejects by design. These checks used to live
// in a python3 + PyYAML block inside tests/e2e/logs_smoke.sh, which made an
// interpreter documented nowhere in the runbook the gate on the only
// Docker-free validation the demo had; the Go toolchain is already required to
// build this repo.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

const (
	lokiDatasourcePath = "../../observability/grafana/datasources/loki.yml"
	logsDashboardPath  = "../../observability/grafana/dashboards/logs.json"
	composePath        = "../../deployments/docker/docker-compose.yml"

	// The runbook navigates by these names, and the compose file wires Grafana
	// to the backend by this service name.
	datasourceName     = "observability-platform-logs"
	datasourceUID      = "obs-loki"
	dashboardUID       = "obs-logs-v1"
	dashboardTitle     = "Observability Platform Logs"
	backendComposeName = "backend"
)

// dsRef is Grafana's datasource reference, used at dashboard, panel, target,
// and variable level.
type dsRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type datasourceFile struct {
	APIVersion  int `yaml:"apiVersion"`
	Datasources []struct {
		Name     string         `yaml:"name"`
		Type     string         `yaml:"type"`
		UID      string         `yaml:"uid"`
		Access   string         `yaml:"access"`
		URL      string         `yaml:"url"`
		JSONData map[string]any `yaml:"jsonData"`
	} `yaml:"datasources"`
}

type dashboardTarget struct {
	RefID      string `json:"refId"`
	Expr       string `json:"expr"`
	Datasource *dsRef `json:"datasource"`
}

type dashboardPanel struct {
	ID         int               `json:"id"`
	Title      string            `json:"title"`
	Type       string            `json:"type"`
	Datasource *dsRef            `json:"datasource"`
	Targets    []dashboardTarget `json:"targets"`
}

// dashboardVariable covers both variable kinds in this dashboard. Query is a
// json.RawMessage because Grafana 11 writes an object for a query variable and
// a plain string for a textbox — one Go type cannot hold both.
type dashboardVariable struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Label      string          `json:"label"`
	Multi      bool            `json:"multi"`
	IncludeAll bool            `json:"includeAll"`
	Definition string          `json:"definition"`
	Datasource *dsRef          `json:"datasource"`
	Query      json.RawMessage `json:"query"`
}

// lokiVariableQuery is the object form of a Loki query variable. Type 1 is
// LokiVariableQueryType.LabelValues; an empty Stream means the label's values
// are fetched unscoped, which is what /loki/api/v1/label/{name}/values serves.
type lokiVariableQuery struct {
	Label  string `json:"label"`
	RefID  string `json:"refId"`
	Stream string `json:"stream"`
	Type   int    `json:"type"`
}

type dashboardFile struct {
	UID        string           `json:"uid"`
	Title      string           `json:"title"`
	Panels     []dashboardPanel `json:"panels"`
	Templating struct {
		List []dashboardVariable `json:"list"`
	} `json:"templating"`
}

func loadDatasource(t *testing.T) datasourceFile {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(lokiDatasourcePath))
	if err != nil {
		t.Fatalf("read %s: %v", lokiDatasourcePath, err)
	}
	var f datasourceFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s is not valid YAML — Grafana would skip it at startup: %v", lokiDatasourcePath, err)
	}
	// Exactly one, so the [0] every caller uses is not quietly ignoring a
	// second datasource that the dashboard might really be pointing at.
	if len(f.Datasources) != 1 {
		t.Fatalf("%s declares %d datasources, want exactly 1", lokiDatasourcePath, len(f.Datasources))
	}
	return f
}

func loadDashboard(t *testing.T) dashboardFile {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(logsDashboardPath))
	if err != nil {
		t.Fatalf("read %s: %v", logsDashboardPath, err)
	}
	var d dashboardFile
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("%s is not valid JSON — Grafana would skip it at startup: %v", logsDashboardPath, err)
	}
	return d
}

// TestLokiDatasourceProvisioning pins the fields Grafana and the runbook both
// depend on: the name the runbook tells you to click, the uid every dashboard
// target references, and the proxy access mode the URL below relies on.
func TestLokiDatasourceProvisioning(t *testing.T) {
	ds := loadDatasource(t).Datasources[0]

	if ds.Name != datasourceName {
		t.Errorf("name = %q, want %q (docs/runbooks/grafana-logs-demo.md navigates by this name)", ds.Name, datasourceName)
	}
	if ds.Type != "loki" {
		t.Errorf("type = %q, want \"loki\"", ds.Type)
	}
	if ds.UID != datasourceUID {
		t.Errorf("uid = %q, want %q (dashboard targets reference this uid)", ds.UID, datasourceUID)
	}
	// access: proxy is what makes the compose-internal URL below work at all.
	// In direct mode the browser would fetch http://backend:8080 itself, and
	// that name only resolves inside the compose network.
	if ds.Access != "proxy" {
		t.Errorf("access = %q, want \"proxy\"; the URL is a compose service name that only resolves server-side", ds.Access)
	}
	if got, ok := ds.JSONData["maxLines"]; !ok || got != 1000 {
		t.Errorf("jsonData.maxLines = %v (present=%v), want 1000", got, ok)
	}
}

// TestLokiDatasourceURLMatchesComposeBackend cross-references the datasource
// URL against docker-compose.yml rather than asserting a magic string, so
// renaming the backend service or moving its port fails here instead of in a
// browser. The classic version of this bug is a URL of http://localhost:8080,
// which inside the Grafana container points at Grafana itself.
func TestLokiDatasourceURLMatchesComposeBackend(t *testing.T) {
	ds := loadDatasource(t).Datasources[0]

	const wantScheme = "http://"
	if !strings.HasPrefix(ds.URL, wantScheme) {
		t.Fatalf("url = %q, want an %s URL", ds.URL, wantScheme)
	}
	hostPort := strings.TrimPrefix(ds.URL, wantScheme)
	hostPort = strings.TrimSuffix(hostPort, "/")
	host, port, found := strings.Cut(hostPort, ":")
	if !found {
		t.Fatalf("url = %q, want an explicit host:port", ds.URL)
	}

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

	if host != backendComposeName {
		t.Errorf("url host = %q, want the compose service name %q", host, backendComposeName)
	}
	svc, ok := compose.Services[host]
	if !ok {
		names := make([]string, 0, len(compose.Services))
		for n := range compose.Services {
			names = append(names, n)
		}
		t.Fatalf("url host %q is not a service in %s; services are %v — Grafana would not resolve it", host, composePath, names)
	}

	var containerPorts []string
	for _, p := range svc.Ports {
		containerPorts = append(containerPorts, containerPort(p))
	}
	if !slices.Contains(containerPorts, port) {
		t.Errorf("url port %q is not a container port of compose service %q (container ports: %v)", port, host, containerPorts)
	}
}

// containerPort returns the container side of one compose ports entry, for both
// the short "8080:8080" form and the long {target: 8080, published: 8080} form.
func containerPort(entry any) string {
	switch v := entry.(type) {
	case string:
		// "8080:8080", "127.0.0.1:8080:8080", or a bare "8080".
		parts := strings.Split(strings.TrimSuffix(v, "/tcp"), ":")
		return parts[len(parts)-1]
	case map[string]any:
		return fmt.Sprintf("%v", v["target"])
	default:
		return fmt.Sprintf("%v", entry)
	}
}

// TestLogsDashboardIdentity pins the uid the runbook and ARCHITECTURE_NOTES
// name, and the title the runbook navigates by.
func TestLogsDashboardIdentity(t *testing.T) {
	d := loadDashboard(t)
	if d.UID != dashboardUID {
		t.Errorf("uid = %q, want %q", d.UID, dashboardUID)
	}
	if d.Title != dashboardTitle {
		t.Errorf("title = %q, want %q (the runbook navigates by this title)", d.Title, dashboardTitle)
	}
	if len(d.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
}

// TestLogsDashboardVariables enforces the two constraints the dashboard cannot
// survive losing. Multi-select or an *All* option makes Grafana interpolate
// service=~"api|worker", and regex label matchers return 400 by design — the
// dropdown would look fine and every panel would error.
func TestLogsDashboardVariables(t *testing.T) {
	d := loadDashboard(t)

	var names []string
	for _, v := range d.Templating.List {
		names = append(names, v.Name)
	}
	want := []string{"service", "level", "search"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("variables = %v, want %v (panel expressions reference these by name)", names, want)
	}

	for _, v := range d.Templating.List {
		if v.Type != "query" {
			continue
		}
		t.Run(v.Name, func(t *testing.T) {
			if v.Multi {
				t.Errorf("multi = true; multi-select interpolates a regex label matcher, which returns 400")
			}
			if v.IncludeAll {
				t.Errorf("includeAll = true; the All option interpolates %s=~\"a|b\", which returns 400", v.Name)
			}
			if v.Datasource == nil || v.Datasource.UID != datasourceUID {
				t.Errorf("datasource = %+v, want uid %q; a variable on the wrong datasource never populates", v.Datasource, datasourceUID)
			}

			var q lokiVariableQuery
			if err := json.Unmarshal(v.Query, &q); err != nil {
				t.Fatalf("query is not the Grafana 11 object form: %v (raw: %s)", err, v.Query)
			}
			// Type 1 is LabelValues. Any other type asks for something this
			// backend does not serve — type 0 (LabelNames) or a raw stream
			// query — and the dropdown comes back empty.
			if q.Type != 1 {
				t.Errorf("query.type = %d, want 1 (LabelValues); it is what /loki/api/v1/label/%s/values answers", q.Type, v.Name)
			}
			if q.Label != v.Name {
				t.Errorf("query.label = %q, want %q; the dropdown would list another label's values", q.Label, v.Name)
			}
			if q.Stream != "" {
				t.Errorf("query.stream = %q, want empty; a scoped variable query needs /loki/api/v1/series, which returns 404", q.Stream)
			}
			if wantDef := fmt.Sprintf("label_values(%s)", v.Name); v.Definition != wantDef {
				t.Errorf("definition = %q, want %q", v.Definition, wantDef)
			}
		})
	}
}

// TestLogsDashboardTargetsUseTheProvisionedDatasource cross-references every
// panel target's uid against loki.yml's own uid, so the two files cannot drift
// into two independent constants.
func TestLogsDashboardTargetsUseTheProvisionedDatasource(t *testing.T) {
	wantUID := loadDatasource(t).Datasources[0].UID
	d := loadDashboard(t)

	targets := 0
	for _, p := range d.Panels {
		// A text panel legitimately has no datasource; a query panel that lost
		// one falls back to Grafana's default datasource and renders an error.
		if p.Datasource != nil && p.Datasource.UID != wantUID {
			t.Errorf("panel %d (%q) datasource uid = %q, want %q", p.ID, p.Title, p.Datasource.UID, wantUID)
		}
		for _, tg := range p.Targets {
			targets++
			if tg.Datasource == nil {
				t.Errorf("panel %d (%q) target %q has no datasource", p.ID, p.Title, tg.RefID)
				continue
			}
			if tg.Datasource.UID != wantUID {
				t.Errorf("panel %d (%q) target %q datasource uid = %q, want %q (loki.yml)",
					p.ID, p.Title, tg.RefID, tg.Datasource.UID, wantUID)
			}
		}
	}
	if targets == 0 {
		t.Fatal("no panel targets found; the dashboard would render nothing")
	}
}

// selectorPattern captures the stream selector — the {...} at the head of a
// LogQL expression — separately from any line filters that follow it. The
// distinction matters: =~ and !~ are supported on lines and unsupported on
// labels, so a blanket search for those operators would forbid a legal query.
var selectorPattern = regexp.MustCompile(`^\{[^}]*\}`)

// variablePattern matches Grafana's $name and ${name} interpolation syntax.
var variablePattern = regexp.MustCompile(`\$\{?(\w+)\}?`)

// TestLogsDashboardQueriesStayInsideTheSupportedSubset checks the panel
// expressions themselves. Two ways a dashboard breaks silently: a stream
// selector with a regex or negative label matcher (400 by design, because the
// label index is equality-only), and a misspelled variable, which Grafana
// leaves uninterpolated so the panel queries a literal "$sevice" and shows an
// empty result rather than an error.
func TestLogsDashboardQueriesStayInsideTheSupportedSubset(t *testing.T) {
	d := loadDashboard(t)

	declared := map[string]bool{}
	for _, v := range d.Templating.List {
		declared[v.Name] = true
	}

	for _, p := range d.Panels {
		for _, tg := range p.Targets {
			label := fmt.Sprintf("panel %d (%q) target %q", p.ID, p.Title, tg.RefID)
			if strings.TrimSpace(tg.Expr) == "" {
				t.Errorf("%s has an empty expr", label)
				continue
			}

			selector := selectorPattern.FindString(tg.Expr)
			if selector == "" {
				t.Errorf("%s expr %q does not start with a {...} stream selector", label, tg.Expr)
				continue
			}
			for _, op := range []string{"=~", "!~", "!="} {
				if strings.Contains(selector, op) {
					t.Errorf("%s selector %q uses %q; label matchers are equality-only and this returns 400", label, selector, op)
				}
			}

			for _, m := range variablePattern.FindAllStringSubmatch(tg.Expr, -1) {
				if !declared[m[1]] {
					t.Errorf("%s expr %q references undeclared variable %q; Grafana leaves it uninterpolated and the panel silently returns nothing",
						label, tg.Expr, m[1])
				}
			}
		}
	}
}
