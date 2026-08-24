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

	"github.com/masonwheeler/observability-platform/internal/logs"
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
		Name      string         `yaml:"name"`
		Type      string         `yaml:"type"`
		UID       string         `yaml:"uid"`
		Access    string         `yaml:"access"`
		URL       string         `yaml:"url"`
		IsDefault bool           `yaml:"isDefault"`
		JSONData  map[string]any `yaml:"jsonData"`
	} `yaml:"datasources"`
}

type dashboardTarget struct {
	RefID      string `json:"refId"`
	Expr       string `json:"expr"`
	Datasource *dsRef `json:"datasource"`
}

type dashboardPanel struct {
	ID          int               `json:"id"`
	Title       string            `json:"title"`
	Type        string            `json:"type"`
	Datasource  *dsRef            `json:"datasource"`
	Targets     []dashboardTarget `json:"targets"`
	FieldConfig struct {
		Defaults struct {
			Custom struct {
				DrawStyle string `json:"drawStyle"`
				Stacking  struct {
					Mode string `json:"mode"`
				} `json:"stacking"`
			} `json:"custom"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
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

func loadDatasource(t *testing.T, path string) datasourceFile {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var f datasourceFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s is not valid YAML — Grafana would skip it at startup: %v", path, err)
	}
	// Exactly one, so the [0] every caller uses is not quietly ignoring a
	// second datasource that the dashboard might really be pointing at.
	if len(f.Datasources) != 1 {
		t.Fatalf("%s declares %d datasources, want exactly 1", path, len(f.Datasources))
	}
	return f
}

func loadDashboard(t *testing.T, path string) dashboardFile {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var d dashboardFile
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("%s is not valid JSON — Grafana would skip it at startup: %v", path, err)
	}
	return d
}

// TestLokiDatasourceProvisioning pins the fields Grafana and the runbook both
// depend on: the name the runbook tells you to click, the uid every dashboard
// target references, and the proxy access mode the URL below relies on.
func TestLokiDatasourceProvisioning(t *testing.T) {
	ds := loadDatasource(t, lokiDatasourcePath).Datasources[0]

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
	ds := loadDatasource(t, lokiDatasourcePath).Datasources[0]

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
	d := loadDashboard(t, logsDashboardPath)
	if d.UID != dashboardUID {
		t.Errorf("uid = %q, want %q", d.UID, dashboardUID)
	}
	if d.Title != dashboardTitle {
		t.Errorf("title = %q, want %q (the runbook navigates by this title)", d.Title, dashboardTitle)
	}
}

// wantPanels is the exact panel set the phase requires: the volume panel,
// two queryable logs panels, and the text panel documenting the supported
// subset.
//
// The target count is the load-bearing field. A logs panel whose targets went
// to [] still renders — as an empty panel — so counting targets dashboard-wide
// cannot notice it, and every per-target check below would simply iterate one
// fewer time and pass. Pinning the count per panel is what makes "this panel
// has a query" an assertion rather than a side effect of the file's contents.
// volumePanelID is the log-volume panel added in Phase 4.6.
const volumePanelID = 4

var wantPanels = []struct {
	id      int
	kind    string
	title   string
	targets int
}{
	{volumePanelID, "timeseries", "Log volume by level — $service", 1},
	{1, "logs", "All logs — $service", 1},
	{2, "logs", "$level logs — $service", 1},
	{3, "text", "Supported LogQL subset", 0},
}

// loadPanels returns the dashboard's panels after asserting the set matches
// wantPanels exactly. Every test that walks panels goes through this, so a panel
// that lost its target fails in each test that cares instead of being silently
// skipped by a loop over whatever survived.
func loadPanels(t *testing.T) []dashboardPanel {
	t.Helper()
	panels := loadDashboard(t, logsDashboardPath).Panels

	if len(panels) != len(wantPanels) {
		var got []string
		for _, p := range panels {
			got = append(got, fmt.Sprintf("%d:%s(%q, %d targets)", p.ID, p.Type, p.Title, len(p.Targets)))
		}
		t.Fatalf("panels = %v, want exactly %d: %+v", got, len(wantPanels), wantPanels)
	}
	for i, want := range wantPanels {
		p := panels[i]
		if p.ID != want.id {
			t.Errorf("panel %d has id %d, want %d", i, p.ID, want.id)
		}
		if p.Type != want.kind {
			t.Errorf("panel %d (%q) type = %q, want %q", p.ID, p.Title, p.Type, want.kind)
		}
		if p.Title != want.title {
			t.Errorf("panel %d title = %q, want %q", p.ID, p.Title, want.title)
		}
		if len(p.Targets) != want.targets {
			t.Errorf("panel %d (%q) has %d targets, want %d; a logs panel without its query renders empty",
				p.ID, p.Title, len(p.Targets), want.targets)
		}
	}
	return panels
}

// TestLogsVolumePanelRendersStackedBars pins the volume panel's rendering config.
// A timeseries panel with the right query still renders — as unstacked lines —
// if drawStyle or stacking regress, and every other check in this file would stay
// green: the expression parses, the datasource resolves, the targets are present.
// The only thing that would notice is a human looking at it, and the browser check
// is the one DoD item this suite cannot perform. So pin the two fields that decide
// whether "log volume by level" reads as a volume histogram or as line spaghetti.
func TestLogsVolumePanelRendersStackedBars(t *testing.T) {
	for _, p := range loadPanels(t) {
		if p.ID != volumePanelID {
			continue
		}
		if got := p.FieldConfig.Defaults.Custom.DrawStyle; got != "bars" {
			t.Errorf("volume panel drawStyle = %q, want \"bars\" — it would render as lines", got)
		}
		if got := p.FieldConfig.Defaults.Custom.Stacking.Mode; got != "normal" {
			t.Errorf("volume panel stacking mode = %q, want \"normal\" — per-level bars would overlap instead of stacking", got)
		}
		return
	}
	t.Fatalf("volume panel id %d not found", volumePanelID)
}

// TestLogsDashboardPanels is loadPanels' assertions standing on their own, so
// the panel set has a test that names it rather than only being enforced as a
// precondition of the datasource and LogQL checks.
func TestLogsDashboardPanels(t *testing.T) {
	loadPanels(t)
}

// wantVariables is the exact template variable list: name and kind, in order.
// The kind matters as much as the name. A query variable is populated live from
// /loki/api/v1/label/{name}/values; demoting one to custom or constant would
// freeze the dropdown at hardcoded values — the demo would still render, but it
// would have stopped reading anything from the backend, which is the whole
// point of the panel.
var wantVariables = []struct {
	name string
	kind string
}{
	{"service", "query"},
	{"level", "query"},
	{"search", "textbox"},
}

// TestLogsDashboardVariables enforces the constraints the dashboard cannot
// survive losing. The loop is driven by wantVariables rather than by each
// variable's own declared type, so a variable that changed kind fails the kind
// assertion instead of quietly skipping every check below it.
func TestLogsDashboardVariables(t *testing.T) {
	wantDatasourceType := loadDatasource(t, lokiDatasourcePath).Datasources[0].Type
	d := loadDashboard(t, logsDashboardPath)

	if len(d.Templating.List) != len(wantVariables) {
		var got []string
		for _, v := range d.Templating.List {
			got = append(got, fmt.Sprintf("%s(%s)", v.Name, v.Type))
		}
		t.Fatalf("variables = %v, want exactly %d: %+v", got, len(wantVariables), wantVariables)
	}

	for i, want := range wantVariables {
		v := d.Templating.List[i]
		t.Run(want.name, func(t *testing.T) {
			if v.Name != want.name {
				t.Fatalf("variable %d is %q, want %q (panel expressions reference these by name)", i, v.Name, want.name)
			}
			if v.Type != want.kind {
				t.Errorf("type = %q, want %q", v.Type, want.kind)
			}

			if want.kind == "textbox" {
				// The runbook states an empty Search box means "no filter".
				// A default here would open the dashboard pre-filtered.
				var def string
				if err := json.Unmarshal(v.Query, &def); err != nil {
					t.Fatalf("textbox query is not a string: %v (raw: %s)", err, v.Query)
				}
				if def != "" {
					t.Errorf("textbox default = %q, want empty; the dashboard would open pre-filtered", def)
				}
				return
			}

			if v.Multi {
				t.Errorf("multi = true; multi-select interpolates a regex label matcher, which returns 400")
			}
			if v.IncludeAll {
				t.Errorf("includeAll = true; the All option interpolates %s=~\"a|b\", which returns 400", v.Name)
			}
			checkDatasourceRef(t, "variable", v.Datasource, wantDatasourceType, datasourceUID)

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

// checkDatasourceRef asserts one Grafana datasource reference points at the
// provisioned datasource by *both* type and uid, each cross-referenced against
// loki.yml rather than compared to a constant.
//
// The type is not decoration. Grafana resolves the datasource by uid but builds
// the query editor and the query model from the type, so a reference reading
// {"type": "prometheus", "uid": "obs-loki"} sends the panel's LogQL through the
// Prometheus query path — the panel breaks while the uid still looks right.
func checkDatasourceRef(t *testing.T, label string, ref *dsRef, wantType, wantUID string) {
	t.Helper()
	if ref == nil {
		t.Errorf("%s has no datasource; it would fall back to Grafana's default", label)
		return
	}
	if ref.Type != wantType {
		t.Errorf("%s datasource type = %q, want %q (loki.yml)", label, ref.Type, wantType)
	}
	if ref.UID != wantUID {
		t.Errorf("%s datasource uid = %q, want %q (loki.yml)", label, ref.UID, wantUID)
	}
}

// TestLogsDashboardTargetsUseTheProvisionedDatasource cross-references every
// panel and target datasource reference against loki.yml's own type and uid, so
// the two files cannot drift into independent constants.
func TestLogsDashboardTargetsUseTheProvisionedDatasource(t *testing.T) {
	ds := loadDatasource(t, lokiDatasourcePath).Datasources[0]

	for _, p := range loadPanels(t) {
		// A text panel legitimately has no datasource. A panel with targets is
		// a query panel, and one that lost its datasource falls back to
		// Grafana's default and renders an error.
		if len(p.Targets) > 0 {
			checkDatasourceRef(t, fmt.Sprintf("panel %d (%q)", p.ID, p.Title), p.Datasource, ds.Type, ds.UID)
		}
		for _, tg := range p.Targets {
			checkDatasourceRef(t, fmt.Sprintf("panel %d (%q) target %q", p.ID, p.Title, tg.RefID), tg.Datasource, ds.Type, ds.UID)
		}
	}
}

// variablePattern matches Grafana's $name and ${name} interpolation syntax.
var variablePattern = regexp.MustCompile(`\$\{?(\w+)\}?`)

// variableValues are the values Grafana would interpolate for a viewer with the
// dropdowns on real demo streams, so each panel expression can be handed to the
// production parser in the form the backend would actually receive.
//
// Search carries two: the empty default every viewer sees on first render —
// |= "" must parse, and the runbook promises it matches every line — and an
// ordinary substring. Both are values a viewer could legitimately produce; a
// viewer who types a quote gets a 400 the runbook documents rather than the
// dashboard preventing, so that case is deliberately not simulated here.
//
// __interval also carries two: the minute-scale value typical of a wide time
// range, and "20ms", the form Grafana emits on a short one. Both must survive
// the range grammar in [$__interval] — a millisecond value is exactly the kind
// a static check would not think to try.
var variableValues = map[string][]string{
	"service":    {"api"},
	"level":      {"error"},
	"search":     {"", "timeout"},
	"__interval": {"1m", "20ms"},
}

// grafanaBuiltins are variables Grafana provides itself, so a panel may reference
// one without the dashboard declaring it. They still need a test value: an
// interval that interpolates to something the range grammar rejects breaks the
// panel in the browser while every static check here passes.
var grafanaBuiltins = map[string]bool{"__interval": true}

// interpolate substitutes Grafana template variables the way Grafana does,
// leaving anything it has no value for in place so the caller can catch it.
func interpolate(expr string, values map[string]string) string {
	return variablePattern.ReplaceAllStringFunc(expr, func(m string) string {
		if v, ok := values[variablePattern.FindStringSubmatch(m)[1]]; ok {
			return v
		}
		return m
	})
}

// parsePanelExpr runs a panel expression through whichever production parser the
// backend would use for it, routing on logs.IsLogExpression — the same predicate
// internal/api's query handlers dispatch on, rather than a re-implementation of
// it — so this test and the backend can never disagree about which parser owns a
// given expression. Routing here rather than skipping non-log expressions keeps
// the guarantee this test exists for — that every panel expression parses.
func parsePanelExpr(expr string) error {
	if !logs.IsLogExpression(expr) {
		_, err := logs.ParseMetricQuery(expr)
		return err
	}
	_, err := logs.ParseLogQL(expr)
	return err
}

// TestLogsDashboardQueriesStayInsideTheSupportedSubset runs every panel
// expression through the backend's own LogQL parser, with the template
// variables interpolated as Grafana would interpolate them.
//
// Pattern-matching the expression for known-bad operators only rejects the
// mistakes the test author thought of: it would wave through `| json`,
// `| logfmt`, `line_format`, a metric query, or any other unsupported form,
// each of which returns 400 and leaves the panel showing an error. The real
// parser is the only thing that agrees with the backend by construction — the
// same reason examples/sample-app validates its payloads with the real logs
// validators instead of a hand-written shape check.
func TestLogsDashboardQueriesStayInsideTheSupportedSubset(t *testing.T) {
	d := loadDashboard(t, logsDashboardPath)

	declared := map[string]bool{}
	for _, v := range d.Templating.List {
		declared[v.Name] = true
		if len(variableValues[v.Name]) == 0 {
			t.Errorf("variable %q has no test value in variableValues; add one so panel expressions using it are still parsed", v.Name)
		}
	}
	for name := range grafanaBuiltins {
		declared[name] = true
	}

	for _, p := range loadPanels(t) {
		for _, tg := range p.Targets {
			label := fmt.Sprintf("panel %d (%q) target %q", p.ID, p.Title, tg.RefID)
			if strings.TrimSpace(tg.Expr) == "" {
				t.Errorf("%s has an empty expr", label)
				continue
			}

			// Grafana leaves an unknown variable uninterpolated, so the panel
			// queries a literal "$sevice" and shows an empty result rather than
			// an error — the parser cannot catch this, because a literal is a
			// perfectly valid label value.
			for _, m := range variablePattern.FindAllStringSubmatch(tg.Expr, -1) {
				if !declared[m[1]] {
					t.Errorf("%s expr %q references undeclared variable %q; Grafana leaves it uninterpolated and the panel silently returns nothing",
						label, tg.Expr, m[1])
				}
			}

			for _, values := range variableCombinations(tg.Expr, declared) {
				expr := interpolate(tg.Expr, values)
				if strings.Contains(expr, "$") {
					// Guarded rather than assumed: an expression that still has
					// a variable in it is not the query the backend receives,
					// so parsing it would prove nothing.
					t.Errorf("%s expr %q still contains a variable after interpolation with %v", label, expr, values)
					continue
				}
				if err := parsePanelExpr(expr); err != nil {
					t.Errorf("%s expr %q interpolates to %q, which the backend's own parser rejects: %v",
						label, tg.Expr, expr, err)
				}
			}
		}
	}
}

// variableCombinations expands the variables an expression uses into every
// combination of their test values, so a panel is checked in each state a
// viewer can put it in — for Search, both the empty default and a filled box.
func variableCombinations(expr string, declared map[string]bool) []map[string]string {
	combos := []map[string]string{{}}
	for _, m := range variablePattern.FindAllStringSubmatch(expr, -1) {
		name := m[1]
		if !declared[name] {
			continue
		}
		var next []map[string]string
		for _, combo := range combos {
			if _, done := combo[name]; done {
				next = append(next, combo)
				continue
			}
			for _, v := range variableValues[name] {
				withValue := map[string]string{name: v}
				for k, existing := range combo {
					withValue[k] = existing
				}
				next = append(next, withValue)
			}
		}
		combos = next
	}
	return combos
}
