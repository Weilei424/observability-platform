package e2e_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/config"
	yaml "go.yaml.in/yaml/v3"
)

const (
	composeSplitPath    = "../../deployments/docker/docker-compose.split.yml"
	prometheusSplitPath = "../../observability/prometheus/prometheus.split.yml"
)

var splitComponents = []string{"gateway", "ingester", "querier", "store", "compactor"}

type composeSplitService struct {
	Environment map[string]string `yaml:"environment"`
	Ports       []any             `yaml:"ports"`
	DependsOn   map[string]any    `yaml:"depends_on"`
	Networks    map[string]struct {
		Aliases []string `yaml:"aliases"`
	} `yaml:"networks"`
}

func loadComposeSplit(t *testing.T) map[string]composeSplitService {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(composeSplitPath))
	if err != nil {
		t.Fatalf("read %s: %v", composeSplitPath, err)
	}
	var f struct {
		Services map[string]composeSplitService `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s is not valid YAML: %v", composeSplitPath, err)
	}
	return f.Services
}

func TestComposeSplitPublishesOnlyLoopbackPortsFromTheEdge(t *testing.T) {
	ports := composePublishedPortsIn(t, composeSplitPath)
	var services []string
	for _, p := range ports {
		if !slices.Contains(loopbackHosts, p.HostIP) {
			t.Errorf("%s: %q publishes %q beyond loopback", composeSplitPath, p.Service, p.Raw)
		}
		if !slices.Contains(services, p.Service) {
			services = append(services, p.Service)
		}
	}
	slices.Sort(services)
	if want := []string{"gateway", "grafana", "prometheus"}; !slices.Equal(services, want) {
		t.Errorf("%s publishes ports for %v, want exactly %v: internal APIs must never be reachable from the host", composeSplitPath, services, want)
	}
	doc := repoFile(t, limitationsDoc)
	for _, p := range ports {
		if !strings.Contains(doc, `"`+p.Raw+`"`) {
			t.Errorf("%s does not quote %q, which %s publishes", limitationsDoc, p.Raw, composeSplitPath)
		}
	}
}

func TestComposeSplitGatewayAnswersAsBackend(t *testing.T) {
	svcs := loadComposeSplit(t)
	if _, clash := svcs["backend"]; clash {
		t.Fatal("the split file must not define a backend service: the gateway's alias is that name")
	}
	aliases := svcs["gateway"].Networks["default"].Aliases
	if !slices.Contains(aliases, "backend") {
		t.Fatalf("gateway aliases = %v, want backend: every datasource and producer URL names it", aliases)
	}
	for _, name := range []string{"load-generator", "sample-app"} {
		if got := svcs[name].Environment["OBS_BACKEND_ADDR"]; got != "http://backend:8080" {
			t.Errorf("%s OBS_BACKEND_ADDR = %q, want http://backend:8080", name, got)
		}
	}
}

// Independent start is the point of the split: no component may wait for another.
func TestComposeSplitComponentsDoNotDependOnEachOther(t *testing.T) {
	svcs := loadComposeSplit(t)
	for _, c := range splitComponents {
		for dep := range svcs[c].DependsOn {
			if slices.Contains(splitComponents, dep) {
				t.Errorf("%s depends_on %s; components must start in any order", c, dep)
			}
		}
	}
}

// Each component's environment must be one config.Load accepts: the right
// target and exactly the peer URLs it needs.
func TestComposeSplitComponentEnvironmentsLoad(t *testing.T) {
	svcs := loadComposeSplit(t)
	for _, c := range splitComponents {
		t.Run(c, func(t *testing.T) {
			env := svcs[c].Environment
			if env["OBS_TARGET"] != c {
				t.Fatalf("OBS_TARGET = %q, want %q", env["OBS_TARGET"], c)
			}
			// Viper treats an empty variable as unset, so "" restores a default —
			// except OBS_DATA_DIR, which config reads with LookupEnv and refuses
			// when empty. The stateless components do not set it; give it the default.
			for _, k := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL", "OBS_LOG_LEVEL", "OBS_LOGS_FLUSH_THRESHOLD_BYTES"} {
				t.Setenv(k, "")
			}
			t.Setenv("OBS_DATA_DIR", "data")
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := config.Load(); err != nil {
				t.Fatalf("config.Load with %s's environment: %v", c, err)
			}
		})
	}
}

func TestPrometheusSplitScrapesEveryComponent(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash(prometheusSplitPath))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		ScrapeConfigs []struct {
			StaticConfigs []struct {
				Targets []string          `yaml:"targets"`
				Labels  map[string]string `yaml:"labels"`
			} `yaml:"static_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	svcs := loadComposeSplit(t)
	seen := map[string]bool{}
	for _, sc := range cfg.ScrapeConfigs {
		for _, st := range sc.StaticConfigs {
			for _, target := range st.Targets {
				host, _, _ := strings.Cut(target, ":")
				if _, ok := svcs[host]; !ok {
					t.Errorf("scrape target %s is not a service in %s", target, composeSplitPath)
				}
				if st.Labels["component"] != host || st.Labels["service"] != "observability-platform" {
					t.Errorf("target %s labels = %v, want component=%s service=observability-platform", target, st.Labels, host)
				}
				seen[host] = true
			}
		}
	}
	for _, c := range splitComponents {
		if !seen[c] {
			t.Errorf("%s does not scrape %s", prometheusSplitPath, c)
		}
	}
}
