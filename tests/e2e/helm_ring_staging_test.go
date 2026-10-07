package e2e_test

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

// helm template cannot look anything up, so these tests render a copy of the
// backend chart with one extra template that runs backend.ringStagingCheck
// over fixture objects, in the shape lookup returns them for a live release.
// The real split-configmaps.yaml still runs the check over empty lookups,
// which passes, so a failure here comes from the fixture.

const stagingFixtureTemplate = `{{- if eq .Values.topology "split" }}
{{- include "backend.ringStagingCheck" (list . (fromYaml (.Files.Get "live.yaml"))) }}
{{- end }}
`

// rollout is a Deployment's rollout state as lookup reports it.
type rollout struct{ spec, generation, observed, total, updated, available int }

var rolledOut = rollout{spec: 2, generation: 3, observed: 3, total: 2, updated: 2, available: 2}

// liveDeployment is a Deployment as lookup returns it. count < 0 leaves out
// the ingester-count annotation, as a release from before it was added has.
func liveDeployment(name string, count int, r rollout) map[string]any {
	annotations := map[string]any{"checksum/config": "x"}
	if count >= 0 {
		annotations["observability-platform.dev/ingester-count"] = strconv.Itoa(count)
	}
	return map[string]any{
		"metadata": map[string]any{"name": name, "generation": r.generation},
		"spec": map[string]any{
			"replicas": r.spec,
			"template": map[string]any{"metadata": map[string]any{"annotations": annotations}},
		},
		"status": map[string]any{
			"observedGeneration": r.observed, "replicas": r.total,
			"updatedReplicas": r.updated, "availableReplicas": r.available,
		},
	}
}

func liveConfigMap(count int) map[string]any {
	urls := make([]string, count)
	for i := range urls {
		urls[i] = "http://observability-ingester-" + strconv.Itoa(i) + ".observability-ingester-headless:8080"
	}
	return map[string]any{"data": map[string]any{"OBS_INGESTER_URL": strings.Join(urls, ",")}}
}

// renderWithLive renders the chart against live, returning helm's output and error.
func renderWithLive(t *testing.T, live map[string]any, sets ...string) (string, error) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "backend")
	if err := os.CopyFS(dir, os.DirFS(backendChart)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "templates", "zz-staging-fixture.yaml"), []byte(stagingFixtureTemplate), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(live) // JSON is YAML
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "live.yaml"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"template", "obs", dir, "--set", "topology=split"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	return string(out), err
}

func TestRingStagingReadsTheRunningRelease(t *testing.T) {
	helmAvailable(t)
	const gw, q = "observability-backend", "observability-querier"
	rolling := rollout{spec: 2, generation: 4, observed: 4, total: 3, updated: 1, available: 2}
	unobserved := rollout{spec: 2, generation: 5, observed: 4, total: 2, updated: 2, available: 2}
	live := func(gwCount, qCount int, gwR, qR rollout) map[string]any {
		return map[string]any{
			"gateway": liveDeployment(gw, gwCount, gwR), "querier": liveDeployment(q, qCount, qR),
			"gatewayConfig": liveConfigMap(3), "querierConfig": liveConfigMap(3),
		}
	}
	for _, tc := range []struct {
		name               string
		live               map[string]any
		set                []string
		wantFailureMention string // "" means the render must succeed
	}{
		{"add in one step", live(3, 3, rolledOut, rolledOut), []string{"split.ingester.replicas=4"}, "unstaged ring change"},
		{"add stage 1", live(3, 3, rolledOut, rolledOut), []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=3"}, ""},
		{"add stage 2 after stage 1 rolled out", live(3, 4, rolledOut, rolledOut), []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=4"}, ""},
		{"add stage 2 while the querier still rolls", live(3, 4, rolledOut, rolling), []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=4"}, "deployment/observability-querier has not finished rolling out"},
		{"a rollout the controller has not observed yet", live(3, 4, rolledOut, unobserved), []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=4"}, "deployment/observability-querier has not finished rolling out"},
		{"remove stage 3 while the gateway still rolls", live(3, 4, rolling, rolledOut), []string{"split.ingester.replicas=3", "split.ingester.writeReplicas=3"}, "deployment/observability-backend has not finished rolling out"},
		{"no ring change during a rollout", live(3, 3, rolling, rolling), nil, ""},
		{"previous on a real upgrade", live(3, 3, rolledOut, rolledOut), []string{"split.ingester.previous.replicas=3", "split.ingester.previous.writeReplicas=3"}, "previews only"},
		// The ConfigMaps say 3 and 3; the running pods' annotations say the
		// gateway writes to 4. The pods win: lowering replicas to 3 would hide
		// ingester-3's writes.
		{"the running pods win over the ConfigMaps", live(4, 4, rolledOut, rolledOut), []string{"split.ingester.replicas=3", "split.ingester.writeReplicas=3"}, "unstaged ring change"},
		{"a release without the annotation falls back to its ConfigMaps", live(-1, -1, rolledOut, rolledOut), []string{"split.ingester.replicas=4"}, "unstaged ring change"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := renderWithLive(t, tc.live, tc.set...)
			switch {
			case tc.wantFailureMention == "" && err != nil:
				t.Errorf("render failed, want success: %v\n%.600s", err, out)
			case tc.wantFailureMention != "" && (err == nil || !strings.Contains(out, tc.wantFailureMention)):
				t.Errorf("rendered (err %v), want a failure naming %q\n%.600s", err, tc.wantFailureMention, out)
			}
		})
	}
}

// The pod templates record the ingester count each pod loads, which is what
// the staging check reads back on the next upgrade.
func TestSplitPodTemplatesRecordTheirIngesterCount(t *testing.T) {
	helmAvailable(t)
	out, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology=split",
		"--set", "split.ingester.replicas=4", "--set", "split.ingester.writeReplicas=3").CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, want := range []string{
		"observability-platform.dev/ingester-count: \"3\"", // gateway
		"observability-platform.dev/ingester-count: \"4\"", // querier
	} {
		if strings.Count(string(out), want) != 1 {
			t.Errorf("want exactly one pod template annotated %s", want)
		}
	}
}

// A preview's stand-in for the live counts must give both of them: half of
// it would leave the check comparing against a count nobody gave. The
// assertion names the schema path and the missing field rather than Helm's
// prose, which differs between Helm 3 ("split.ingester.previous: writeReplicas
// is required") and Helm 4 ("at '/split/ingester/previous': missing property
// 'writeReplicas'").
func TestSplitPreviousNeedsBothCounts(t *testing.T) {
	helmAvailable(t)
	for _, tc := range []struct{ set, missing string }{
		{"split.ingester.previous.replicas=3", "writeReplicas"},
		{"split.ingester.previous.writeReplicas=3", "replicas"},
	} {
		out, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology=split", "--set", tc.set).CombinedOutput()
		missing := regexp.MustCompile(`(split\.ingester\.previous|/split/ingester/previous)\b.*\b` + tc.missing + `\b`)
		if err == nil || !strings.Contains(string(out), "values don't meet the specifications of the schema") || !missing.Match(out) {
			t.Errorf("--set %s alone rendered (err %v), want a schema failure naming split.ingester.previous and its missing %s\n%.400s", tc.set, err, tc.missing, out)
		}
	}
}

// podTemplates renders the split chart and returns each workload's pod
// template, marshalled, by workload name: a changed template is a rollout.
func podTemplates(t *testing.T, sets ...string) map[string]string {
	t.Helper()
	args := []string{"template", "obs", backendChart, "--set", "topology=split"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("render %v: %v\n%s", sets, err, out)
	}
	templates := map[string]string{}
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var obj struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Template any `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := dec.Decode(&obj); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode: %v", err)
		}
		if obj.Kind != "Deployment" && obj.Kind != "StatefulSet" {
			continue
		}
		b, err := json.Marshal(obj.Spec.Template)
		if err != nil {
			t.Fatal(err)
		}
		templates[obj.Metadata.Name] = string(b)
	}
	if len(templates) != 5 {
		t.Fatalf("rendered %d split workloads, want 5", len(templates))
	}
	return templates
}

// A ring stage restarts only the component whose ingester list it changes:
// scaling the ingester StatefulSet must not roll its existing pods, and the
// store and compactor never roll for a ring change. Restarting them would
// open read-wide 503 windows at every stage.
func TestRingStagesRollOnlyTheComponentWhoseListChanges(t *testing.T) {
	helmAvailable(t)
	stages := []struct {
		name  string
		sets  []string
		rolls string // the one workload whose pod template may change
	}{
		{"3/3", nil, ""},
		{"add stage 1: 4/3", []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=3"}, "observability-querier"},
		{"add stage 2: 4/4", []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=4"}, "observability-backend"},
		{"remove stage 1: 4/3", []string{"split.ingester.replicas=4", "split.ingester.writeReplicas=3"}, "observability-backend"},
		{"remove stage 3: 3/3", []string{"split.ingester.replicas=3", "split.ingester.writeReplicas=3"}, "observability-querier"},
	}
	prev := podTemplates(t, stages[0].sets...)
	for _, st := range stages[1:] {
		cur := podTemplates(t, st.sets...)
		for name, tmpl := range cur {
			changed := tmpl != prev[name]
			if name == st.rolls && !changed {
				t.Errorf("%s: %s's pod template did not change, so its pods keep the old ingester list", st.name, name)
			}
			if name != st.rolls && changed {
				t.Errorf("%s: %s's pod template changed, which restarts it for a ring change that is not its own", st.name, name)
			}
		}
		prev = cur
	}
}
