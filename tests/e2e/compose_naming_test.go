package e2e_test

// The Compose smoke test derives its project name per run, and that name is the
// only thing separating two concurrent runs: compose containers and volumes
// carry their project name and nothing else, so two runs sharing one are
// indistinguishable and the loser's teardown runs `down -v` on the winner's
// stack.
//
// It has been wrong twice. First a fixed name guarded by a hand-rolled lock
// that was itself racy. Then a name suffixed only from `date +%s%N` — which is
// GNU-only: any BSD or macOS date has no %N and emits a literal "N", so the
// suffix degraded to whole seconds and every run started in the same second
// shared an identity. A stubbed clock reproduced two runs on one project, both
// tearing it down.
//
// So the property is pinned here rather than argued in a comment: with the
// clock pinned to a constant, two processes must still derive different names.
// That holds only if identity includes something process-unique, which is what
// the pid field in the script is for.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const composeSmokeScript = "compose_smoke.sh"

// projectNameFrom sources the script in library mode and prints the project
// name it derived. dateStub, when non-empty, is installed as a `date` earlier on
// PATH that ignores its arguments and prints that text — standing in for a
// coarse or broken clock.
func projectNameFrom(t *testing.T, dateStub string, env ...string) string {
	t.Helper()

	script, err := filepath.Abs(composeSmokeScript)
	if err != nil {
		t.Fatalf("abs %s: %v", composeSmokeScript, err)
	}

	path := os.Getenv("PATH")
	if dateStub != "" {
		dir := t.TempDir()
		// Ignores its arguments and prints the fixed value, which is what a
		// clock with no sub-second resolution amounts to.
		stub := filepath.Join(dir, "date")
		if err := os.WriteFile(stub, []byte("#!/bin/sh\necho "+dateStub+"\n"), 0o755); err != nil {
			t.Fatalf("write date stub: %v", err)
		}
		path = dir + string(os.PathListSeparator) + path
	}

	cmd := exec.Command("bash", "-c", `source "$1"; printf '%s' "$PROJECT"`, "bash", script)
	cmd.Env = append(os.Environ(), "OBS_COMPOSE_SMOKE_LIB_ONLY=1", "PATH="+path)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sourcing %s in library mode failed: %v\n%s", composeSmokeScript, err, out)
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		t.Fatalf("%s derived an empty project name; the sourcing hook or PROJECT assignment changed shape", composeSmokeScript)
	}
	return name
}

// TestComposeProjectNameIsUniquePerProcess is the regression test for the
// collision. The clock is pinned to one value for every invocation, which is
// exactly the macOS/BSD behaviour and the reproducer for the bug; the names
// must still differ.
func TestComposeProjectNameIsUniquePerProcess(t *testing.T) {
	const frozenClock = "1789700000000000000"

	seen := map[string]bool{}
	const runs = 12
	for i := 0; i < runs; i++ {
		name := projectNameFrom(t, frozenClock)
		if seen[name] {
			t.Fatalf("two invocations derived the same project name %q with the clock pinned to %s. "+
				"Identity is coming from the clock alone, so two concurrent runs share a project — "+
				"and the one whose `up` loses the race for the host ports tears down the other's stack.",
				name, frozenClock)
		}
		seen[name] = true
	}
	if len(seen) != runs {
		t.Fatalf("expected %d distinct project names, got %d", runs, len(seen))
	}
}

// TestComposeProjectNameSurvivesAClockWithoutNanoseconds pins the specific
// portability trap: a date that does not understand %N. A literal "N" must not
// reach the project name, because compose rejects nothing here and the name
// would simply be wrong in a way no test noticed.
func TestComposeProjectNameSurvivesAClockWithoutNanoseconds(t *testing.T) {
	// What BSD date actually prints for `+%s%N`.
	name := projectNameFrom(t, "1789700000N")
	if strings.ContainsAny(name, "N") {
		t.Errorf("project name %q carries the literal N that a BSD date emits for an unsupported %%N", name)
	}
	if !isValidComposeProject(name) {
		t.Errorf("project name %q is not a valid compose project name", name)
	}
}

// TestComposeProjectNameHonoursThePrefix pins the override's semantics:
// OBS_COMPOSE_PROJECT chooses the prefix and cannot opt out of uniqueness,
// which is what stops two runs told the same value from colliding.
func TestComposeProjectNameHonoursThePrefix(t *testing.T) {
	const prefix = "my-own-prefix"
	a := projectNameFrom(t, "", "OBS_COMPOSE_PROJECT="+prefix)
	b := projectNameFrom(t, "", "OBS_COMPOSE_PROJECT="+prefix)

	for _, n := range []string{a, b} {
		if !strings.HasPrefix(n, prefix+"-") {
			t.Errorf("project name %q does not start with the requested prefix %q", n, prefix)
		}
		if n == prefix {
			t.Errorf("project name is exactly the prefix %q; uniqueness must not be opt-out, or two runs given the same value collide", prefix)
		}
	}
	if a == b {
		t.Errorf("two runs given OBS_COMPOSE_PROJECT=%q derived the same project %q", prefix, a)
	}
}

// isValidComposeProject mirrors compose's own constraint: lowercase letters,
// digits, dashes and underscores, starting with a letter or digit.
func isValidComposeProject(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case (r == '-' || r == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}
