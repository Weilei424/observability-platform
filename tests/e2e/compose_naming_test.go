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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const composeSmokeScript = "compose_smoke.sh"

// Frozen inputs, and what `tr 0-9 a-j` renders them as. Asserting the rendered
// forms appear in the derived name is what stops this test passing vacuously:
// if a stub is not picked up, the field is live, the names differ for the wrong
// reason, and the uniqueness check below would still pass.
const (
	frozenClock  = "1789700000"
	frozenClockR = "bhijhaaaaa"
	frozenNonce  = "4242424242"
	frozenNonceR = "ececececec"
)

// stubDir builds a PATH entry holding fixed `date` and `od`, so the only input
// left varying between processes is the pid. Both stubs ignore their arguments.
func stubDir(t *testing.T, clock, nonce string) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{"date": clock, "od": nonce} {
		if value == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name),
			[]byte("#!/bin/sh\necho "+value+"\n"), 0o755); err != nil {
			t.Fatalf("write %s stub: %v", name, err)
		}
	}
	return dir
}

// projectName sources the script in library mode and returns the project name it
// derived, with stubDir (when given) ahead of PATH.
func projectName(stubs string, env ...string) (string, error) {
	script, err := filepath.Abs(composeSmokeScript)
	if err != nil {
		return "", err
	}
	path := os.Getenv("PATH")
	if stubs != "" {
		path = stubs + string(os.PathListSeparator) + path
	}
	cmd := exec.Command("bash", "-c", `source "$1"; printf '%s' "$PROJECT"`, "bash", script)
	cmd.Env = append(os.Environ(), "OBS_COMPOSE_SMOKE_LIB_ONLY=1", "PATH="+path)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("sourcing %s in library mode failed: %v\n%s", composeSmokeScript, err, out)
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("%s derived an empty project name; the sourcing hook or PROJECT assignment changed shape", composeSmokeScript)
	}
	return name, nil
}

func projectNameFrom(t *testing.T, stubs string, env ...string) string {
	t.Helper()
	name, err := projectName(stubs, env...)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

// TestComposeProjectNameIsUniquePerProcess is the regression test for the
// collision, and it isolates the guarantee the script actually leans on.
//
// Freezing only the clock was not enough: with the urandom nonce still live,
// the names differed no matter what, so the test passed even with the pid field
// replaced by a constant — it confirmed the outcome without exercising the
// mechanism. Both the clock and the nonce are frozen here, leaving the pid as
// the sole varying input, and the processes run concurrently so their pids are
// necessarily distinct.
func TestComposeProjectNameIsUniquePerProcess(t *testing.T) {
	stubs := stubDir(t, frozenClock, frozenNonce)

	const runs = 12
	type result struct {
		name string
		err  error
	}
	results := make(chan result, runs)

	// Released together, so the processes genuinely overlap rather than running
	// one after another where a pid could legitimately be reused.
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < runs; i++ {
		go func() {
			start.Wait()
			name, err := projectName(stubs)
			results <- result{name, err}
		}()
	}
	start.Done()

	seen := map[string]bool{}
	for i := 0; i < runs; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		// Vacuity guard: if either stub was not picked up, that field is still
		// live and the names below would differ for a reason this test is not
		// testing.
		if !strings.Contains(r.name, "s"+frozenClockR) {
			t.Fatalf("project name %q does not carry the frozen clock (%s -> %s); the date stub was not used, so this test proves nothing about the pid",
				r.name, frozenClock, frozenClockR)
		}
		if !strings.Contains(r.name, "r"+frozenNonceR) {
			t.Fatalf("project name %q does not carry the frozen nonce (%s -> %s); the od stub was not used, so uniqueness here could come from the nonce rather than the pid",
				r.name, frozenNonce, frozenNonceR)
		}
		if seen[r.name] {
			t.Fatalf("two concurrent processes derived the same project name %q with both the clock and the nonce frozen. "+
				"Identity is not process-unique, so two overlapping runs share a project — and the one whose `up` loses the "+
				"race for the host ports tears down the other's stack.", r.name)
		}
		seen[r.name] = true
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
	name := projectNameFrom(t, stubDir(t, "1789700000N", frozenNonce))
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

// TestSmokeScriptsRejectLibraryModeOnDirectExecution covers the other way a
// suite goes quietly green.
//
// Both smoke scripts expose a library mode so tests can source them without
// needing Docker. The hook ended `return 0 2>/dev/null || exit 0`, and when the
// script is executed rather than sourced the `return` fails and the `exit 0`
// runs — the script reports success having asserted nothing. `make
// smoke-compose` and `make smoke-kind` inherit the environment, so a single
// stray export turned the entire suite into a pass. Executing with the variable
// set must fail loudly instead.
func TestSmokeScriptsRejectLibraryModeOnDirectExecution(t *testing.T) {
	for _, tc := range []struct{ script, env string }{
		{"compose_smoke.sh", "OBS_COMPOSE_SMOKE_LIB_ONLY"},
		{"kind_smoke.sh", "OBS_KIND_SMOKE_LIB_ONLY"},
	} {
		t.Run(tc.script, func(t *testing.T) {
			cmd := exec.Command("bash", tc.script)
			cmd.Env = append(os.Environ(), tc.env+"=1")
			out, err := cmd.CombinedOutput()

			if err == nil {
				t.Fatalf("`%s=1 bash %s` exited 0. Run through its make target with that variable "+
					"exported, the whole suite reports success having asserted nothing.\n%s",
					tc.env, tc.script, out)
			}
			if !strings.Contains(string(out), tc.env) {
				t.Errorf("%s refused, but the message does not name %s, so a reader cannot tell what to unset:\n%s",
					tc.script, tc.env, out)
			}
		})
	}
}

// TestSmokeScriptsStillSourceInLibraryMode is the other half: the refusal above
// must not have broken the sourcing path the tests depend on.
func TestSmokeScriptsStillSourceInLibraryMode(t *testing.T) {
	for _, tc := range []struct{ script, env, probe string }{
		{"compose_smoke.sh", "OBS_COMPOSE_SMOKE_LIB_ONLY", `printf '%s' "$PROJECT"`},
		{"kind_smoke.sh", "OBS_KIND_SMOKE_LIB_ONLY", `printf '%s' "$(type -t prom_sample_count)"`},
	} {
		t.Run(tc.script, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", `source "$1"; `+tc.probe, "bash", tc.script)
			cmd.Env = append(os.Environ(), tc.env+"=1")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("sourcing %s in library mode failed: %v\n%s", tc.script, err, out)
			}
			if strings.TrimSpace(string(out)) == "" {
				t.Errorf("sourcing %s in library mode produced nothing; the hook no longer exposes what tests source it for", tc.script)
			}
		})
	}
}
