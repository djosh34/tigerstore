//go:build planted

package sim

import (
	"regexp"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
)

// TestDiag269 repeats exactly the simulation used by B01a seed 1, including
// the end-of-run convergence check. It omits only independent/witness checks
// that run after convergence and can each spend a minute searching.
func TestDiag269(t *testing.T) {
	planted.Active, activePlanted = "B01a", "B01a"
	t.Cleanup(func() { planted.Active, activePlanted = "", "" })
	res, err := runSeed(t, DefaultConfig(1))
	if res != nil {
		for _, line := range res.Trace {
			if strings.Contains(line, "diag269:") {
				t.Log(line)
			}
		}
	}
	if err == nil {
		t.Log("seed 1 converged; no internal checker found the planted stale read")
		return
	}
	if !regexp.MustCompile(staleRead).MatchString(err.Error()) {
		t.Fatal(err)
	}
	t.Logf("seed 1 converged; expected planted failure: %v", err)
}
