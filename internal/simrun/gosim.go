//go:build sim

package simrun

import (
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
)

// InGosim reports whether the tests run under gosim.
const InGosim = true

// Race reports whether the tests run natively under the race detector.
const Race = false

// Bubble runs f, which runs one simulation. gosim runs the whole test on
// its clock already; it fails a run that takes longer than its timeout of
// virtual time, which Bubble sets to an hour from now.
func Bubble(t *testing.T, f func(*testing.T)) {
	gosim.SetSimulationTimeout(time.Hour)
	f(t)
}

// Artifact saves data under name for the driver of the run.
func Artifact(name string, data []byte) { gosim.WriteArtifact(name, data) }

// Here reports whether the test runs its body in this process: always
// under gosim.
func Here(*testing.T) bool { return true }

// Drive reports false: under gosim the test runs its body.
func Drive(*testing.T) bool { return false }

// Do drives runs from outside gosim only.
func Do(t *testing.T, _ Run) *Result {
	t.Helper()
	t.Fatal("simrun.Do runs gosim from outside it")
	return nil
}

// dir names the package's directory for replay lines, which only the
// driver prints.
func dir(*testing.T) string { return "." }
