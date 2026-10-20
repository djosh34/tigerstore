//go:build sim

package simrun

import (
	"encoding/json"
	"strconv"
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
func Drive(*testing.T, uint64) bool { return false }

// each runs the seeds, which the driver gives one at a time, and saves
// what body returned for it (Seeds).
func each[T any](t *testing.T, seeds []uint64, _ []string, body func(*testing.T, uint64) T, _ func(uint64, T) bool) bool {
	t.Helper()
	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			b, err := json.Marshal(body(t, seed))
			if err != nil {
				t.Fatal(err)
			}
			Artifact("seed.json", b)
		})
	}
	return false
}
