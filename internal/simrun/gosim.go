//go:build sim

package simrun

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
	"github.com/jellevandenhooff/gosim/gosimruntime"
)

// InGosim reports whether the tests run under gosim.
const InGosim = true

// Race reports whether the tests run natively under the race detector.
const Race = false

// Bubble runs f, which runs one simulation. gosim runs the whole test on
// its clock already; it fails a run that takes longer than its timeout of
// virtual time, which Bubble sets to an hour from now.
func Bubble(t *testing.T, f func(*testing.T)) { BubbleFor(t, time.Hour, f) }

// BubbleFor is Bubble with a timeout of d from now, for a simulation that
// lasts longer than an hour.
func BubbleFor(t *testing.T, d time.Duration, f func(*testing.T)) {
	gosim.SetSimulationTimeout(d)
	f(t)
}

// WallLimitPassed reports whether the run passed its limit of real time,
// which the driver sets ($SIM_WALL).
func WallLimitPassed() bool { return gosim.WallLimitPassed() }

// CPUTime is the CPU time the process has used so far, on all of its
// threads. It measures what a run's work costs, and must never change
// what the run does.
func CPUTime() (time.Duration, error) { return gosim.CPUTime() }

// TimerFirings is how many timers fired in the run so far: wake-ups from
// time.Sleep, sends on timers' and tickers' channels and AfterFunc calls,
// of the nodes and the simulator alike. Reading it changes nothing in the
// run.
func TimerFirings() uint64 { return gosim.TimerFirings() }

// Aside runs f, which must not block, without drawing from the run's
// random stream: gosim's maps draw from it on inserts and iteration, so
// work the simulator does for itself, such as keeping its records, would
// change every later choice of the run.
func Aside(f func()) {
	gosimruntime.BeginControlledNondeterminism()
	defer gosimruntime.EndControlledNondeterminism()
	f()
}

// Stacks returns every goroutine's stack, the simulated nodes' included.
func Stacks() []byte { return gosim.Stacks() }

// Artifact saves data under name for the driver of the run.
func Artifact(name string, data []byte) { gosim.WriteArtifact(name, data) }

// Here reports whether the test runs its body in this process: always
// under gosim.
func Here(*testing.T) bool { return true }

// Drive reports false: under gosim the test runs its body.
func Drive(*testing.T, uint64) bool { return false }

// Do starts a gosim run, which only the native driver does: under gosim a
// test of a set never gets to the driver's part of it (Seeds).
func Do(t *testing.T, r Run) *Result {
	t.Helper()
	t.Fatalf("%s: a gosim run starts only from the native driver", r.Test)
	return nil
}

// each runs the seeds, which the driver gives one at a time, and saves
// what body returned for it (Seeds). Then it runs what AfterSeed set.
func each[T any](t *testing.T, seeds []uint64, _ []string, body func(*testing.T, uint64) T, _ func(uint64, T) bool) bool {
	t.Helper()
	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			defer func() {
				for _, f := range afterSeed {
					f()
				}
				afterSeed = nil
			}()
			b, err := json.Marshal(body(t, seed))
			if err != nil {
				t.Fatal(err)
			}
			Artifact("seed.json", b)
		})
	}
	return false
}

// afterSeed is what runs after the seed's last simulation.
var afterSeed []func()

// AfterSeed runs f once the seed's last simulation is over. Work between
// two simulations of one gosim run moves the later one, so work that only
// some runs of a seed do, such as saving it for a double run, waits.
func AfterSeed(f func()) { afterSeed = append(afterSeed, f) }
