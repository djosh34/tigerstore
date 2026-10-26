package gosim

import (
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/simulation"
)

// IsSim returns if the program is running inside of a gosim simulation.
//
// To include or exclude code or tests only if running inside of a gosim the
// gosim tool also supports a build tag. Use the `//go:build sim` build
// constraint at the top of a go file to only include it in simulated builds or
// `//go:build !sim` to exclude it from simulated from builds.
func IsSim() bool {
	return gosimruntime.IsSim()
}

// Yield yields the processor, allowing other goroutines to run. Similar to
// [runtime.Gosched] but for gosim's scheduler.
//
// TODO: Replace with runtime.Gosched() instead?
func Yield() {
	gosimruntime.Yield()
}

/*
type Simulation struct{}

func CurrentSimulation() Simulation {
	return Simulation{}
}

// SetConnected configures the network connectivity between addresses a and b.
//
// TODO: How will this fail? Unrouteable or rejected
// TODO: Support asymmetry
func (s Simulation) SetConnected(a, b string, connected bool) error {
	return simulation.SyscallSetConnected(a, b, connected)
}

// SetDelay configures the network latency between addresses a and b.
//
// TODO: Support asymmetry
func (s Simulation) SetDelay(a, b string, delay time.Duration) error {
	return simulation.SyscallSetDelay(a, b, delay)
}

// SetSimulationTimeout resets the simulation timeout to the given duration.
// After the simulated duration the simululation will fail with a test timeout
// error.
func (s Simulation) SetSimulationTimeout(timeout time.Duration) {
	err := simulation.SyscallSetSimulationTimeout(timeout)
	if err != nil {
		panic(err)
	}
}
*/

// SetConnected configures the network connectivity between addresses a and b.
//
// TODO: How will this fail? Unrouteable or rejected
// TODO: Support asymmetry
func SetConnected(a, b string, connected bool) error {
	return simulation.SyscallSetConnected(a, b, connected)
}

// SetDelay configures the network latency between addresses a and b.
//
// TODO: Support asymmetry
func SetDelay(a, b string, delay time.Duration) error {
	return simulation.SyscallSetDelay(a, b, delay)
}

// SetSimulationTimeout resets the simulation timeout to the given duration.
// After the simulated duration the simululation will fail with a test timeout
// error.
func SetSimulationTimeout(d time.Duration) {
	err := simulation.SyscallSetSimulationTimeout(d)
	if err != nil {
		panic(err)
	}
}

// WriteArtifact saves a copy of data under name in the result of the current
// run, for the test that started the run: a test using metatesting reads it
// from RunResult.Artifacts. The simulation cannot read it back. Writing a name
// again replaces its data.
func WriteArtifact(name string, data []byte) {
	gosimruntime.WriteArtifact(name, data)
}

// Reseed sets the scheduler's random state from seed (gosimruntime.Reseed).
func Reseed(seed int64) { gosimruntime.Reseed(seed) }
