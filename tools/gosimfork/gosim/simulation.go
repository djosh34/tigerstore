package gosim

import (
	"errors"
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

// RealMark writes msg to the real stderr with the real time (#441).
func RealMark(msg string) { gosimruntime.RealMark(msg) }

// RealNanotime is the real clock (#441).
func RealNanotime() int64 { return gosimruntime.RealNanotime() }

// RealFile is a file on the real disk (#441).
type RealFile struct{ id int }

// RealCreate creates a file on the real disk, nil if it fails.
func RealCreate(path string) *RealFile {
	id := gosimruntime.RealCreate(path)
	if id == 0 {
		return nil
	}
	return &RealFile{id}
}

func (f *RealFile) Write(b []byte) (int, error) {
	if !gosimruntime.RealWrite(f.id, b) {
		return 0, errRealWrite
	}
	return len(b), nil
}

func (f *RealFile) Close() error {
	if !gosimruntime.RealClose(f.id) {
		return errRealWrite
	}
	return nil
}

var errRealWrite = errors.New("real file write failed")
