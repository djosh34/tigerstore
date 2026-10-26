package gosim

import (
	"iter"
	"net/netip"

	"github.com/jellevandenhooff/gosim/internal/simulation"
	"github.com/jellevandenhooff/gosim/internal/simulation/fs"
)

// A Machine represents a simulated machine. Each machine has its own disk,
// network stack, and set of global variables. Machines can only communicate
// over the network.
//
// A Machine can be stopped and restarted. After a restart, a Machine has
// a new, freshly-initialized set of global variables, but the same disk
// and network address as before. The exception is //go:embed variables: the
// machines of a run share them, and they get their embedded values again only
// at the start of a run.
//
// Once a machine's main function returns, the machine stops as
// if called [Machine.Stop] with all writes flushed to disk and
// all network connections closed gracefully.
//
// When a machine stops all code running on the machine is
// stopped without running any pending `defer`ed calls.
// All timers are stopped and will not fire.
type Machine struct {
	// underlying
	id int
}

// CurrentMachine returns the Machine the caller is running on.
func CurrentMachine() Machine {
	return Machine{id: simulation.CurrentMachineID()}
}

// NewSimpleMachine creates a new Machine that will run the given main function. The
// Machine will start immediately.
//
// See [Machine] for a detailed description of machines and their behavior.
func NewSimpleMachine(mainFunc func()) Machine {
	machineID := simulation.SyscallMachineNew("", "", mainFunc)
	// TODO: make a New machine default to a Stopped state?
	return Machine{id: machineID}
}

// MachineConfig configures a Machine.
type MachineConfig struct {
	// Label is the displayed name of a machine in the logs and the hostname
	// returned by os.Hostname. Optional, will be a numbered machine if
	// empty.
	Label string
	// Addr is the network address of the machine. Only IPv4 addresses are
	// currently supported. Optional, will be allocated if empty.
	Addr netip.Addr
	// MainFunc is the starting function of the machine. When MainFunc returns
	// the machine will be stopped.
	MainFunc func()
}

// NewMachine creates a new Machine with the given configuration.
// The Machine will start immediately.
//
// See [Machine] for a detailed description of machines and their behavior.
func NewMachine(opts MachineConfig) Machine {
	machineID := simulation.SyscallMachineNew(opts.Label, opts.Addr.String(), opts.MainFunc)
	return Machine{id: machineID}
}

// Crash stops the machine harshly. A crash does not flush any non-fsynced
// writes to disk, and does not properly close open network connections.
func (m Machine) Crash() {
	// TODO: add more tests around stop/crash/restart, for current machine/others.
	// TODO: document what Crash does if the machine is already stopped?

	// if m == CurrentMachine() {
	// panic("help can't crash self (yet)")
	// }
	simulation.SyscallMachineStop(m.id, false)
}

// Stop stops the machine gracefully. Pending writes will be fsynced to disk,
// and open network connections will be closed.
func (m Machine) Stop() {
	// TODO: document and consider difference between Crash and Stop?
	simulation.SyscallMachineStop(m.id, true)
}

// SetMainFunc changes the main function of the machine for future restarts.
func (m Machine) SetMainFunc(mainFunc func()) {
	if err := simulation.SyscallMachineSetBootProgram(m.id, mainFunc); err != nil {
		panic(err)
	}
}

// Restart restarts the machine. The machine must be stopped
// before Restart can be called.
func (m Machine) Restart() {
	if m == CurrentMachine() {
		panic("help can't crash self (yet)")
	}
	if err := simulation.SyscallMachineRestart(m.id, false); err != nil {
		panic(err)
	}
}

// RestartWithPartialDisk restarts the machine. A random subset of non-fsynced
// writes will be flushed to disk.
// TODO: Move this partial flushing to crash instead.
func (m Machine) RestartWithPartialDisk() {
	if m == CurrentMachine() {
		panic("help can't crash self (yet)")
	}
	if err := simulation.SyscallMachineRestart(m.id, true); err != nil {
		panic(err)
	}
}

// InodeInfo holds low-level information of the requested
// inode.
//
// TODO: make internal?
// TODO: copy pasted from fs
type InodeInfo struct {
	MemLinks   int
	MemExists  bool
	DiskLinks  int
	DiskExists bool
	Handles    int
	Ops        int
}

// GetInodeInfo inspects the disk of the machine, returning
// low-level information of the requested inode.
func (m Machine) GetInodeInfo(inode int) InodeInfo {
	var info fs.InodeInfo
	simulation.SyscallMachineInodeInfo(m.id, inode, &info)
	return InodeInfo(info)
}

/*
func (m *Machine) Recover() {
	// XXX: require a crash beforehand?
	// XXX: go through OS somehow?
	simCrash(m.os.filesystem)
}
*/

// XXX: add some tests that simCrash returns a subset of (or all?) traces found by Recover

// Wait waits for the machine to finish to stop. A machine stops when its main
// function returns or when it is explicitly stopped with [Machine.Stop] or
// [Machine.Crash]. For details, see the documentation of [Machine].
func (m Machine) Wait() {
	simulation.SyscallMachineWait(m.id)
}

// IterDiskCrashStates iterates over all possible subsets of partially
// fsync-ed writes on the disk of the crashed machine.
//
// TODO: Make this iterate over disks instead?
// TODO: Make internal?
func (m Machine) IterDiskCrashStates(program func()) iter.Seq[Machine] {
	iter := simulation.SyscallMachineRecoverInit(m.id, program)
	called := false
	return func(yield func(Machine) bool) {
		if called {
			panic("iterator only works once")
		}
		called = true
		defer simulation.SyscallMachineRecoverRelease(iter)
		for {
			machineID, ok := simulation.SyscallMachineRecoverNext(iter)
			if !ok {
				return
			}
			m := Machine{id: machineID}
			if !yield(m) {
				break
			}
		}
	}
}

// SetSometimesCrashOnSync configures the machine to sometimes crash
// before or after the fsync system call.
//
// TODO: What is the probability?
// TODO: Have a generic mechanism that covers all system calls instead?
func (m Machine) SetSometimesCrashOnSync(crash bool) {
	if err := simulation.SyscallMachineSetSometimesCrashOnSync(m.id, crash); err != nil {
		panic(err)
	}
}

// Label returns the label of the machine.
func (m Machine) Label() string {
	label, err := simulation.SyscallMachineGetLabel(m.id)
	if err != nil {
		panic(err)
	}
	return label
}
