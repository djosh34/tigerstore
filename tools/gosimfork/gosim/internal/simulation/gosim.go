package simulation

import (
	"fmt"
	"net/netip"
	"syscall"
	"time"

	"github.com/jellevandenhooff/gosim/internal/simulation/fs"
	"github.com/jellevandenhooff/gosim/internal/simulation/network"
	"github.com/jellevandenhooff/gosim/internal/simulation/syscallabi"
)

const defaultTimeout = 10 * time.Minute

// GosimOS implements all simulation-level system calls, such as creating
// machines, messing with the network, etc.
type GosimOS struct {
	dispatcher syscallabi.Dispatcher

	simulation *Simulation

	// TODO: this is currently protected by simulation.mu.

	// TODO: have a common by-ID scheme because I don't want infinite maps, please
	nextCrashIterId int
	crashItersById  map[int]func() (int, bool)
}

func NewGosimOS(s *Simulation, d syscallabi.Dispatcher) *GosimOS {
	return &GosimOS{
		dispatcher: d,

		simulation: s,

		crashItersById: make(map[int]func() (int, bool)),
	}
}

func (g *GosimOS) SetSimulationTimeout(d time.Duration, invocation *syscallabi.Syscall) error {
	g.simulation.timeoutTimer.Reset(d)
	return nil
}

func (g *GosimOS) MachineNew(label string, addrStr string, program any, invocation *syscallabi.Syscall) (machineId int) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	// TODO: handle failure but do allow an empty addrStr to indicate unset addr
	addr, _ := netip.ParseAddr(addrStr)

	bootProgram := program.(func())

	machine := g.simulation.newMachine(label, addr, fs.NewLinuxFilesystem(), bootProgram)

	// TODO: don't start machine???
	g.simulation.startMachine(machine)

	return machine.id
}

func (g *GosimOS) MachineRecoverInit(machineID int, program any, invocation *syscallabi.Syscall) int {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	fs := machine.filesystem

	// TODO: clone the filesystem or somehow prevent modifications
	iter := fs.IterCrashes()
	counter := 0

	id := g.nextCrashIterId
	g.nextCrashIterId++

	bootProgram := program.(func())

	g.crashItersById[id] = func() (int, bool) {
		fs, ok := iter()
		if !ok {
			return 0, false
		}

		label := fmt.Sprintf("%s-iter-recover-%d", machine.label, counter)
		counter++

		var addr netip.Addr
		newMachine := g.simulation.newMachine(label, addr, fs, bootProgram)
		g.simulation.startMachine(newMachine)

		return newMachine.id, true
	}
	return id
}

func (g *GosimOS) MachineRecoverNext(crashIterId int, invocation *syscallabi.Syscall) (int, bool) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	// XXX: jank
	iter := g.crashItersById[crashIterId]
	return iter()
}

func (g *GosimOS) MachineRecoverRelease(crashIterId int, invocation *syscallabi.Syscall) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	// XXX: jank
	delete(g.crashItersById, crashIterId)
}

func (g *GosimOS) MachineInodeInfo(machineID int, inode int, info syscallabi.ValueView[fs.InodeInfo], invocation *syscallabi.Syscall) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	info.Set(machine.filesystem.GetInodeInfo(inode))
}

func (g *GosimOS) MachineWait(machineID int, invocation *syscallabi.Syscall) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	if machine.runtimeMachine == nil {
		// XXX: improve how we mark stopped please
		invocation.Complete()
	} else {
		machine.waiters = append(machine.waiters, invocation)
	}
}

func (g *GosimOS) MachineStop(machineID int, graceful bool, invocation *syscallabi.Syscall) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	g.simulation.stopMachine(machine, graceful)
}

func (g *GosimOS) MachineRestart(machineID int, partialDisk bool, invocation *syscallabi.Syscall) (err error) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]

	if machine.runtimeMachine != nil {
		return syscall.EALREADY
	}

	machine.filesystem = machine.filesystem.CrashClone(partialDisk)
	g.simulation.startMachine(machine)

	// XXX: return a new machine ID?
	// what if a machine gets restarted multiple times?
	// do we track some kind of status?
	return nil
}

func (g *GosimOS) MachineGetLabel(machineID int, invocation *syscallabi.Syscall) (label string, err error) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]

	return machine.label, nil
}

func (g *GosimOS) MachineSetBootProgram(machineID int, program any, invocation *syscallabi.Syscall) (err error) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	machine.bootProgram = program.(func())

	return nil
}

func (g *GosimOS) MachineSetSometimesCrashOnSync(machineID int, value bool, invocation *syscallabi.Syscall) (err error) {
	g.simulation.mu.Lock()
	defer g.simulation.mu.Unlock()

	machine := g.simulation.machinesById[machineID]
	// XXX: on restart how do we copy these?
	machine.sometimesCrashOnSyncMu.Lock()
	machine.sometimesCrashOnSync = true
	machine.sometimesCrashOnSyncMu.Unlock()
	return nil
}

func mustHostPair(a, b string) network.HostPair {
	return network.HostPair{
		SourceHost: netip.MustParseAddr(a), DestHost: netip.MustParseAddr(b),
	}
}

func (g *GosimOS) SetConnected(a, b string, connected bool, invocation *syscallabi.Syscall) error {
	g.simulation.network.SetConnected(mustHostPair(a, b), connected)
	return nil
}

func (g *GosimOS) SetDelay(a, b string, delay time.Duration, invocation *syscallabi.Syscall) error {
	g.simulation.network.SetDelay(mustHostPair(a, b), delay)
	return nil
}
