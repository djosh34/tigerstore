//go:build !linux

// This file lets the os package compile on non-linux go builds.
//
// TODO: Fix build tags somehow to make this file unnecessary.

package simulation

import "github.com/jellevandenhooff/gosim/internal/simulation/syscallabi"

type LinuxOS struct{}

type linuxOSIface interface{}

func NewLinuxOS(simulation *Simulation, machine *Machine, dispatcher syscallabi.Dispatcher) *LinuxOS {
	return &LinuxOS{}
}

func (l *LinuxOS) doShutdown() {
}
