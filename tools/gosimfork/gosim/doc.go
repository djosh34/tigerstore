/*
Package gosim contains the main API for interacting with Gosim simulations.
Gosim is a simulation testing framework that aims to make it easier to write
reliable distributed systems code in go. See the README.md file for an
introduction to Gosim.

# Introduction

Distributed systems can fail in many surprising ways. Exhaustively thinking of
all things that can go wrong and writing tests for them is infeasible. Gosim
simulates the external components that a program might interact with, such as
the disk, network, clocks, and more. Gosim has an API to introduce failures in
these systems (like chaos testing) to test that a program can handle otherwise
difficult-to-reproduce failures.

# Writing and running gosim tests

Gosim tests are normal Go tests that are run in a Gosim simulation. To run Gosim
tests, use the 'gosim test' command with similar arguments as the standard 'go
test' command:

	# run an normal test
	go test -run TestName -v ./path/to/pkg
	# run a gosim test
	go run github.com/jellevandenhooff/gosim/cmd/gosim test -run TestName -v ./path/to/pkg

For more information on the 'gosim' command see its documentation at
[github.com/jellevandenhooff/gosim/cmd/gosim].  To run Gosim tests from within a
Go test, use the
[github.com/jellevandenhooff/gosim/metatesting] package.

# Gosim simulation

When tests run inside Gosim they run in a simulation and they do not interact
with the normal operating system. Gosim simulates:

  - Real time. The time is simulated by Gosim (like on the Go playground) so that
    it advances automatically when all goroutines are paused. This means tests run
    quickly even if they sleep for a long time.

  - The filesystem. Gosim implements its own filesystem that can simulate data
    loss when writes are not fsync-ed properly.

  - The network. Gosim implements its own network that can introduce extra latency
    and partition machines.

  - Machines. Inside of Gosim a single Go test can create multiple machines which
    are new instantiations of a Go program with their own global variables
    (except //go:embed variables, which they share), their own disk, and their
    own network address.

Gosim implements its simulation at the Go runtime and system call level,
emulating Linux system calls. Gosim compiles all code with GOOS set to linux. To
interact with the simulation, programs can use the standard library with
functions like [time.Sleep], [os.OpenFile], [net.Dial], and all others working
as you would expect.

To control the simulation, this package has functions like [NewMachine] and
[Machine.Crash] to create and manipulate machines and [SetConnected] to
manipulate the network.

# Gosim internals

For a description of how Gosim works, see the design in
https://github.com/jellevandenhooff/gosim/blob/main/docs/design.md
*/
package gosim
