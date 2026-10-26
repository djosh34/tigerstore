# Goals

The goal of Gosim is to make writing correct systems code in Go easier and more
fun. To accomplish that goal Gosim is designed to:

- Fit in with Go. Testing with Gosim should be like normal testing. Its APIs
follow the conventions from `go test` and the `testing` packages.

- Give fast feedback. Gosim should work well with a frequent `gosim test`
workflow.

- Work with the Go ecosystem. Gosim should not require applications to be
rewritten in a different style, and should support commonly used packages like
gRPC.

- Be understandable. Gosim's simulation is involved with many moving pieces.
Those should be as debuggable as possible for when things go wrong.

# How Gosim works

Gosim runs Go code in a simulated environment. Gosim includes its own
lightweight simulated Go runtime and implementations of Linux system calls
backed by a network and disk simulation. Gosim's implementation consists of
three layers:

The first layer is Gosim's simulated Go runtime which implements the basic
features of the Go language and runtime: goroutines, time, maps, and
synchronization with channels and semaphores.  The simulated runtime provides
determinism and complete control over scheduling and time. This lets Gosim
accelerate time when goroutines are paused. Gosim translates standard Go code to
call into its runtime, and then compiles the translated code using the standard
Go compiler. This translation replaces eg. `go foo()` with
`gosimruntime.Go(foo)`.

The second layer is a set of hooks in the Go standard library to call into
Gosim's runtime instead of the normal runtime. This makes the standard
`sync.Mutex` or `time.AfterFunc` call into `gosimruntime` instead of the normal
Go runtime.

The final layer is Gosim's simulated operating system. Gosim simulates Linux
system calls with its fake operating system. This fake operating system is
written in standard Go and runs on Gosim's runtime. All system calls in the
standard library are replaced to call into Gosim's OS, so that
`os.Open` eventually calls `simulation.LinuxOS.SysOpenat`.

Gosim exposes a high-level API in the `gosim` package that lets tests create
simulated machines, crash them, disconnect them, and more, to verify that
applications can handle those scenarios.

To run the tests Gosim includes a CLI `gosim` with a `gosim test` command that
translates a package, runs its with Gosim's runtime and OS, and prints
the result as if it were a normal `go test`.

## The deterministic runtime

Gosim's deterministic runtime runs goroutines in a controlled way using
cooperative scheduling. Only one goroutine runs at a time and at every
synchronization point the goroutine calls into the scheduler to consider
switching to another goroutine. This scheduling decision is made using a
deterministic random number generator, and if a program is run again it will
make the exact same scheduling decisions.

Behind the scenes, each simulated goroutine is implemented using a coroutine
(using the API underlying `iter.Pull`). This lets Gosim quickly switch between
goroutines.

Besides the non-deterministic concurrency primitives, Go also exposes
non-determinism in the `rand` package and with `map` iteration order. Gosim
replaces the randomness in `rand` with its own seeded random number generator,
and provides a `map` that has varying yet deterministic iteration order.

Not all sources of non-determinism are controlled by Gosim. For example, memory
allocation will give different pointers. Code that behaves non-deterministically
can be flagged by Gosim's tracing mechanism, which calculates a running hash
over all events like spawning goroutines, context switches, etc.

The implementation of the runtime is in the
[github.com/jellevandenhooff/gosim/gosimruntime](../gosimruntime/) package.

## Simulated time

To simulate time Gosim tracks the state of every goroutine. If all goroutines
are waiting for time to advance, Gosim advances the clock to the next earlier
timestamp that any goroutine is waiting for. This simulated time lets Gosim run
programs that wait for a long simulated time in less real time.

For example, consider the program

```go
// main goroutine
ch := make(chan struct{})
go func() { // goroutine A
    time.Sleep(time.Hour)
    close(ch)
}()
var wg sync.WaitGroup
wg.Add(1)
go func() { // goroutine B
    defer wg.Done()
    <-ch
    time.Sleep(time.Minute)
}()
wg.Wait()
```

At the start of this program, the main goroutine and the two spawned goroutines
will all be waiting. The main goroutine is waiting on `wg.Wait()`, B is waiting
on `<-ch`, and A is waiting on `time.Sleep(time.Hour)`. When running this code,
the Gosim runtime will see all goroutines paused and advance time by one hour.
Then A will close the channel, B will awaken and call `time.Sleep(time.Minute)`,
time will be advanced again, etc.

## The translator

To make existing Go programs use the deterministic runtime, Gosim translates
existing code to call into its own runtime. The translator works at the AST
level and replaces all interactions with channels, maps, and goroutines with
calls to its own runtime. For example, it rewrites

```go
go func() {
    log.Println("hello!")
}()
```

to

```go
gosimruntime.Go(func() {
    log.Println("hello!")
})
```

and

```go
m = map[int]string{
    1: "ok",
    2: "bar",
}
log.Println(m[1])
```

to

```go
m = gosimruntime.MapLiteral[int, string]{
    {K: 1, V: "ok"},
    {K: 2, V: "bar"},
}.Build()
log.Println(m.Get(1))
```

After translating, Gosim compiles the translated code using the
standard Go compiler, and it can be debugged or instrumented with
standard Go tools like `go tool pprof` or `delve`.

The translator runs on almost all code, including the standard library.  Only
the `gosimruntime` packages remains untranslated. Translated code can access
non-translated code by annotating an import with `//gosim:notranslate`.  This
lets, for example, Gosim's wrapper `reflect` package access the underlying real
`reflect` package.

The github.com/jellevandenhooff/gosim/internal/translator package implements the
translator. It is exposed through the Gosim CLI.

An alternative design could have modified the Go runtime or compiler to instead
compile standard Go programs into a deterministic version, but the runtime and
compiler are modified much more often than the Go language specification.
Hopefully maintaining this translator will be less work than maintaining a
custom compiler.

## Standard library hooks

Gosim translates not only application code to be tested, but also
the entire Go standard library. The interface between the Go standard
library and the runtime library is a (relatively) small set of functions,
such as

```go
func newTimer(when, period int64, f func(arg any, seq uintptr, delay int64), arg any, c *hchan) *timeTimer
```

which lives in `runtime/time.go`. For all such functions Gosim has hooks in the
package github.com/jellevandenhooff/gosim/internals/hooks/go127. The hooks are
Go version specific because there are no API stability guarantees for these
low-level unexported runtime functions. The hooks in Gosim are implemented as
calls to the gosimruntime package.

By translating the entire go standard library, applications should notice
as few differences between reality and simulation.

An alternative design (and an earlier version of Gosim) instead replaced
higher-level functions like `os.OpenFile` with simulated or mocked versions.
However, implementing all functions used by programs in a faithful way is very
difficult. Although the internal API that Gosim now replaces does not have
stability guarantees, it is much smaller than the entire standard library.

Gosim wraps the `reflect` package with its own version to hide the differences
between the built-in `map` and Gosim's `gosimruntime.Map`.

## Globals and machines

Gosims runs multiple simulated machines in own go process, each with their own
copy of global variables. This duplication is necessary because there are quite
a few important global variables, such as the `net/http` connection pool, that
make no sense to share between machines.

The translator replaces all accesses to such variables to call into a special
gosimruntime call

```go
var global string

func Get() string {
    return global
}
```

becomes

```go
type Globals struct {
    global string
}

func G() *Globals {
    // call to gosimruntime to get an appropriate pointer
}

func Get() string {
    return G().global
}
```

This lets gosim faithfully simulate multiple go processes in a single go process.

Whenever gosim starts a new simulated machine, it allocates and initializes all
global variables by calling each package's `init()` functions. Some globals are
large, take a long time to initialize, and are never modified afterwards; for
example, the unicode tables. As an optimization, Gosim allocates and
initializes them only once.

Communication between machines is tricky. Because each machine has its own
copy of global variables, sharing objects between machines can cause all kinds
of issues: For example, `io.EOF` can be a different object on each machine,
and so checking if an error is a sentinel value no longer works.

To prevent problems, machines communicate only between eachother using the
simulated operating system. The operating system and each machine communicate
over a channel (which does work between machines) and work on raw values that
are safe to share.

An alternative design might run each simulated go process in its own physical
process. However, the overhead for communicating and starting such processes
seems high, although I have not benchmarked this.

## The system call interface

Gosim simulates the operating system as an independent simulated go process.
The operating system has a model of a network, filesystem, etc., all stored as
Go structs, etc. The programs that Gosim tests call into this using its syscall
interface defined in
github.com/jellevandenhooff/gosim/internal/simulation/syscallabi.Syscall.
This is a RPC-like mechanism that works safely between Gosim's simulated machines.

At a code level, Gosim's translator replaces the wrapper functions in the
`syscall` package that call `syscall6` with its own wrapper functions that
call into its `syscallabi` package. The `gensyscall` tool generates these
wrapper functions, as well as a dispatcher and interface to be implemented
by the simulated operating system.

The calling code for eg. `SYS_PREAD64` becomes

```go
func SyscallSysPread64(fd int, p []byte, offset int64) (n int, err error) {
    // generated wrapper
}
```

and the implementation becomes

```go
func (l *LinuxOS) SysPread64(fd int, data syscallabi.ByteSliceView, offset int64) (int, error) {
    // figure out what to write
    n := data.Write([]byte{...})
    return n, nil
}
```

The `syscallabi` runs all operating system code on its own simulated Go machine.
The boundary is illustrated by the `syscallabi.ByteSliceView` type which lets
the operating system read and write from userspace without triggering the race
detector and makes sure that there are no variables accidentally shared between
machines.

To glue this call to the standard library, the calling code is wrapped in a
standard library hook

```go
func Syscall_pread(fd int, p []byte, offset int64) (n int, err error) {
	return simulation.SyscallSysPread64(fd, p, offset)
}
```

and finally linked into the rewritten standard library with a linkname

```go
//go:linkname pread translated/github.com/jellevandenhooff/gosim/internal_/hooks/go127.Syscall_pread
func pread(fd int, p []byte, offset int64) (n int, err error)
```

The translator uses a linkname to prevent cycles in the import graph.

## The simulated operating system

Gosim's simulated network implements a TCP API by sending TCP-like packets over
a virtual network. Each link on the network can have configurable latency, or
be temporarily disabled, to simulate challenging network conditions. The
simulated network implements this with normal Go timers, arranging for each
packet to be delivered at the correct time.

Gosim's simulated filesystem implements a Posix-style filesystem API,
with read, write, and fsync calls. The simulated filesystem tracks
in-flight writes so that it can simulate machine crashes with all
the tricky behavior that Linux can portray.

## The gosim API for tested programs

Gosim's implementation is spread among a number of internal packages.  To
provide a consistent API to handle machines, the simulated network, etc.  the
[github.com/jellevandenhooff/gosim package](..) provides a high-level API to
create new machines, manipulate the network, etc. Behind the scenes these are
implemented as custom system calls to the simulated operating system.

## The gosim CLI and integration with Go tests

Gosim aspires to be as easy to use as the standard Go tooling. To run a test
using Gosim, ideally all that you need to do is replace `> go test` with
`> gosim test` on the command line. In practice, `gosim` becomes
`go run github.com/jellevandenhooff/gosim/cmd/gosim`
but otherwise the tool exposes flags like `go test`.

When running such a test, `gosim` first translates the code, caching
packages to not retranslate eg. the standard library, and then invokes
`go test` on the translated code.

To integrate with the `go test` tool, the translated code provides its own
`TestMain` function that takes over control of the test execution. Each test is
run on its own simulated machine, so that each test gets its own globals and
will run the same (given the same seed).

## Logging and formatting

To make logs from multiple machines and goroutines readable, Gosim annotates all
logs with their source machine, goroutine, and timestamp. To implement this,
Gosim uses a JSON-formatting `log/slog` handler in each machine that includes
the current machine and goroutine as extra fields. The default `log` handler
writes to the same `log/slog` handler, and all logging code in the `testing`
package is modified as well. The `gosim` CLI by default parses the JSON logs and
pretty-prints them. Behind the scenes all logs are JSON so they can be parsed by
metatesting code.

A simplified log line in JSON might look like

```
{"time":"2020-01-15T14:10:03.000001234Z","level":"INFO","msg":"hello info","machine":"main","foo":"bar","goroutine":4,"step":1}
```

and gets formatted as

```
    1 main/4     14:10:03.000 INF behavior/log_gosim_test.go:53 > hello info foo=bar
```

## Metatesting

A larger module might want to run Gosim tests as parts of its normal `go test`
suite. With the [github.com/jellevandenhooff/gosim/metatesting](../metatesting)
package, normal tests can invoke Gosim tests with varying seeds and inspecting
their logs. Such tests can check that the Gosim tests are deterministic, or that
programs behave as expected by reading their logs.

A future goal of metatesting is to fuzz Gosim tests by controlling seeds and
randomness and seeing how they behave.

## Race detection

To help find concurrency bugs Gosim works with the built-in Go race detector.
Since Gosim translates Go code to call into own runtime and then runs it with
the standard Go compiler, it can use the built-in race detector with only minor
special handling.

The Go compiler will insert all standard race detector read and write calls,
notifying when a goroutine accesses potentially shared memory. This does not
require any extra work. Gosim's runtime is instrumented to call into the race
detector whenever it creates a happens-before dependency, for example in its
channel and semapahore code.

For gosim's simulation to correctly explore all behavior, it is important that
goroutines do not influence eachother between the synchronization points where
Gosim explicitly controls the scheduler. This is exactly what the race detector
verifies.

Gosim's internal runtime state (the structs in the `gosimruntime` package) are
always accessed from a single system goroutine so that accesses do not race.
Whenever a userspace goroutine needs to modify this state (eg. to spawn a new
goroutine), it performs an upcall that temporarily switches to the system
goroutine to modify the runtime state before switching back.

Functions implementing concurrency primitives, eg. channel reads, do no always
switch back to the system goroutine. Those functions are all marked
`//go:norace` to not trigger the race detector.

To test the race detector, gosim includes a copy of (most of) the race detector
tests from the go toolchain.

## Controlled non-determinism

Gosim has an escape hatch to allow local non-determinism that has no externally
visible side-effects. An example of this is the type cache used in the
`encoding/json` package. Gosim allows this type cache to be shared between
machines (and even between Gosim tests) to speed up `encoding/json` code.
Rebuilding type descriptors for every test would be wasteful and does not
explore interesting behavior, so caching the type descriptors is appealing.
However, since this caching logic writes to a shared map and synchronizes access
using a mutex, it does trigger Gosim's non-determinism detector.

A solution to this is the `gosimruntime.BeginControlledNondeterminism` call
which enters a code region that cannot yield nor consume randomness. In essence,
it promises that the non-determinism in that section will not impact other code
and pauses trace recording. If later code does get impacted hopefully the trace
will capture that non-determinism there, but debugging will be difficult. The
translator inserts calls as a patch to some packages.

# Background

## Bug finding in distributed systems

Perhaps most famous in the distributed systems bug finding world is [Jepsen's
blog](https://jepsen.io/blog).  Using their tools
[Jepsen](https://github.com/jepsen-io/jepsen), to run and mess with existing
distributed systems, and [Elle](https://github.com/jepsen-io/elle), to then
validate the behavior of those systems, Jepsen has found many bugs.

Jepsen works with existing systems and runs them on actual cloud servers,
messing with the actual network. Gosim aims to have less overhead and complexity
by simulating the servers and network instead.

## Simulation testing

Simulation testing has a long history. Some current approaches and inspirations
follow. This is not an exhaustive list or deep evaluation.

The 2014 talk
["Testing Distributed Systems w/ Deterministic Simulation" by Will Wilson](https://www.youtube.com/watch?v=4fFDFbi3toc)
introduces simulation testing as used by FoundationDB, later described in
the
[FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf).
The
current code for FoundationDB is once again open-source and contains
examples of the scenarios they run against like
[swizzle clogging](https://github.com/apple/foundationdb/blob/release-7.3/fdbserver/workloads/RandomClogging.actor.cpp)
mentioned in the talk.
FoundationDB's simulation testing intercepts interactions between application
code and its own custom Flow actor and RPC system.

The start-up [Antithesis](https://antithesis.com), founded by some of the people
behind FoundationDB, offers simulation testing-as-a-service on the virtual
machine level. Their [blog](https://antithesis.com/blog/) describes the work
they do.  Antithesis implements determinisim at the virtual machine level, and
can run arbitrary Docker images.

The [Shadow](https://github.com/shadow/shadow) simulation testing tool focuses
on simulating networks and is used primarily by the Tor project. Shadow runs
arbitrary applications and intercepts the system calls they make.

The C# framework [Coyote](https://microsoft.github.io/coyote) by Microsoft is a
simulation testing framework focused on testing actor systems. Coyote simulates
by instrumenting C# programs that use its RPC and actor mechanism.

A framework for Rust [Madsim](https://github.com/madsim-rs/madsim/) is used to
test the RisingWave database https://risingwave.com. Madsim runs Rust programs
and intercepts calls at the Tokio library level. Programs must use Tokio to test
with Madsim.

Another framework for Rust is [Tokio's Turmoil](https://github.com/tokio-rs/turmoil). Turmoil works similar to Madsim
and intercepts at the Tokio library level. Programs must use Tokio to test with
Turmoil.

Gosim's approach for simulating a distributed system at the system call level is
remeniscent of the papers
["Efficient System-Enforced Deterministic Parallelism"](https://dedis.cs.yale.edu/2010/det/papers/osdi10.pdf)
or
["Deterministic Process Groups in dOS"](https://homes.cs.washington.edu/~luisceze/publications/osdi10-dos.pdf).

The [TigerBeetle database](https://github.com/tigerbeetle/tigerbeetle/), written
in Zig, uses a homegrown simulation testing system, VOPR, described at
https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HACKING.md#simulation-tests.
VOPR simulates at a state-machine level and is tightly coupled to TigerBeetle.

## Bug finding in concurrent code

Gosim's scheduler could be used to test low-level concurrent or lock-free code.
The package does not include support, but the approach of carefully scheduling
goroutines at every synchronization point is supported by Gosim's runtime.

The [Loom model checker](https://github.com/tokio-rs/loom) for Rust, now part of
Tokio, was used by AWS in validating a Rust service as described in their paper
[Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3](https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3).

Anoter simple but effective approach for finding bugs in concurrent code is the
PCT algorithm originally described in
[A Randomized Scheduler with Probabilistic Guarantees of Finding Bugs](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/asplos277-pct.pdf).

## Testing filesystem code

Gosim's filesystem simulation with fine-grained fsync support is reminiscent
of [Alice (Application-Level Intelligent Crash Explorer)](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf).
Like Alice, Gosim's filesystem splits file system system calls into smaller
operations that can individually be persisted or lost on crash, and can test
that an application can handle those scenarios.

## Testing strategies for existing Go code

Many existing distributed systems are written in Go and they use a variety
of testing strategies for their critical components.

The Raft library [github.com/etcd-io/raft](https://github.com/etcd-io/raft) is
used by both [Etcd](https://github.com/etcd-io/etcd) and [CockroachDB](https://github.com/cockroachdb/cockroach). The critical [Raft state machine in
Etcd](https://github.com/etcd-io/raft/blob/main/raft.go) is implemented with side-effect free
transition functions that take in events like time advancing
and receiving messages. Outgoing RPCs are buffered in the implementation. This design lends itself to
[thorough tests](https://github.com/etcd-io/raft/tree/main/testdata) that cover in detail
how the Raft state machine should behave among different inputs.

The key-value library
[github.com/cockroachdb/pebble](https://github.com/cockroachdb/pebble) backing
CockroachDB tests, among others, with an error-injecting virtual filesystem.

The Raft library [github.com/hashicorp/raft](https://github.com/hashicorp/raft)
used by [Consul](https://github.com/hashicorp/consul) has
[fuzzy tests](https://github.com/hashicorp/raft/tree/main/fuzzy) that check for
correctness under tricky network conditions by instrumenting their RPC stack

Many tests for Go try to mock Go's built-in time package to test
time-outs without tests becoming slow. One initial package
was [github.com/benbjohnson/clock](https://github.com/benbjohnson/clock).
A big difficulty with mocking the clock is that is not clear when
time should advance.

The [testing/synctest proposal](https://github.com/golang/go/issues/67434)
adds a new API for mocking the clock in a Go program scoped to
some goroutines under test.