//go:build linkname

package coro

import (
	"unsafe"
	_ "unsafe"

	"github.com/jellevandenhooff/gosim/internal/race"
)

type coro struct{}

//go:linkname newcoro runtime.newcoro
func newcoro(func(*coro)) *coro

//go:linkname coroswitch runtime.coroswitch
func coroswitch(*coro)

//go:linkname coroexit runtime.coroexit
func coroexit(*coro)

// A Coro is a coroutine: A goroutine with explicit and cheap context switching.
// Coro uses go's runtime coroutine implementation (which otherwise powers
// iter.Pull) using linkname to get low overhead and direct access to coroexit.
type Coro struct {
	coro *coro
}

// Start starts the coroutine, running f in a new coroutine. It should be called only
// once on a Coro. It lets the coroutine run until the first call to Yield or the final
// call to Finish.
//
//go:norace
func (c *Coro) Start(f func()) {
	c.coro = newcoro(func(*coro) {
		race.Acquire(unsafe.Pointer(c))
		f()
		panic("wtf")
	})
	race.Release(unsafe.Pointer(c))
	coroswitch(c.coro)
	race.Acquire(unsafe.Pointer(c))
}

// Next must be called from outside the coroutine. It lets the coroutine run
// until the next call to Yield or the final call to Finish.
//
//go:norace
func (c *Coro) Next() {
	race.Release(unsafe.Pointer(c))
	coroswitch(c.coro)
	race.Acquire(unsafe.Pointer(c))
}

// Yield be called form inside the coroutine. It pauses the coroutine, yielding to
// the caller of Next (or Start).
//
//go:norace
func (c *Coro) Yield() {
	race.Release(unsafe.Pointer(c))
	coroswitch(c.coro)
	race.Acquire(unsafe.Pointer(c))
}

// Finish must be called from inside the coroutine. It stops the coroutine,
// yielding to the caller of Next (or Start). Finish terminates the coroutine,
// any defers in the callstack will _not_ be run.
//
//go:norace
func (c *Coro) Finish() {
	race.Release(unsafe.Pointer(c))
	coroexit(c.coro)
	panic("wtf")
}
