//go:build race

package coro

// Upcallcoro is a wrapper around a Coro that allows executing "upcalls":
// functions calls from inside the coroutine that execute on the stack outside
// the coroutine.
//
// Upcalls are used by the gosim runtime to have a single outside goroutine
// modify scheduler (and other) state without triggering the race detector.
// Whenever user code, for example, spawns a new goroutine which modifies
// shared scheduler state, it does that with Upcall.
//
// In non-race instrumented builds Upcall can invoke the function on the
// stack inside the coroutine.
type Upcallcoro struct {
	coro Coro

	// upcall is set to non-nil whenever an upcall is made
	upcall func()
}

//go:norace
func (u *Upcallcoro) Start(f func()) {
	u.coro.Start(f)
	for u.upcall != nil {
		u.upcall()
		u.upcall = nil
		u.coro.Next()
	}
}

func (u *Upcallcoro) Next() {
	for {
		u.coro.Next()
		if u.upcall == nil {
			break
		}
		u.upcall()
		u.upcall = nil
	}
}

// Upcall must be called from inside the coroutine, and invokes f on the stack
// that called Start or Next.
//
//go:norace
func (u *Upcallcoro) Upcall(f func()) {
	// norace to allow writing upcall

	// TODO: It might be more efficient to hook into the go runtime's race
	// detector integration and manually switch race detector contexts instead
	// of switching stacks entirely.
	u.upcall = f
	u.coro.Yield()
}

func (u *Upcallcoro) Yield() {
	u.coro.Yield()
}

func (u *Upcallcoro) Finish() {
	u.coro.Finish()
}
