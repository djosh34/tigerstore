package gosimruntime

import (
	"time"
)

// TODO: move clock to machine so we can simulate broken clocks and offsets

// race detector:
//   there is an edge from timer start/reset to func firing, or channel being
//   written

// TODO:
// - come up with some rules as to what thread code runs on (make a special time goroutine?)
// - come up with some rules as to when/if timer should ever yield
// - figure out happens-before rules
// - write some tests that assert all this

//go:norace
func Nanotime() int64 {
	return gs.get().clock.now
}

type clock struct {
	now  int64 // unix nsec
	heap *timerHeap
}

func newClock() *clock {
	return &clock{
		// TODO: make start timestamp random also
		now:  time.Date(2020, 1, 15, 14, 10, 3, 1234, time.UTC).UnixNano(),
		heap: newTimerHeap(),
	}
}

//go:norace
func (c *clock) anyWaiting() bool {
	return c.heap.len() > 0
}

//go:norace
func (c *clock) maybefire() (*Timer, bool) {
	if c.heap.len() == 0 {
		return nil, false
	}
	if c.heap.peek().when <= c.now {
		e := c.heap.pop()
		return e, true
	}
	return nil, false
}

//go:norace
func (c *clock) doadvance() {
	e := c.heap.peek()
	t := e.when
	if t <= c.now {
		panic("bad")
	}
	// XXX: we allow adding at now/earlier to handle instant timers
	c.now = t
	if checksummer := gs.get().checksummer; checksummer != nil {
		checksummer.recordIntInt(checksumKeyTimeNow, uint64(c.now), 0)
	}
}

func (c *clock) removemachine(m *Machine) {
	c.heap.removemachine(m)
}

func unpause(t *Timer) {
	if getg() != nil {
		panic("expect to run without a goroutine")
	}
	t.Arg.(*goroutine).setWaiting(false)
}

//go:norace
func Sleep(duration int64) {
	start := gs.get().clock.now
	if duration <= 0 {
		return
	}
	end := start + duration

	// no sudog??? cool.
	// XXX: use a sudog so we can have every wait paired with a sudog and some
	// sensible checking for that?
	g := getg()

	// XXX: reuse this timer?
	NewTimer(unpause, g, g.machine, end)

	g.wait()
}

type Timer struct {
	when    int64
	handler func(t *Timer)
	Arg     any
	Machine *Machine

	pos int
}

// TODO: reconsider if the timer needs to be go:norace. Maybe do a pivot to
// runtime stack here also?

//go:norace
func NewTimer(handler func(arg *Timer), arg any, machine *Machine, when int64) *Timer {
	t := &Timer{
		when:    when,
		handler: handler,
		Arg:     arg,
		Machine: machine,
		pos:     -1,
	}

	gs.get().clock.heap.add(t)

	return t
}

//go:norace
func (t *Timer) Reset(when int64) bool {
	stopped := t.pos != -1
	if stopped {
		gs.get().clock.heap.adjust(t, when)
	} else {
		t.when = when
		gs.get().clock.heap.add(t)
	}
	return stopped
}

//go:norace
func (t *Timer) Stop() bool {
	stopped := t.pos != -1
	if stopped {
		gs.get().clock.heap.remove(t)
	}
	return stopped
}
