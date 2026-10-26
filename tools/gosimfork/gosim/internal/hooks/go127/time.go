package go127

import (
	"time"
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/race"
)

func Time_now() (sec int64, nsec int32, mono int64) {
	now := gosimruntime.Nanotime()

	// TODO: somehow avoid this division?
	sec = now / 1e9
	mono = now
	nsec = int32(mono - sec*1e9)

	return
}

func Time_runtimeNow() (sec int64, nsec int32, mono int64) {
	return Time_now()
}

// gosim has no synctest bubbles.
func Time_runtimeIsBubbled() bool {
	return false
}

func Time_runtimeNano() int64 {
	return gosimruntime.Nanotime()
}

func Time_Sleep(duration time.Duration) {
	gosimruntime.Sleep(int64(duration))
}

//go:norace
func fireRuntimeTimer(t *gosimruntime.Timer) {
	rt := t.Arg.(*timeTimer)
	machine := t.Machine

	f := rt.f
	arg := rt.arg
	seq := rt.seq

	// TODO: only Go here in race mode; otherwise (always?) try to reuse some
	// outer (per-machine?) goroutine
	gosimruntime.GoFromTimerHandler(func() {
		// TODO: in mean mode, insert delay here?
		if race.Enabled {
			race.Acquire(unsafe.Pointer(rt))
		}
		delta := int64(0) // TODO: adjust?
		f(arg, seq, delta)
		// FIXME: this might be borked; standard library expects to run
		// atomically instead of in a goroutine?
	}, machine)

	if rt.period != 0 {
		// FIXME: this might be borked; base off now?
		rt.when += rt.period
		t.Reset(rt.when)
	}
}

type timeTimer struct {
	c    unsafe.Pointer
	init bool
	timer
}

type timer struct {
	pp     *gosimruntime.Timer // repurposed pp
	when   int64
	period int64
	f      func(arg any, seq uintptr, delta int64) // NOTE: must not be closure
	arg    any                                     // passed to f
	seq    uintptr                                 // passed to f
}

//go:norace
func Time_newTimer(when, period int64, f func(any, uintptr, int64), arg any, cp unsafe.Pointer) *timeTimer {
	t := &timeTimer{
		init: true, // huh
	}

	if race.Enabled {
		race.Release(unsafe.Pointer(t))
	}

	// TODO: use cp to do async chan stuff?

	t.when = when
	t.period = period
	t.f = f
	t.arg = arg
	t.seq = 0

	t.pp = gosimruntime.NewTimer(fireRuntimeTimer, t, gosimruntime.CurrentMachine(), t.when)

	return t
}

//go:norace
func Time_stopTimer(t *timeTimer) bool {
	return t.pp.Stop()
}

//go:norace
func Time_resetTimer(t *timeTimer, when, period int64) bool {
	if race.Enabled {
		race.Release(unsafe.Pointer(t))
	}

	t.when = when
	t.period = period

	return t.pp.Reset(when)
}
