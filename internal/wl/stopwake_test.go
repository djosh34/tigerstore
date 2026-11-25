package wl

import (
	"context"
	"encoding/binary"
	"testing"

	"golang.org/x/sys/unix"
)

// TestACancelRightAfterTheLoopLookedWakesItAgain is #828: a loop's context
// is cancelled right after the loop looked for a stop on a post's wake,
// and the cancel's wake comes then too. The loop read its wake after it
// looked, which took the cancel's with the post's: nothing woke it to look
// again, and Run never returned. Its wake must still be there for the next
// turn. The loop here is one eventfd and a context, with no goroutine of
// its own that writes a wake: the test writes each, in that order.
func TestACancelRightAfterTheLoopLookedWakesItAgain(t *testing.T) {
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	wake := func() {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 1)
		if _, err := unix.Write(fd, b[:]); err != nil {
			t.Error(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := &loop{ctx: ctx, hoFD: fd}
	wake() // a post's
	hook := func(*loop) {
		cancel()
		wake() // the cancel's, as run's goroutine writes it once ctx is done
	}
	testAfterStopCheck.Store(&hook)
	t.Cleanup(func() { testAfterStopCheck.Store(nil) })
	if l.wakeStops() {
		t.Fatal("the loop stopped before its context was cancelled")
	}
	var b [8]byte
	if n, err := unix.Read(fd, b[:]); err != nil || n != 8 {
		t.Fatalf("the cancel's wake was read with the post's (read %d, %v): nothing wakes the loop to look for its stop again", n, err)
	}
}
