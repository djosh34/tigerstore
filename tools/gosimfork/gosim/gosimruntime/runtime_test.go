package gosimruntime

import (
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestIsSim(t *testing.T) {
	if IsSim() {
		t.Error("should not be sim")
	}
}

func dump() string {
	var buf [32 * 1024]byte
	return string(buf[:runtime.Stack(buf[:], true)])
}

const baseNumGoroutines = 2

func expectNumGoroutines(t *testing.T, situation string) {
	t.Helper()

	// XXX: give any pre-existing goroutines time to fully exit
	start := time.Now()
	for runtime.NumGoroutine() != baseNumGoroutines {
		if time.Since(start) >= 1*time.Second {
			t.Errorf("too many goroutines at %s, expected %d", situation, baseNumGoroutines)
			t.Log(dump())
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func init() {
	SetSyscallAllocator(func(goroutineId int) unsafe.Pointer {
		return nil
	})
	initializeRuntime(func(f func()) {
		f()
		SetAbortError(ErrMainReturned)
	})
}

// TODO: Rewrite to use another API? Could add a call to dump goroutines to metatesting...?
func TestCleanupGoroutines(t *testing.T) {
	expectNumGoroutines(t, "start")

	var hit atomic.Int32
	var deferred atomic.Int32
	f := func() {
		waiting := NewChan[struct{}](0)
		pause := NewChan[struct{}](0)
		for i := 0; i < 5; i++ {
			Go(func() {
				defer func() {
					deferred.Add(1)
				}()

				waiting.Recv() // will not block
				hit.Add(1)
				pause.Recv() // should block
			})
			waiting.Send(struct{}{})
		}
	}
	run(f, 1, false, false, "INFO", makeConsoleLogger(os.Stderr), nil)

	expectNumGoroutines(t, "end")
	if hit.Load() != 5 {
		t.Error("recv not hit")
	}
	if deferred.Load() != 0 {
		t.Error("deferred hit")
	}
}
