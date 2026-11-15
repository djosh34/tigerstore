package loop

import (
	"testing"
	"time"
)

// timedFlusher is a flusher that only has a time.
type timedFlusher struct{ flushed int }

func (f *timedFlusher) Flush() { f.flushed++ }

// TestCancelDropsTimes cancels one of three flushers' times: the other two
// keep theirs, and the loop no longer refers to the first.
func TestCancelDropsTimes(t *testing.T) {
	l := New(NewChanPoller())
	a, b, c := &timedFlusher{}, &timedFlusher{}, &timedFlusher{}
	now := time.Now()
	l.After(a, now.Add(time.Hour))
	l.After(b, now.Add(2*time.Hour))
	l.After(c, now.Add(3*time.Hour))
	l.Cancel(a)
	l.Cancel(a)
	if len(l.timers) != 2 || len(l.timed) != 2 || l.timed[b] == l.timed[c] {
		t.Fatalf("after cancel: %d times, %d indexed", len(l.timers), len(l.timed))
	}
	for f, i := range l.timed {
		if l.timers[i].f != f {
			t.Fatalf("timed indexes %d for a flusher whose time is not there", i)
		}
	}
	if _, ok := l.timed[a]; ok {
		t.Fatal("the canceled flusher still has a time")
	}
}
