// Package diaglog is a diagnosis-only prototype for #680. It trades log
// completeness for a bounded, nonblocking path from the event loop to a
// possibly stalled diagnostic sink. It is not a production logging policy.
package diaglog

import (
	"fmt"
	"sync/atomic"
)

type Queue struct {
	lines   chan string
	done    chan struct{}
	Dropped atomic.Uint64
}

func New(write func(string), capacity int) *Queue {
	q := &Queue{lines: make(chan string, capacity), done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for line := range q.lines {
			write(line)
		}
	}()
	return q
}

func (q *Queue) Logf(format string, args ...any) {
	// Format now: callers may reuse the objects passed in args next turn.
	line := fmt.Sprintf(format, args...)
	select {
	case q.lines <- line:
	default:
		q.Dropped.Add(1)
	}
}

// Close requires stopped producers and a sink that eventually returns.
func (q *Queue) Close() { close(q.lines); <-q.done }
