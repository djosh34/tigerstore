// Package logq logs through a queue, so that a caller never waits for the
// log's output. The node's event loop logs as it works: slow writes to
// the log, slow calls, a leader change. A process's log goes to a file or
// a pipe, which may sit on the disk the node writes its data to. While
// that disk stalls, a write to the log can take seconds, and a loop that
// waits for it sends no heartbeats and loses its term (#680).
//
// Infof formats the line and stamps it with the time of the call, then
// queues it; one goroutine writes the queue out, in order. When the queue
// is full the line is dropped and counted, and the next line written says
// how many were.
package logq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Size is how many lines a Log holds by default while its output is
// slow: a few MiB of lines at most.
const Size = 1 << 14

// stopWait bounds how long Stop waits for the lines still queued to be
// written: an output that stays stuck must not keep the process from
// ending.
const stopWait = 5 * time.Second

// Log queues lines for a zap logger's output.
type Log struct {
	core    zapcore.Core
	lines   chan zapcore.Entry
	dropped atomic.Uint64
	// stopped is set by Stop, after which lines are dropped; quit tells
	// the writer to write what is queued and end, and done closes when it
	// has.
	stopped atomic.Bool
	stop    sync.Once
	quit    chan struct{}
	done    chan struct{}
}

// New returns a Log that writes to lg's output through a queue of size
// lines, and starts its writer.
func New(lg *zap.Logger, size int) *Log {
	l := &Log{core: lg.Core(), lines: make(chan zapcore.Entry, size), quit: make(chan struct{}), done: make(chan struct{})}
	go l.write()
	return l
}

// Infof logs a line at the info level without waiting for the output. A
// line that does not fit the queue is dropped and counted, and so is
// every line after Stop.
func (l *Log) Infof(format string, args ...any) {
	if !l.core.Enabled(zapcore.InfoLevel) {
		return
	}
	if l.stopped.Load() {
		l.dropped.Add(1)
		return
	}
	e := zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Now(), Message: fmt.Sprintf(format, args...)}
	select {
	case l.lines <- e:
	default:
		l.dropped.Add(1)
	}
}

// write writes the queued lines in order, each after a report of the
// lines dropped before it, until Stop; then what is left in the queue.
func (l *Log) write() {
	defer close(l.done)
	for {
		select {
		case e := <-l.lines:
			l.entry(e)
		case <-l.quit:
			for {
				select {
				case e := <-l.lines:
					l.entry(e)
				default:
					l.reportDropped(time.Now())
					return
				}
			}
		}
	}
}

// entry writes e, after a report of the lines dropped before it.
func (l *Log) entry(e zapcore.Entry) {
	l.reportDropped(e.Time)
	if ce := l.core.Check(e, nil); ce != nil {
		ce.Write()
	}
}

// reportDropped writes how many lines were dropped since the last report,
// if any were.
func (l *Log) reportDropped(at time.Time) {
	n := l.dropped.Swap(0)
	if n == 0 {
		return
	}
	e := zapcore.Entry{Level: zapcore.WarnLevel, Time: at, Message: fmt.Sprintf("dropped %d log lines while the log's output was slow", n)}
	if ce := l.core.Check(e, nil); ce != nil {
		ce.Write()
	}
}

// Stop takes no more lines, and waits until those queued are written, for
// at most a few seconds.
func (l *Log) Stop() {
	l.stop.Do(func() {
		l.stopped.Store(true)
		close(l.quit)
	})
	select {
	case <-l.done:
	case <-time.After(stopWait):
	}
}
