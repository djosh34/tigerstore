// Package logq logs through a queue, so that a caller never waits for the
// log's output. The node's event loop logs as it works: slow writes to
// the log, slow calls, a leader change. A process's log goes to a file or
// a pipe, which may sit on the disk the node writes its data to. While
// that disk stalls, a write to the log can take seconds, and a loop that
// waits for it sends no heartbeats and loses its term (#680).
//
// Infof formats the line and stamps it with the time and place of the
// call, then queues it; one goroutine writes the queue out, in order.
// When the queue is full the line is dropped and counted, and the next
// line queued carries the count, so the report of the drops is written
// where they were.
package logq

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Size is how many lines a Log holds by default while its output is
// slow.
const Size = 1 << 14

// stopWait bounds how long Stop waits for the lines still queued to be
// written.
const stopWait = 5 * time.Second

// line is one queued line: when and where it was logged, its message, and
// how many lines were dropped just before it.
type line struct {
	at      time.Time
	pc      uintptr
	msg     string
	dropped uint64
}

// Log queues lines for a zap logger's output.
type Log struct {
	core    zapcore.Core
	lines   chan line
	dropped atomic.Uint64
	// quit tells the writer to write what is queued and end, and done
	// closes when it has.
	stop sync.Once
	quit chan struct{}
	done chan struct{}
}

// New returns a Log that writes to lg's output through a queue of size
// lines, and starts its writer.
func New(lg *zap.Logger, size int) *Log {
	l := &Log{core: lg.Core(), lines: make(chan line, size), quit: make(chan struct{}), done: make(chan struct{})}
	go l.write()
	return l
}

// Infof logs a line at the info level without waiting for the output. A
// line that does not fit the queue is dropped and counted. After Stop no
// line is written.
func (l *Log) Infof(format string, args ...any) {
	if !l.core.Enabled(zapcore.InfoLevel) {
		return
	}
	var pc [1]uintptr
	runtime.Callers(2, pc[:])
	ln := line{at: time.Now(), pc: pc[0], msg: fmt.Sprintf(format, args...), dropped: l.dropped.Swap(0)}
	select {
	case l.lines <- ln:
	default:
		l.dropped.Add(ln.dropped + 1)
	}
}

// write writes the queued lines in order until Stop, then what is left in
// the queue, and the lines dropped after the last.
func (l *Log) write() {
	defer close(l.done)
	for {
		select {
		case ln := <-l.lines:
			l.entry(ln)
		case <-l.quit:
			for {
				select {
				case ln := <-l.lines:
					l.entry(ln)
				default:
					l.reportDropped(time.Now(), l.dropped.Swap(0))
					return
				}
			}
		}
	}
}

// entry writes ln, after the report of the lines dropped before it.
func (l *Log) entry(ln line) {
	l.reportDropped(ln.at, ln.dropped)
	e := zapcore.Entry{Level: zapcore.InfoLevel, Time: ln.at, Message: ln.msg}
	if f, _ := runtime.CallersFrames([]uintptr{ln.pc}).Next(); f.PC != 0 {
		e.Caller = zapcore.NewEntryCaller(f.PC, f.File, f.Line, true)
	}
	l.check(e)
}

// reportDropped writes that n lines were dropped, if any were.
func (l *Log) reportDropped(at time.Time, n uint64) {
	if n > 0 {
		l.check(zapcore.Entry{Level: zapcore.WarnLevel, Time: at, Message: fmt.Sprintf("dropped %d log lines while the log's output was slow", n)})
	}
}

// check writes e, and reports a failed write on stderr, as zap's logger
// does.
func (l *Log) check(e zapcore.Entry) {
	if ce := l.core.Check(e, nil); ce != nil {
		ce.ErrorOutput = zapcore.Lock(os.Stderr)
		ce.Write()
	}
}

// Stop takes no more lines, and waits until those queued are written, for
// at most a few seconds. A writer still stuck after them keeps waiting on
// the output, and so does any other write to it.
func (l *Log) Stop() {
	l.stop.Do(func() { close(l.quit) })
	select {
	case <-l.done:
	case <-time.After(stopWait):
	}
}
