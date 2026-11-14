// Package logq logs through a queue, so that a caller never waits for the
// log's output. The node's event loop logs as it works: slow writes to
// the log, slow calls, a leader change. A process's log goes to a file or
// a pipe, which may sit on the disk the node writes its data to. While
// that disk stalls, a write to the log can take seconds, and a loop that
// waits for it sends no heartbeats and loses its term (#680).
//
// A Log is a zap core in front of another: Logger's lines are checked,
// stamped and given their caller at the call, as zap does, then queued;
// one goroutine writes the queue to the other core, in order. When the
// queue is full a line is dropped and counted, and the next line queued
// carries the count, so the report of the drops is written where they
// were.
package logq

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Size is how many lines a Log holds by default while its output is
// slow: about 3 MiB of queue.
const Size = 1 << 14

// stopWait bounds how long Stop waits for the lines still queued to be
// written.
const stopWait = 5 * time.Second

// line is one queued line: the core it goes to, with its fields, and how
// many lines were dropped just before it.
type line struct {
	core    zapcore.Core
	entry   zapcore.Entry
	fields  []zapcore.Field
	dropped uint64
}

// Log queues the lines of a zap logger for its output.
type Log struct {
	base    zapcore.Core
	lines   chan line
	dropped atomic.Uint64
	// mu orders the lines queued with Stop: stopped is set under its
	// write lock, and a line is queued under its read lock, so that none
	// is queued once the writer may have ended.
	mu      sync.RWMutex
	stopped bool
	stop    sync.Once
	quit    chan struct{}
	done    chan struct{}
}

// New returns a Log in front of lg's core with a queue of size lines, and
// starts its writer.
func New(lg *zap.Logger, size int) *Log {
	l := &Log{base: lg.Core(), lines: make(chan line, size), quit: make(chan struct{}), done: make(chan struct{})}
	go l.write()
	return l
}

// Logger returns lg with its lines going through the queue.
func (l *Log) Logger(lg *zap.Logger) *zap.Logger {
	return lg.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core { return queued{l: l, next: c} }))
}

// queued is the core of Logger: it queues what next would write.
type queued struct {
	l    *Log
	next zapcore.Core
}

func (q queued) Enabled(lvl zapcore.Level) bool { return q.next.Enabled(lvl) }

func (q queued) With(fields []zapcore.Field) zapcore.Core {
	return queued{l: q.l, next: q.next.With(fields)}
}

// Check asks next whether it takes e, as its sampling of repeated lines
// decides, and queues e for it if so.
func (q queued) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if q.next.Check(e, nil) != nil {
		return ce.AddCore(e, q)
	}
	return ce
}

// Write queues e without waiting for the output. Its fields are encoded
// when the writer writes it, so a caller must not pass values it changes
// after the call. A line that does not fit
// the queue, or comes after Stop, is dropped and counted. A line that
// ends the process, at DPanic or above, is written at once, since nothing
// would write it after.
func (q queued) Write(e zapcore.Entry, fields []zapcore.Field) error {
	if e.Level > zapcore.ErrorLevel {
		return q.next.Write(e, fields)
	}
	q.l.add(line{core: q.next, entry: e, fields: fields})
	return nil
}

// Sync syncs the output. The lines still queued are written by the
// writer, or by Stop.
func (q queued) Sync() error { return q.next.Sync() }

// add queues ln, with the count of the lines dropped before it. The
// count is taken under the read lock, once the line may be queued: a
// count taken across Stop would be neither queued nor left for Stop's
// report.
func (l *Log) add(ln line) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.stopped {
		ln.dropped = l.dropped.Swap(0)
		select {
		case l.lines <- ln:
			return
		default:
		}
	}
	l.dropped.Add(ln.dropped + 1)
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
	l.reportDropped(ln.entry.Time, ln.dropped)
	report(ln.core.Write(ln.entry, ln.fields))
}

// reportDropped writes that n lines were dropped, if any were.
func (l *Log) reportDropped(at time.Time, n uint64) {
	if n > 0 {
		e := zapcore.Entry{Level: zapcore.WarnLevel, Time: at, Message: fmt.Sprintf("dropped %d log lines while the log's output was slow", n)}
		report(l.base.Write(e, nil))
	}
}

// report reports a failed write on stderr, as zap's logger does.
func report(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v write error: %v\n", time.Now(), err)
	}
}

// Stop takes no more lines, and waits until those queued are written, for
// at most a few seconds. A writer still stuck after them keeps waiting on
// the output, and so does any other write to it.
func (l *Log) Stop() {
	l.stop.Do(func() {
		l.mu.Lock()
		l.stopped = true
		l.mu.Unlock()
		close(l.quit)
	})
	select {
	case <-l.done:
	case <-time.After(stopWait):
	}
}
