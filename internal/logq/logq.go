// Package logq logs lines through a queue, so that a caller never waits
// for the log's output. The node's event loop logs as it works: slow
// writes to the log, slow calls, a leader change, and with
// GODEBUG=http2debug=2 x/net's control frames through the standard log
// package, whose lines it queues at the warn level. A process's log goes to a file or a pipe, which may sit on the
// disk the node writes its data to. While that disk stalls, a write to the
// log can take seconds, and a loop that waits for it sends no heartbeats
// and loses its term (#680).
//
// A line is formatted and stamped with the time of the call, then queued;
// one goroutine writes the queue to a zap core, in order, through the
// core's own Check, so its levels and sampling hold. When the queue is
// full a line is dropped and counted, and the next line queued carries the
// count, so the report of the drops is written where they were.
package logq

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
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

// line is one queued line: when and where it was logged, its level and
// message, and how many lines were dropped just before it.
type line struct {
	at      time.Time
	pc      uintptr
	level   zapcore.Level
	msg     string
	dropped uint64
}

// Log queues lines for a zap logger's output. It is also an io.Writer, for
// the standard log package (log.SetOutput): each write is one line.
type Log struct {
	core  zapcore.Core
	lines chan line
	// mu makes the count of dropped lines, the queueing of a line and
	// Stop one order: a line takes the count when it is queued, a line
	// that is not queued adds to it, and after stopped no line is queued.
	mu      sync.Mutex
	dropped uint64
	stopped bool
	stop    sync.Once
	// quit tells the writer to write what is queued and end, and done
	// closes when it has.
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

// Infof logs a line at the info level without waiting for the output, with
// its caller. A line that does not fit the queue, or comes after Stop, is
// dropped and counted.
func (l *Log) Infof(format string, args ...any) {
	if !l.core.Enabled(zapcore.InfoLevel) {
		return
	}
	var pc [1]uintptr
	runtime.Callers(2, pc[:])
	l.add(line{at: time.Now(), pc: pc[0], level: zapcore.InfoLevel, msg: fmt.Sprintf(format, args...)})
}

// Write logs p, a line of the standard log package, at the warn level
// without waiting for the output. Such lines are warnings, such as
// net/http's report of a handler that panicked, and went to stderr at any
// log level before.
func (l *Log) Write(p []byte) (int, error) {
	if l.core.Enabled(zapcore.WarnLevel) {
		l.add(line{at: time.Now(), level: zapcore.WarnLevel, msg: string(bytes.TrimSuffix(p, []byte("\n")))})
	}
	return len(p), nil
}

// add queues ln with the count of the lines dropped before it, or drops
// and counts it.
func (l *Log) add(ln line) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.stopped {
		ln.dropped = l.dropped
		select {
		case l.lines <- ln:
			l.dropped = 0
			return
		default:
		}
	}
	l.dropped++
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
					l.mu.Lock()
					n := l.dropped
					l.dropped = 0
					l.mu.Unlock()
					l.reportDropped(time.Now(), n)
					return
				}
			}
		}
	}
}

// entry writes ln, after the report of the lines dropped before it.
func (l *Log) entry(ln line) {
	l.reportDropped(ln.at, ln.dropped)
	e := zapcore.Entry{Level: ln.level, Time: ln.at, Message: ln.msg}
	if ln.pc != 0 {
		if f, _ := runtime.CallersFrames([]uintptr{ln.pc}).Next(); f.PC != 0 {
			e.Caller = zapcore.NewEntryCaller(f.PC, f.File, f.Line, true)
		}
	}
	l.check(e)
}

// reportDropped writes that n lines were dropped, if any were.
func (l *Log) reportDropped(at time.Time, n uint64) {
	if n > 0 {
		l.check(zapcore.Entry{Level: zapcore.WarnLevel, Time: at, Message: fmt.Sprintf("dropped %d log lines while the log's output was slow", n)})
	}
}

// check writes e where the core's Check sends it, and reports a failed
// write on stderr, as zap's logger does.
func (l *Log) check(e zapcore.Entry) {
	if ce := l.core.Check(e, nil); ce != nil {
		ce.ErrorOutput = zapcore.Lock(os.Stderr)
		ce.Write()
	}
}

// Stop takes no more lines, and waits until those queued are written, for
// at most a few seconds. A writer still stuck after them keeps waiting on
// the output.
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
