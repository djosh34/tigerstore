package loop

import (
	"context"
	"maps"
	"net"
	"slices"
	"sync"
	"time"
)

// Handler owns a socket on the loop.
type Handler interface {
	// Ready runs on the loop each time the socket's token is due: the
	// handler reads until the socket would block and writes what it holds.
	Ready()
}

// Flusher has output that the loop sends at the end of its turn.
type Flusher interface {
	// Flush sends what the flusher holds, as far as its socket takes it.
	Flush()
}

// Urgent is a handler that the loop runs even while its owner takes no
// input from sockets (Dispatch), such as a peer connection whose control
// messages must not wait for a slow disk. It holds back itself what the
// owner cannot take.
type Urgent interface {
	Urgent()
}

// Stopper is a handler that learns when the loop's owner stopped for
// good (Drain), so that it fails what it waits for.
type Stopper interface {
	Stopped()
}

// Loop dispatches a poller's tokens to the handlers of their sockets, runs
// the work other goroutines post to it, and sends the output its handlers
// queued during a turn. Its owner, a node's committer, calls Wait,
// Dispatch and Flush in turn on one goroutine, the loop; Attach, Forget and
// Dirty run there too. Post and Wake may be called from any goroutine.
type Loop struct {
	p        Poller
	next     uint64
	handlers map[uint64]Handler
	// due are the tokens the last Wait returned, held those that came
	// while the owner took no input from sockets (Dispatch), and again
	// those whose handlers asked to run at the next turn (Again).
	due, held, again []uint64
	mu               sync.Mutex
	posted           []func()
	running          []func()
	dirty            []Flusher
	isDirty          map[Flusher]bool
	// stopped is set once the loop ended for good (Drain), under mu.
	stopped bool
	// timers are flushers to flush again at a time (After), at most one
	// per flusher, which timed indexes.
	timers []timer
	timed  map[Flusher]int
}

// timer is a flusher to flush at or after a time.
type timer struct {
	at time.Time
	f  Flusher
}

// New returns a loop over p.
func New(p Poller) *Loop {
	return &Loop{p: p, handlers: map[uint64]Handler{}, isDirty: map[Flusher]bool{}, timed: map[Flusher]int{}}
}

// Attach hands c to the loop with h as its handler, whose Ready runs once
// right away, and returns the socket and its token.
func (l *Loop) Attach(c net.Conn, h Handler) (Socket, uint64, error) {
	l.next++
	token := l.next
	s, err := l.p.Attach(c, token)
	if err != nil {
		return nil, 0, err
	}
	l.handlers[token] = h
	l.due = append(l.due, token)
	return s, token, nil
}

// Forget drops the handler of a socket that was closed.
func (l *Loop) Forget(token uint64) { delete(l.handlers, token) }

// Post runs f on the loop at its next turn, and wakes it. It reports
// false, and drops f, once the loop stopped.
func (l *Loop) Post(f func()) bool {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return false
	}
	l.posted = append(l.posted, f)
	l.mu.Unlock()
	l.p.Wake()
	return true
}

// Wake makes the loop take a turn soon.
func (l *Loop) Wake() { l.p.Wake() }

// Wait waits for due sockets, posted work, a Wake or the timeout; a
// negative timeout waits without a limit.
func (l *Loop) Wait(timeout time.Duration) error {
	l.mu.Lock()
	if len(l.posted) > 0 {
		timeout = 0
	}
	l.mu.Unlock()
	var err error
	l.due, err = l.p.Wait(l.due, timeout)
	return err
}

// Dispatch runs the posted work, then the handlers of the due sockets if
// sockets is set. If it is not, as while the owner can take no more input,
// the due tokens wait for a later Dispatch that takes them (Holding), and
// their handlers that are Flushers are flushed, so output that waited for
// a socket still goes out while its input waits.
func (l *Loop) Dispatch(sockets bool) {
	l.expire()
	l.mu.Lock()
	l.running, l.posted = l.posted, l.running[:0]
	l.mu.Unlock()
	for i, f := range l.running {
		f()
		l.running[i] = nil
	}
	l.due = append(l.due, l.again...)
	l.again = l.again[:0]
	if !sockets {
		for _, token := range l.due {
			switch h := l.handlers[token].(type) {
			case Urgent:
				h.(Handler).Ready()
				continue
			case Flusher:
				l.Dirty(h)
			}
			l.held = append(l.held, token)
		}
		l.due = l.due[:0]
		return
	}
	if len(l.held) > 0 {
		// The two lists swap their arrays, so neither shares the other's.
		l.due, l.held = append(l.held, l.due...), l.due[:0]
	}
	// A handler may attach a socket, whose token joins l.due meanwhile.
	for i := 0; i < len(l.due); i++ {
		if h, ok := l.handlers[l.due[i]]; ok {
			h.Ready()
		}
	}
	l.due = l.due[:0]
}

// Again runs the handler of token at the loop's next turn, as a socket
// that stopped reading while its owner was busy asks.
func (l *Loop) Again(token uint64) {
	l.again = append(l.again, token)
}

// Holding reports whether due tokens wait for a Dispatch that takes
// sockets, which then needs no wait first.
func (l *Loop) Holding() bool { return len(l.held) > 0 || len(l.again) > 0 }

// Dirty lists f to flush at the end of the turn.
func (l *Loop) Dirty(f Flusher) {
	if !l.isDirty[f] {
		l.isDirty[f] = true
		l.dirty = append(l.dirty, f)
	}
}

// After lists f to flush at or after at, as a socket whose output waits
// checks its write deadline. A flusher has one time at most: the earliest
// it asked for.
func (l *Loop) After(f Flusher, at time.Time) {
	if i, ok := l.timed[f]; ok {
		if at.Before(l.timers[i].at) {
			l.timers[i].at = at
		}
		return
	}
	l.timed[f] = len(l.timers)
	l.timers = append(l.timers, timer{at: at, f: f})
}

// Timeout bounds timeout, the owner's own, by the earliest After; a
// negative timeout means none.
func (l *Loop) Timeout(timeout time.Duration) time.Duration {
	if len(l.timers) == 0 {
		return timeout
	}
	at := l.timers[0].at
	for _, t := range l.timers[1:] {
		if t.at.Before(at) {
			at = t.at
		}
	}
	d := max(time.Until(at), 0)
	if timeout < 0 {
		return d
	}
	return min(timeout, d)
}

// expire lists the flushers whose time came.
func (l *Loop) expire() {
	if len(l.timers) == 0 {
		return
	}
	now := time.Now()
	keep := l.timers[:0]
	for _, t := range l.timers {
		if now.Before(t.at) {
			l.timed[t.f] = len(keep)
			keep = append(keep, t)
			continue
		}
		delete(l.timed, t.f)
		l.Dirty(t.f)
	}
	clear(l.timers[len(keep):])
	l.timers = keep
}

// Flush flushes every flusher listed this turn.
func (l *Loop) Flush() {
	// A flush may list another flusher, which this pass takes too.
	for i := 0; i < len(l.dirty); i++ {
		f := l.dirty[i]
		delete(l.isDirty, f)
		f.Flush()
		l.dirty[i] = nil
	}
	l.dirty = l.dirty[:0]
}

// Drain serves the loop's sockets on their own once the owner stopped for
// good, until no handler is left, and then closes the poller: every
// handler that is a Stopper learns that the owner stopped, and the sockets
// go on until their owners close them, as a server answers calls that the node stopped and closes its
// connections when it stops itself. Post drops what comes after the last
// handler went. The owner calls it on the loop, as its last act there.
func (l *Loop) Drain() {
	l.Dispatch(false)
	// In the order of their tokens, so a simulated run replays.
	for _, token := range slices.Sorted(maps.Keys(l.handlers)) {
		if s, ok := l.handlers[token].(Stopper); ok {
			s.Stopped()
		}
	}
	l.Flush()
	for {
		l.mu.Lock()
		if len(l.handlers) == 0 && len(l.posted) == 0 {
			l.stopped = true
			l.mu.Unlock()
			_ = l.p.Close()
			return
		}
		l.mu.Unlock()
		if err := l.Wait(l.Timeout(-1)); err != nil {
			return
		}
		l.Dispatch(true)
		l.Flush()
	}
}

// Run runs the loop on its own until ctx ends, for an owner that has no
// turns of its own, such as a test of the peer transport without a node.
func (l *Loop) Run(ctx context.Context) error {
	stop := context.AfterFunc(ctx, l.Wake)
	defer stop()
	for {
		if err := l.Wait(l.Timeout(-1)); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		l.Dispatch(true)
		l.Flush()
	}
}

// Close closes the poller.
func (l *Loop) Close() error { return l.p.Close() }
