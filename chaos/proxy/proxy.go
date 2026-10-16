// Package proxy is a seeded TCP fault proxy for real-process chaos tests.
// Each link of a cluster (one client to one node, or one node to another)
// gets its own Proxy. A Proxy relays the bytes of every connection it
// accepts to one upstream address without terminating TLS, and applies
// the one Fault installed with Set until the next Set.
//
// Forward is the direction from the side that connected to the proxy to
// the upstream; Backward is the reply direction. A seed fixes the random
// choices of each connection and direction (which reads lose, gain or
// flip a byte, and where), keyed by connection number. It does not fix
// connection order or kernel timing, so a seed reproduces the policy, not
// the run.
package proxy

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Dir selects the directions a fault applies to.
type Dir uint8

const (
	// Forward is from the connecting side to the upstream.
	Forward Dir = 1 << iota
	// Backward is from the upstream to the connecting side.
	Backward
	// Both is Forward and Backward.
	Both = Forward | Backward
)

func (d Dir) String() string {
	switch d {
	case Forward:
		return "forward"
	case Backward:
		return "backward"
	case Both:
		return "both"
	}
	return fmt.Sprintf("dir(%d)", uint8(d))
}

// MarshalText makes Dir readable in JSON evidence.
func (d Dir) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// Kind names a fault.
type Kind string

// The faults. Each counts a hit for every read or connection it changes.
const (
	// None relays bytes unchanged.
	None Kind = "none"
	// Delay writes each read Delay after it was read. Order is kept, so a
	// read never overtakes an earlier one.
	Delay Kind = "delay"
	// Switching is Delay with a delay that switches between Delay and
	// Delay2 every Every.
	Switching Kind = "switching-delay"
	// Stall stops reading and writing. Bytes already read are held, the
	// kernel buffers fill, and the senders block. Nothing is lost: the
	// held bytes go out after the next Set.
	Stall Kind = "stall"
	// Drop deletes one byte of a read: of the first read after Set, then
	// of one read in four. TCP does not resend deleted stream bytes.
	Drop Kind = "drop"
	// Corrupt flips one bit of a read, chosen like Drop.
	Corrupt Kind = "corrupt"
	// Duplicate repeats one byte of a read, chosen like Drop.
	Duplicate Kind = "duplicate"
	// Cut discards every byte read, so the receiver hears nothing.
	Cut Kind = "cut"
	// Reset closes every open connection with a TCP RST when installed,
	// and every new connection while installed. It ignores Dir.
	Reset Kind = "reset"
	// Rate limits writes to Rate bytes per second with no idle credit.
	Rate Kind = "rate"
)

// Kinds lists every fault but None.
var Kinds = []Kind{Delay, Switching, Stall, Drop, Corrupt, Duplicate, Cut, Reset, Rate}

// Fault is the fault a proxy applies.
type Fault struct {
	Kind   Kind          `json:"kind"`
	Dir    Dir           `json:"dir,omitempty"`
	Delay  time.Duration `json:"delay,omitempty"`
	Delay2 time.Duration `json:"delay2,omitempty"`
	Every  time.Duration `json:"every,omitempty"`
	Rate   int           `json:"rate,omitempty"`
}

// Validate reports a fault that the proxy cannot apply as asked.
func (f Fault) Validate() error {
	switch f.Kind {
	case None:
		return nil
	case Reset:
	case Delay:
		if f.Delay <= 0 {
			return errors.New("delay needs a positive Delay")
		}
	case Switching:
		if f.Delay < 0 || f.Delay2 <= 0 || f.Every <= 0 || f.Delay == f.Delay2 {
			return errors.New("switching delay needs two different delays and a positive Every")
		}
	case Rate:
		if f.Rate <= 0 {
			return errors.New("rate needs a positive Rate")
		}
	case Stall, Drop, Corrupt, Duplicate, Cut:
	default:
		return fmt.Errorf("unknown fault %q", f.Kind)
	}
	if f.Kind != Reset && (f.Dir == 0 || f.Dir&^Both != 0) {
		return fmt.Errorf("fault %s needs a direction", f.Kind)
	}
	return nil
}

func (f Fault) applies(d Dir) bool { return f.Kind != None && f.Dir&d != 0 }

// Stats is the state of a proxy.
type Stats struct {
	// Generation counts calls to Set.
	Generation uint64 `json:"generation"`
	// Fault is the installed fault.
	Fault Fault `json:"fault"`
	// Hits counts the reads and connections the installed fault changed.
	Hits uint64 `json:"hits"`
	// Conns counts accepted connections; Open is how many are open.
	Conns uint64 `json:"conns"`
	Open  int    `json:"open"`
	// Bytes counts bytes read in each direction.
	ForwardBytes  uint64 `json:"forward_bytes"`
	BackwardBytes uint64 `json:"backward_bytes"`
}

// Event is one fault action, kept for evidence.
type Event struct {
	At     time.Duration `json:"at"`
	Gen    uint64        `json:"gen"`
	Conn   uint64        `json:"conn"`
	Dir    Dir           `json:"dir,omitempty"`
	Action Kind          `json:"action"`
	Bytes  int           `json:"bytes,omitempty"`
	Offset uint64        `json:"offset,omitempty"`
}

// maxEvents bounds the kept events; Stats counts all of them.
const maxEvents = 4096

// state is one installed fault. changed closes when the next Set replaces it.
type state struct {
	fault   Fault
	gen     uint64
	changed chan struct{}
}

// Proxy relays connections to one upstream and applies one fault at a time.
type Proxy struct {
	ln       net.Listener
	upstream string
	seed     uint64
	start    time.Time
	cur      atomic.Pointer[state]
	hits     atomic.Uint64
	fwd, bwd atomic.Uint64

	mu      sync.Mutex
	pairs   map[*pair]bool
	conns   uint64
	events  []Event
	dropped uint64
	closing bool
	wg      sync.WaitGroup
}

type pair struct {
	id      uint64
	a, b    *net.TCPConn
	done    chan struct{}
	once    sync.Once
	damaged atomic.Bool
}

func (q *pair) close(reset bool) {
	q.once.Do(func() {
		if reset {
			_ = q.a.SetLinger(0)
			_ = q.b.SetLinger(0)
		}
		_ = q.a.Close()
		_ = q.b.Close()
		close(q.done)
	})
}

// Listen starts a proxy on addr that relays to upstream.
func Listen(addr, upstream string, seed uint64) (*Proxy, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, upstream: upstream, seed: seed, start: time.Now(), pairs: map[*pair]bool{}}
	p.cur.Store(&state{fault: Fault{Kind: None}, changed: make(chan struct{})})
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address the proxy listens on.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Set installs f in place of the current fault, resets the hit count,
// releases held bytes and closes with RST every connection that an
// earlier fault damaged (dropped, cut, corrupted or duplicated bytes), as
// its byte stream can not recover. It returns the installed state.
func (p *Proxy) Set(f Fault) (Stats, error) {
	if err := f.Validate(); err != nil {
		return Stats{}, err
	}
	p.mu.Lock()
	old := p.cur.Load()
	p.hits.Store(0)
	p.cur.Store(&state{fault: f, gen: old.gen + 1, changed: make(chan struct{})})
	close(old.changed)
	var reset []*pair
	for q := range p.pairs {
		if f.Kind == Reset || q.damaged.Load() {
			reset = append(reset, q)
		}
	}
	p.mu.Unlock()
	for _, q := range reset {
		if f.Kind == Reset {
			p.hit(old.gen+1, q.id, 0, Reset, 0, 0)
		}
		q.close(true)
	}
	return p.Stats(), nil
}

// Stats returns the current state.
func (p *Proxy) Stats() Stats {
	s := p.cur.Load()
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Generation: s.gen, Fault: s.fault, Hits: p.hits.Load(), Conns: p.conns, Open: len(p.pairs),
		ForwardBytes: p.fwd.Load(), BackwardBytes: p.bwd.Load(),
	}
}

// Events returns the kept fault actions and how many were not kept.
func (p *Proxy) Events() ([]Event, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Event(nil), p.events...), p.dropped
}

// Close stops the proxy and closes every connection.
func (p *Proxy) Close() error {
	err := p.ln.Close()
	p.mu.Lock()
	p.closing = true
	for q := range p.pairs {
		q.close(false)
	}
	p.mu.Unlock()
	p.wg.Wait()
	return err
}

func (p *Proxy) hit(gen, conn uint64, d Dir, action Kind, n int, off uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only a hit of the current fault counts as its evidence: a relay
	// that loaded the state before Set may report a hit of the old one
	// after the reset. Set changes the state under p.mu too.
	if gen == p.cur.Load().gen {
		p.hits.Add(1)
	}
	if len(p.events) >= maxEvents {
		p.dropped++
		return
	}
	p.events = append(p.events, Event{At: time.Since(p.start), Gen: gen, Conn: conn, Dir: d, Action: action, Bytes: n, Offset: off})
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		u, err := net.DialTimeout("tcp", p.upstream, time.Second)
		if err != nil {
			_ = c.Close()
			continue
		}
		q := &pair{a: c.(*net.TCPConn), b: u.(*net.TCPConn), done: make(chan struct{})}
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			q.close(false)
			return
		}
		p.conns++
		q.id = p.conns
		p.pairs[q] = true
		p.mu.Unlock()
		if s := p.cur.Load(); s.fault.Kind == Reset {
			p.hit(s.gen, q.id, 0, Reset, 0, 0)
			q.close(true)
		}
		p.wg.Go(func() {
			var w sync.WaitGroup
			w.Go(func() { p.relay(q, Forward, q.a, q.b, &p.fwd) })
			w.Go(func() { p.relay(q, Backward, q.b, q.a, &p.bwd) })
			w.Wait()
			q.close(false)
			p.mu.Lock()
			delete(p.pairs, q)
			p.mu.Unlock()
		})
	}
}

// chunk is one read on its way to the writer. A nil data ends the stream.
type chunk struct {
	data []byte
	due  time.Time
	eof  bool
}

// queueLen bounds the reads one direction holds, so a delayed or rate
// limited link pushes back on its sender instead of buffering without end.
const queueLen = 16

// relay copies src to dst in direction d. The reader applies byte faults
// and stalls; the writer applies delays and rate limits.
func (p *Proxy) relay(q *pair, d Dir, src, dst *net.TCPConn, count *atomic.Uint64) {
	ch := make(chan chunk, queueLen)
	go p.write(q, d, dst, ch)
	rng := rand.New(rand.NewPCG(p.seed, q.id<<1|uint64(d>>1)))
	buf := make([]byte, 32<<10)
	var off, lastGen uint64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			count.Add(uint64(n))
			data := append([]byte(nil), buf[:n]...)
			s := p.cur.Load()
			first := s.gen != lastGen
			lastGen = s.gen
			if s.fault.applies(d) {
				if s.fault.Kind == Stall {
					// Hold this read until the stall ends; the kernel
					// buffers fill behind it.
					p.hit(s.gen, q.id, d, Stall, n, off)
					for s.fault.Kind == Stall && s.fault.applies(d) {
						select {
						case <-s.changed:
						case <-q.done:
							return
						}
						s = p.cur.Load()
					}
					lastGen = s.gen
				}
				data = p.mutate(q, d, s, data, off, first, rng)
			}
			off += uint64(n)
			if len(data) > 0 {
				c := chunk{data: data, due: time.Now()}
				switch f := s.fault; {
				case f.Kind == Delay && f.applies(d):
					c.due = c.due.Add(f.Delay)
				case f.Kind == Switching && f.applies(d):
					c.due = c.due.Add(switching(f, time.Since(p.start)))
				}
				select {
				case ch <- c:
				case <-q.done:
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				select {
				case ch <- chunk{eof: true}:
				case <-q.done:
				}
				return
			}
			q.close(false)
			return
		}
	}
}

// switching returns the delay of a switching fault at time t.
func switching(f Fault, t time.Duration) time.Duration {
	if (t/f.Every)%2 == 0 {
		return f.Delay
	}
	return f.Delay2
}

// mutate applies the byte faults to one read.
func (p *Proxy) mutate(q *pair, d Dir, s *state, data []byte, off uint64, first bool, rng *rand.Rand) []byte {
	switch s.fault.Kind {
	case None, Stall, Reset:
	case Cut:
		p.hit(s.gen, q.id, d, Cut, len(data), off)
		q.damaged.Store(true)
		return nil
	case Drop, Corrupt, Duplicate:
		if !first && rng.IntN(4) != 0 {
			return data
		}
		i := rng.IntN(len(data))
		p.hit(s.gen, q.id, d, s.fault.Kind, 1, off+uint64(i))
		q.damaged.Store(true)
		if s.fault.Kind == Drop {
			return append(data[:i], data[i+1:]...)
		}
		if s.fault.Kind == Corrupt {
			data[i] ^= 1 << rng.IntN(8)
			return data
		}
		return append(data[:i+1], data[i:]...)
	case Delay, Switching, Rate:
		// The writer delays or paces; a hit is one read it holds back.
		p.hit(s.gen, q.id, d, s.fault.Kind, len(data), off)
	}
	return data
}

func (p *Proxy) write(q *pair, d Dir, dst *net.TCPConn, ch chan chunk) {
	for {
		var c chunk
		select {
		case c = <-ch:
		case <-q.done:
			return
		}
		if c.eof {
			_ = dst.CloseWrite()
			return
		}
		if !sleep(q, time.Until(c.due)) {
			return
		}
		if s := p.cur.Load(); s.fault.Kind == Rate && s.fault.applies(d) {
			if !sleep(q, time.Duration(len(c.data))*time.Second/time.Duration(s.fault.Rate)) {
				return
			}
		}
		if _, err := dst.Write(c.data); err != nil {
			q.close(false)
			return
		}
	}
}

// sleep waits for d or until the pair closes, and reports whether the
// pair is still open.
func sleep(q *pair, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-q.done:
		return false
	}
}
