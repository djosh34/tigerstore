// Throwaway encrypted byte-stream proxy. No TLS termination or packet access.
package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const block = 4096

var modes = []string{"none", "drop", "short", "long", "switching", "stall", "reset", "corrupt", "duplicate", "one-way-cut", "both-cut", "fast", "slow"}

type event struct {
	Connection         uint64 `json:"connection"`
	Direction          int    `json:"direction"` // 0 client to upstream, 1 upstream to client
	Offset             uint64 `json:"offset"`    // bytes read from source after arm, before mutations
	Action             string `json:"action"`
	DelayNS            int64  `json:"delay_ns,omitempty"`
	AtNS               int64  `json:"at_ns"`
	RateBytesPerSecond int64  `json:"rate_bytes_per_second,omitempty"`
}
type policy struct {
	mode  string
	seed  uint64
	armed atomic.Bool
}
type proxy struct {
	listener net.Listener
	upstream string
	policy   *policy
	mu       sync.Mutex
	events   []event
	pairs    map[*pair]bool
	seq      uint64
	closing  bool
	trace    bool // immutable after first connection
	wg       sync.WaitGroup
	start    time.Time
}
type pair struct {
	a, b *net.TCPConn
	done chan struct{}
	once sync.Once
}

func (p *pair) close(reset bool) {
	p.once.Do(func() {
		if reset {
			_ = p.a.SetLinger(0)
			_ = p.b.SetLinger(0)
		}
		_ = p.a.Close()
		_ = p.b.Close()
		close(p.done)
	})
}
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func choice(seed, id uint64, dir int, offset uint64) uint64 {
	return mix(seed ^ mix(id) ^ mix(uint64(dir)+123) ^ mix(offset/block))
}
func newProxy(upstream, mode string, seed uint64, armed bool) (*proxy, error) {
	return listenProxy("127.0.0.1:0", upstream, mode, seed, armed)
}
func listenProxy(addr, upstream, mode string, seed uint64, armed bool) (*proxy, error) {
	valid := false
	for _, m := range modes {
		if m == mode {
			valid = true
		}
	}
	if !valid {
		return nil, fmt.Errorf("unknown mode %q", mode)
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return nil, e
	}
	p := &proxy{listener: l, upstream: upstream, policy: &policy{mode: mode, seed: seed}, pairs: make(map[*pair]bool), start: time.Now(), trace: true}
	p.policy.armed.Store(armed)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}
func (p *proxy) accept() {
	defer p.wg.Done()
	for {
		c, e := p.listener.Accept()
		if e != nil {
			return
		}
		// A bounded dial keeps shutdown finite. Listener closure is checked before registration.
		u, e := net.DialTimeout("tcp", p.upstream, time.Second)
		if e != nil {
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
		p.seq++
		id := p.seq
		p.pairs[q] = true
		p.mu.Unlock()
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			var w sync.WaitGroup
			w.Add(2)
			go func() { defer w.Done(); p.pump(q, id, 0, q.b, q.a) }()
			go func() { defer w.Done(); p.pump(q, id, 1, q.a, q.b) }()
			w.Wait()
			q.close(false)
			p.mu.Lock()
			delete(p.pairs, q)
			p.mu.Unlock()
		}()
	}
}
func (p *proxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closing = true
	for q := range p.pairs {
		q.close(false)
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *proxy) record(id uint64, dir int, off uint64, action string, d time.Duration) {
	if !p.trace {
		return
	}
	p.mu.Lock()
	ev := event{Connection: id, Direction: dir, Offset: off, Action: action, DelayNS: int64(d), AtNS: time.Since(p.start).Nanoseconds()}
	if action == "fast" {
		ev.RateBytesPerSecond = 100_000_000
	}
	if action == "slow" {
		ev.RateBytesPerSecond = 1_000_000
	}
	p.events = append(p.events, ev)
	p.mu.Unlock()
}
func wait(q *pair, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-q.done:
		return false
	}
}
func writeAll(c net.Conn, b []byte) error {
	for len(b) > 0 {
		n, e := c.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func (p *proxy) pump(q *pair, id uint64, dir int, dst, src *net.TCPConn) {
	// Limits lifetime in CLI and tests; no unbounded sketch goroutines.
	_ = src.SetReadDeadline(time.Now().Add(30 * time.Second))
	_ = dst.SetWriteDeadline(time.Now().Add(30 * time.Second))
	buf := make([]byte, 32*1024)
	var off uint64
	var paceStart time.Time
	var paced uint64
	active := false
	fired := false
	target := uint64(32 + choice(p.policy.seed, id, dir, 0)%96)
	mode := p.policy.mode
	for {
		n, e := src.Read(buf)
		if n > 0 {
			armed := p.policy.armed.Load()
			if !armed || mode == "none" {
				if writeAll(dst, buf[:n]) != nil {
					q.close(false)
					return
				}
			} else {
				if !active {
					active = true
					off = 0
				}
				data := buf[:n]
				for len(data) > 0 {
					span := block - int(off%block)
					if span > len(data) {
						span = len(data)
					}
					b := data[:span]
					at := off
					end := off + uint64(span)
					if off%block == 0 {
						var d time.Duration
						switch mode {
						case "short":
							d = time.Millisecond
						case "long":
							d = 25 * time.Millisecond
						case "switching":
							if (off/block+choice(p.policy.seed, id, dir, 0)%2)%2 == 0 {
								d = time.Millisecond
							} else {
								d = 25 * time.Millisecond
							}
						}
						if d > 0 {
							p.record(id, dir, off, mode, d)
							if !wait(q, d) {
								return
							}
						}
					}
					applies := dir == 0 || mode == "both-cut"
					if applies && !fired && target >= at && target < end {
						pos := int(target - at)
						switch mode {
						case "drop":
							p.record(id, dir, target, mode, 0)
							fired = true
							b = append(b[:pos], b[pos+1:]...)
						case "duplicate":
							p.record(id, dir, target, mode, 0)
							fired = true
							copyB := make([]byte, len(b)+1)
							copy(copyB, b[:pos+1])
							copy(copyB[pos+1:], b[pos:])
							b = copyB
						case "corrupt":
							p.record(id, dir, target, mode, 0)
							fired = true
							b[pos] ^= 1
						case "reset":
							p.record(id, dir, target, mode, 0)
							q.close(true)
							return
						case "stall":
							p.record(id, dir, target, mode, 0)
							if writeAll(dst, b[:pos]) != nil {
								q.close(false)
								return
							}
							<-q.done
							return // stop consuming input, bounded by one read buffer
						case "one-way-cut", "both-cut":
							p.record(id, dir, target, mode, 0)
							fired = true
							if writeAll(dst, b[:pos]) != nil {
								q.close(false)
								return
							}
							b = nil
						}
					} else if applies && fired && (mode == "one-way-cut" || mode == "both-cut") {
						b = nil
					}
					if mode == "fast" || mode == "slow" {
						rate := int64(100_000_000)
						if mode == "slow" {
							rate = 1_000_000
						}
						if paceStart.IsZero() {
							paceStart = time.Now()
						}
						paced += uint64(len(b))
						// The cumulative deadline depends on source bytes, not Read sizes.
						// This crude average-rate model allows credit after idle time.
						d := time.Until(paceStart.Add(time.Duration(paced * uint64(time.Second) / uint64(rate))))
						if off%block == 0 {
							p.record(id, dir, off, mode, 0)
						}
						if d > 0 && !wait(q, d) {
							return
						}
					}
					if len(b) > 0 && writeAll(dst, b) != nil {
						q.close(false)
						return
					}
					off = end
					data = data[span:]
				}
			}
		}
		if e != nil {
			if e == io.EOF {
				_ = dst.CloseWrite()
				return
			}
			q.close(false)
			return
		}
	}
}
