package proxy

import (
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

// Router listens on one address for one node, so that every other node
// can name that node by one URL, as a cluster's --initial-cluster does,
// and still goes through a Proxy of its own on the way there. It passes
// each connection on, byte for byte, to the address pick returns for it:
// a Proxy's, or the node's own for "".
type Router struct {
	ln       net.Listener
	upstream string
	pick     func(*net.TCPConn) string
	mu       sync.Mutex
	conns    map[*net.TCPConn]bool
	closing  bool
	wg       sync.WaitGroup
}

// Route starts a Router on addr for the node at upstream.
func Route(addr, upstream string, pick func(*net.TCPConn) string) (*Router, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	r := &Router{ln: ln, upstream: upstream, pick: pick, conns: map[*net.TCPConn]bool{}}
	r.wg.Go(r.accept)
	return r, nil
}

// Addr is the address the Router listens on.
func (r *Router) Addr() string { return r.ln.Addr().String() }

func (r *Router) accept() {
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		a := c.(*net.TCPConn)
		to := r.pick(a)
		if to == "" {
			to = r.upstream
		}
		u, err := net.DialTimeout("tcp", to, time.Second)
		if err != nil {
			_ = a.Close()
			continue
		}
		b := u.(*net.TCPConn)
		if !r.track(a, b) {
			return
		}
		r.wg.Go(func() {
			var w sync.WaitGroup
			w.Go(func() { splice(a, b) })
			w.Go(func() { splice(b, a) })
			w.Wait()
			r.mu.Lock()
			delete(r.conns, a)
			delete(r.conns, b)
			r.mu.Unlock()
		})
	}
}

// track keeps a and b, so that Close closes them, unless the Router is
// closing.
func (r *Router) track(a, b *net.TCPConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		_ = a.Close()
		_ = b.Close()
		return false
	}
	r.conns[a], r.conns[b] = true, true
	return true
}

// splice copies src to dst. At src's end it ends dst's writes; when src
// was reset, as a Proxy's Reset or Cut does, it resets dst too, so the
// dialer sees what it would see without the Router.
func splice(dst, src *net.TCPConn) {
	_, err := io.Copy(dst, src)
	if errors.Is(err, syscall.ECONNRESET) {
		_ = dst.SetLinger(0)
		_ = dst.Close()
		_ = src.Close()
		return
	}
	_ = dst.CloseWrite()
	if err != nil {
		_ = dst.Close()
		_ = src.Close()
	}
}

// Close stops the Router and closes the connections it passes on.
func (r *Router) Close() error {
	err := r.ln.Close()
	r.mu.Lock()
	r.closing = true
	for c := range r.conns {
		_ = c.Close()
	}
	r.mu.Unlock()
	r.wg.Wait()
	return err
}
