package peer

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// errNotDropped is the failure TestPlanted counts as catching B14; any
// other error is a setup failure.
var errNotDropped = errors.New("the stalled peer connection was never dropped")

// proxyConn is one forwarded connection. Once stalled, its forwarder stops
// reading from the sender, like a peer whose reader hangs, and closes held.
type proxyConn struct {
	stall    atomic.Bool
	held     chan struct{}
	release  chan struct{}
	heldOnce sync.Once
}

// stallProxy forwards TCP connections to a target.
type stallProxy struct {
	ln       net.Listener
	target   string
	accepted atomic.Int64
	mu       sync.Mutex
	conns    []*proxyConn
}

func newStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln, target: target}
	t.Cleanup(func() {
		ln.Close() //nolint:errcheck // closing a test listener
		p.mu.Lock()
		for _, c := range p.conns {
			close(c.release)
		}
		p.mu.Unlock()
	})
	go p.serve()
	return p
}

// stallOpen stalls every connection open now; later ones work.
func (p *stallProxy) stallOpen() []*proxyConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.stall.Store(true)
	}
	return append([]*proxyConn(nil), p.conns...)
}

func (p *stallProxy) serve() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", p.target)
		if err != nil {
			in.Close() //nolint:errcheck // the peer redials
			continue
		}
		p.accepted.Add(1)
		c := &proxyConn{held: make(chan struct{}), release: make(chan struct{})}
		p.mu.Lock()
		p.conns = append(p.conns, c)
		p.mu.Unlock()
		go func() {
			io.Copy(in, out) //nolint:errcheck // ends with the connection
			in.Close()       //nolint:errcheck // ends with the connection
		}()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				n, err := in.Read(buf)
				if c.stall.Load() {
					c.heldOnce.Do(func() { close(c.held) })
					<-c.release
					return
				}
				if n > 0 {
					if _, werr := out.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					out.Close() //nolint:errcheck // ends with the connection
					return
				}
			}
		}()
	}
}

// stalledPeerIsDropped runs node 1 sending to node 2 through a proxy, then
// stalls the open connection so that node 1's writes block. The write
// deadline must fail that connection and report node 2 unreachable, and a
// message sent after the stall must reach node 2 over a new connection.
func stalledPeerIsDropped(t *testing.T) error {
	ca, err := testcert.NewCA()
	if err != nil {
		return err
	}
	c1, _, _, err := ca.Issue("node", 2)
	if err != nil {
		return err
	}
	c2, _, _, err := ca.Issue("node", 3)
	if err != nil {
		return err
	}
	addr1, addr2 := freeAddr(t), freeAddr(t)
	b := start(t, 2, map[uint64]string{1: addr1, 2: addr2}, c2, ca.Pool)
	defer b.t.Close() //nolint:errcheck // closing a test transport
	proxy := newStallProxy(t, addr2)
	a := start(t, 1, map[uint64]string{1: addr1, 2: proxy.ln.Addr().String()}, c1, ca.Pool)
	defer a.t.Close() //nolint:errcheck // closing a test transport

	a.t.Send(msg(1, "before"))
	select {
	case <-b.got:
	case <-time.After(10 * time.Second):
		return errors.New("nothing arrived before the stall")
	}
	stalled := proxy.stallOpen()
	if len(stalled) != 1 {
		return fmt.Errorf("%d connections before the stall, want 1", len(stalled))
	}
	unreachable := a.unreachable.Load()
	big := strings.Repeat("x", 128<<10)
	for i := range uint64(200) {
		a.t.Send(msg(10+i, big))
	}
	// The stall is in place once the forwarder got the next bytes and
	// stopped; from then on node 1's writes fill the socket and block.
	select {
	case <-stalled[0].held:
	case <-time.After(5 * time.Second):
		return errors.New("the stall never took effect")
	}
	deadline := time.Now().Add(writeTimeout + 10*time.Second)
	for time.Now().Before(deadline) {
		a.t.Send(msg(999, "after"))
		select {
		case m := <-b.got:
			if string(m.Entries[0].Data) != "after" {
				continue
			}
			if proxy.accepted.Load() < 2 {
				return errors.New("a message arrived after the stall without a new connection")
			}
			if a.unreachable.Load() == unreachable {
				return fmt.Errorf("%w: node 2 was never reported unreachable", errNotDropped)
			}
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w: nothing sent after the stall arrived", errNotDropped)
}

// TestStalledPeerIsDropped is the write deadline's test: see
// stalledPeerIsDropped.
func TestStalledPeerIsDropped(t *testing.T) {
	if err := stalledPeerIsDropped(t); err != nil {
		t.Fatal(err)
	}
}
