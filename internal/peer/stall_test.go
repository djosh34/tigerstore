package peer

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// stallProxy forwards TCP connections to a target. Once stalled, it stops
// reading from the sender on the connections open at that moment, like a
// peer whose reader hangs; later connections work.
type stallProxy struct {
	ln      net.Listener
	target  string
	stalled atomic.Bool
	mu      sync.Mutex
	held    []chan struct{}
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
		for _, h := range p.held {
			close(h)
		}
		p.mu.Unlock()
	})
	go p.serve()
	return p
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
		hold := make(chan struct{})
		p.mu.Lock()
		p.held = append(p.held, hold)
		p.mu.Unlock()
		go func() {
			io.Copy(in, out) //nolint:errcheck // ends with the connection
			in.Close()       //nolint:errcheck // ends with the connection
		}()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				if p.stalled.Load() {
					<-hold
					return
				}
				n, err := in.Read(buf)
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

// stalledPeerIsDropped runs node 1 sending to node 2 through a proxy that
// stops reading from node 1's connection, so node 1's writes block. The
// write deadline must fail that connection, and a message sent after the
// stall must reach node 2 over a new one.
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
	proxy.stalled.Store(true)
	big := strings.Repeat("x", 128<<10)
	for i := range uint64(200) {
		a.t.Send(msg(10+i, big))
	}
	// The write deadline is 5 s; then the sender redials, and the proxy
	// forwards new connections again.
	time.Sleep(100 * time.Millisecond)
	proxy.stalled.Store(false)
	deadline := time.Now().Add(writeTimeout + 10*time.Second)
	for time.Now().Before(deadline) {
		a.t.Send(msg(999, "after"))
		select {
		case m := <-b.got:
			if string(m.Entries[0].Data) == "after" {
				return nil
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("the stalled peer connection was never dropped: nothing sent after the stall arrived")
}

// TestStalledPeerIsDropped is the write deadline's test: see
// stalledPeerIsDropped.
func TestStalledPeerIsDropped(t *testing.T) {
	if err := stalledPeerIsDropped(t); err != nil {
		t.Fatal(err)
	}
}
