package server_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// linkProxy forwards one peer link. stall makes the connections open at
// that moment stop reading from the sender; held is closed once one did.
type linkProxy struct {
	ln       net.Listener
	target   string
	mu       sync.Mutex
	stalled  bool
	held     chan struct{}
	heldOnce sync.Once
	release  chan struct{}
}

func newLinkProxy(t *testing.T, target string) *linkProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &linkProxy{ln: ln, target: target, held: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		ln.Close() //nolint:errcheck // closing a test listener
		close(p.release)
	})
	go p.serve()
	return p
}

func (p *linkProxy) stall() {
	p.mu.Lock()
	p.stalled = true
	p.mu.Unlock()
}

func (p *linkProxy) serve() {
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
		go func() {
			io.Copy(in, out) //nolint:errcheck // ends with the connection
			in.Close()       //nolint:errcheck // ends with the connection
		}()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				n, err := in.Read(buf)
				p.mu.Lock()
				stalled := p.stalled
				p.mu.Unlock()
				if stalled {
					p.heldOnce.Do(func() { close(p.held) })
					<-p.release
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

// TestStalledPeerLinkKeepsQuorum stalls the link from node 1 to node 2:
// node 1's writer blocks until its write deadline drops the connection.
// Whichever node leads, two nodes still reach each other both ways, so
// every write through node 3 must commit within 10 election timeouts.
func TestStalledPeerLinkKeepsQuorum(t *testing.T) {
	var proxy *linkProxy
	c := newClusterWith(t, func(c *cluster) {
		proxy = newLinkProxy(t, c.peerAddrs[2])
		c.via = map[[2]uint64]string{{1, 2}: proxy.ln.Addr().String()}
	})
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(3, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("q"), Value: []byte("0")})
	})
	proxy.stall()
	select {
	case <-proxy.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall never took effect")
	}
	cfg := core.DefaultConfig(1, nil)
	bound := 10 * time.Duration(cfg.ElectionTicks) * 10 * time.Millisecond
	for i := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		_, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("q"), Value: []byte{byte('a' + i)}})
		cancel()
		if err != nil {
			t.Fatalf("put %d with the link from node 1 to node 2 stalled did not commit within %v: %v", i, bound, err)
		}
	}
}
