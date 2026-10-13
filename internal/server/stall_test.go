package server_test

import (
	"bytes"
	"context"
	"errors"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/djosh34/tigerstore/internal/core"
)

// linkProxy forwards one directed peer link. stall makes the connections
// open at that moment stop reading from the sender, so its writes back up;
// held is closed once one of them stopped. Later connections work.
type linkProxy struct {
	ln       net.Listener
	target   string
	accepted atomic.Int64
	mu       sync.Mutex
	open     []*atomic.Bool
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
	defer p.mu.Unlock()
	for _, s := range p.open {
		s.Store(true)
	}
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
		p.accepted.Add(1)
		stalled := &atomic.Bool{}
		p.mu.Lock()
		p.open = append(p.open, stalled)
		p.mu.Unlock()
		go func() {
			io.Copy(in, out) //nolint:errcheck // ends with the connection
			in.Close()       //nolint:errcheck // ends with the connection
		}()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				n, err := in.Read(buf)
				if stalled.Load() {
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

// TestStalledPeerLinkKeepsQuorum runs every peer link through a proxy and
// stalls the leader's link to one follower while a client writes 64 KiB
// values through the other follower. The leader's writer to the stalled
// follower backs up until its write deadline drops the connection; until
// then and after, the leader and the other follower are a quorum, so every
// write must commit within 10 election timeouts.
func TestStalledPeerLinkKeepsQuorum(t *testing.T) {
	proxies := map[[2]uint64]*linkProxy{}
	c := newClusterWith(t, func(c *cluster) {
		c.via = map[[2]uint64]string{}
		for from := uint64(1); from <= 3; from++ {
			for to := uint64(1); to <= 3; to++ {
				if from != to {
					p := newLinkProxy(t, c.peerAddrs[to])
					proxies[[2]uint64{from, to}] = p
					c.via[[2]uint64{from, to}] = p.ln.Addr().String()
				}
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("q"), Value: []byte("0")})
	})
	conn, err := grpc.NewClient(c.running[1].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	st := retry(t, func(ctx context.Context) (*pb.StatusResponse, error) {
		st, err := pb.NewMaintenanceClient(conn).Status(ctx, &pb.StatusRequest{})
		if err == nil && st.Leader == 0 {
			err = errors.New("no leader yet")
		}
		return st, err
	})
	leader := st.Leader
	stalled, healthy := leader%3+1, (leader+1)%3+1
	proxy := proxies[[2]uint64{leader, stalled}]
	kv := c.client(healthy, cert)
	before := proxy.accepted.Load()
	proxy.stall()

	bound := 10 * time.Duration(core.DefaultConfig(1, nil).ElectionTicks) * 10 * time.Millisecond
	value := bytes.Repeat([]byte("v"), 64<<10)
	deadline := time.Now().Add(30 * time.Second)
	puts := 0
	for proxy.accepted.Load() == before && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		_, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("q"), Value: value})
		cancel()
		if err != nil {
			t.Fatalf("put %d with the link from node %d to node %d stalled did not commit within %v: %v", puts, leader, stalled, bound, err)
		}
		puts++
		// About 6 MiB a second, well past the socket buffers, but not a
		// gigabyte of log before the deadline.
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-proxy.held:
	default:
		t.Fatal("the stall never took effect")
	}
	if proxy.accepted.Load() == before {
		t.Fatalf("after %d puts, the leader's writer never hit its write deadline", puts)
	}
	t.Logf("%d puts of 64 KiB committed while the link from node %d to node %d was stalled", puts, leader, stalled)
}
