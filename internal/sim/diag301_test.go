package sim

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestDiag301ClosedPrefix checks the snapshot fault's wait after its
// source stops and closes a connection with unread bytes.
func TestDiag301ClosedPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig(377142)
		cfg.Faults = Faults{FetchCrash: true}
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.shutdown()
		n := s.nodes[2]
		if err := s.start(n); err != nil {
			t.Fatal(err)
		}
		p := n.proc
		q := newPipe()
		c := &conn{s: s, p: p, from: 2, to: 1, in: newPipe(), out: q}
		c.other = &conn{s: s, in: q, out: c.in}
		written := make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			finished <- s.server(p, p.node)(2, 1, 0, diag301Writer{func(b []byte) (int, error) {
				s.mu.Lock()
				q.chunks = append(q.chunks, chunk{at: s.now(), b: append([]byte(nil), b...)})
				s.net.conns[c] = true
				s.mu.Unlock()
				close(written)
				return len(b), nil
			}})
		}()
		<-written
		synctest.Wait()
		// Close leaves q's unread bytes and no error. The fault's wait
		// already saved q, so removing c from conns does not release it.
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		// This test calls Serve itself rather than through the transport.
		// Detach the process so exited does not end the whole simulation
		// before we check whether the Serve callback returned.
		s.mu.Lock()
		n.proc = nil
		s.mu.Unlock()
		p.cancel()
		<-p.node.Done()
		time.Sleep(20 * time.Millisecond)
		select {
		case err := <-finished:
			if !errors.Is(err, errDead) {
				t.Fatalf("snapshot fault returned %v, want %v", err, errDead)
			}
		default:
			buf := make([]byte, 1<<20)
			t.Logf("stacks after source shutdown:\n%s", buf[:runtime.Stack(buf, true)])
			t.Error("snapshot fault still waits for unread bytes after the source stopped")
		}
	})
}

// TestDiag301DiskStop uses the real transport's Serve callback and a real
// failed WAL write. The follower leaves the snapshot prefix unread.
func TestDiag301DiskStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig(377142)
		cfg.Faults = Faults{FetchCrash: true}
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.shutdown()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		s.mu.Unlock()
		if lead < 0 {
			t.Fatal("no leader after three simulated seconds")
		}
		n := s.nodes[lead]
		p := n.proc
		follower := s.nodes[(lead+1)%len(s.nodes)].proc
		r, err := follower.t.Fetch(context.Background(), n.id, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		time.Sleep(20 * time.Millisecond)
		if len(s.net.queued(s, lead, follower.n.id)) == 0 {
			t.Fatal("the snapshot prefix was not queued")
		}
		s.mu.Lock()
		s.armDiskFault(n, 1)
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = p.node.Do(ctx, &pb.PutRequest{Key: []byte("diag301"), Value: []byte("v")})
		if err == nil {
			t.Fatal("the write succeeded with its disk fault armed")
		}
		time.Sleep(20 * time.Millisecond)
		s.mu.Lock()
		stuck := n.proc == p
		trace := strings.Join(s.trace, "\n")
		s.mu.Unlock()
		t.Logf("trace:\n%s", trace)
		if stuck {
			buf := make([]byte, 1<<20)
			t.Logf("stacks after disk error:\n%s", buf[:runtime.Stack(buf, true)])
			t.Error("the disk-failed leader has not exited because transport shutdown is stuck")
		}
	})
}

type diag301Writer struct{ write func([]byte) (int, error) }

func (w diag301Writer) Write(b []byte) (int, error) { return w.write(b) }
