package sim

import (
	"errors"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
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

type diag301Writer struct{ write func([]byte) (int, error) }

func (w diag301Writer) Write(b []byte) (int, error) { return w.write(b) }
