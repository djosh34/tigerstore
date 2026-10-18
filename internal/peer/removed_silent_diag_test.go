package peer

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// A behind learner has no leader and emits no Raft messages. Its transport
// survives snapshot recovery, with an established outbound connection. When
// the cluster removes it, that connection must notice EOF and redial to hear
// the existing removed reply. No Send, Fetch or Call may be needed.
func TestDiag263SilentRemovedMember(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 4)
	a := start(t, 1, lns[1], addrs, cert, ca.Pool)
	t.Cleanup(func() {
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
	})
	removed := make(chan struct{}, 1)
	var dials atomic.Int64
	b := startWith(t, 4, lns[4], addrs, cert, ca.Pool, func(c *Config) {
		c.Removed = func() {
			select {
			case removed <- struct{}{}:
			default:
			}
		}
		c.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
	})
	t.Cleanup(func() {
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		// Wait for both sides of the exact idle connection that removal closes.
		if !b.t.ActiveSince(1).IsZero() && a.t.Versions().In[4] != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial connection did not establish")
		}
		time.Sleep(time.Millisecond)
	}
	before := dials.Load()
	a.t.SetPeers(map[uint64]string{}, []uint64{4})
	select {
	case <-removed:
		t.Logf("silent removed member learned removal after %d reconnect(s)", dials.Load()-before)
	case <-time.After(3 * time.Second):
		t.Fatalf("silent removed member still up: dials before=%d after=%d; stale active connection=%v", before, dials.Load(), !b.t.ActiveSince(1).IsZero())
	}
}
