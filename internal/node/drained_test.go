package node

import (
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestDrainedWaitsForTheLoop stops a node whose loop holds a connection
// that, told the node stopped, writes late, as a peer connection logs
// what waited in its lane when it fails: Drained closes only after the
// write, so whoever stops the node and then reads what the connection
// wrote, as the simulator reads its trace, reads it after. Without that
// wait, the read raced with the write (#638, main's race-sim on 4d8b4e9f).
func TestDrainedWaitsForTheLoop(t *testing.T) {
	n, stop := leaderless(t, &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}, func(*Config) {})
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	h := &lateStopper{l: n.Loop()}
	attached := make(chan error, 1)
	n.Loop().Post(func() {
		var err error
		h.sock, h.token, err = n.Loop().Attach(a, h)
		attached <- err
	})
	if err := <-attached; err != nil {
		t.Fatal(err)
	}
	stop()
	<-n.Drained()
	// A plain read. If nothing orders it after the handler's write, the
	// race detector reports the write, which the test stays to see.
	stopped := h.stopped
	time.Sleep(2 * lateBy)
	if !stopped {
		t.Fatal("Drained closed before the loop's connection heard that the node stopped")
	}
}

// lateBy is how late a lateStopper writes.
const lateBy = 50 * time.Millisecond

// lateStopper is a connection on a loop that, when the node stops, writes
// late and then closes, as the transport's do.
type lateStopper struct {
	l       *loop.Loop
	sock    loop.Socket
	token   uint64
	stopped bool
}

func (h *lateStopper) Ready() {}

func (h *lateStopper) Stopped() {
	time.Sleep(lateBy)
	h.stopped = true
	_ = h.sock.Close()
	h.l.Forget(h.token)
}
