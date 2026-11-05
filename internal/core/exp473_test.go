package core

import (
	"fmt"
	"os"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestExp473TickWithLeases measures, natively and with the default
// configuration, what an idle tick of a leader costs with L leases, and
// what a restart costs with them (research #473, exp branch only).
// EXP473_TICKS=1 runs it.
func TestExp473TickWithLeases(t *testing.T) {
	if os.Getenv("EXP473_TICKS") == "" {
		t.Skip("EXP473_TICKS is not set")
	}
	for _, leases := range []int{0, 1_000, 10_000, 100_000} {
		disk := simfs.New()
		s := startSingleWith(t, disk, 1, nil)
		id := uint64(1)
		for i := 0; i < leases; {
			for j := 0; j < 500 && i < leases; j++ {
				s.n.Submit(Request{ID: id, Op: &pb.LeaseGrantRequest{TTL: 1 << 30}})
				id++
				i++
			}
			s.run()
		}
		if got := len(s.n.state.leases); got != leases {
			t.Fatalf("%d leases, want %d", got, leases)
		}
		const ticks = 2000
		start := time.Now()
		for range ticks {
			s.n.Tick()
			s.run()
		}
		perTick := time.Since(start) / ticks
		start = time.Now()
		startSingleWith(t, disk.Copy(), 1, nil)
		restart := time.Since(start)
		fmt.Printf("exp473 leases=%d tick=%v restart=%v\n", leases, perTick, restart)
	}
}

// TestExp473LeaseCheckpoint checks that a lease of 120 s gets checkpoints
// with LeaseCheckpointTicks of 100, with and without a capable cluster.
func TestExp473LeaseCheckpoint(t *testing.T) {
	for _, capable := range []bool{false, true} {
		s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.LeaseCheckpointTicks = 100 })
		if capable {
			s.capable()
		}
		s.ok(1, &pb.LeaseGrantRequest{ID: 7, TTL: 120})
		for range 350 {
			s.n.Tick()
			s.run()
		}
		l := s.n.state.leases[7]
		fmt.Printf("exp473 capable=%v version=%d remaining=%d\n", capable, s.n.state.version, l.remaining)
	}
}
