package sim

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestStopWhileServingSnapshot fails the leader's next log write while a
// follower leaves unread the snapshot prefix the leader serves it, as seed
// 377142 did. The run must see the leader stop on its disk error at once,
// not when the run ends (#301).
func TestStopWhileServingSnapshot(t *testing.T) {
	if simrun.Drive(t) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
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
			t.Fatal("no leader after 3 s")
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
			t.Fatal("no snapshot prefix waits for the follower")
		}
		s.mu.Lock()
		s.armDiskFault(n, 1)
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if _, err := p.node.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}); err == nil {
			t.Fatal("a put succeeded with the leader's disk fault armed")
		}
		time.Sleep(20 * time.Millisecond)
		s.mu.Lock()
		running := n.proc == p
		s.mu.Unlock()
		if running {
			t.Fatal("the leader stopped on its disk error, but the run still has it running")
		}
	})
}
