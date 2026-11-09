package sim

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestHeartbeatsPassASlowAppend is #553 in the simulator: the leader's
// links to both followers carry 16 KiB/s, and a put of 48 KiB through it
// takes about 3 s to reach each one, three election timeouts and within
// the request timeout of 7 s. Its
// heartbeats go on another connection, so the followers keep hearing from
// it: the put commits and the leader stays in office. With one connection
// a peer's heartbeats waited behind the append, the followers elected
// another leader, and the put was lost.
func TestHeartbeatsPassASlowAppend(t *testing.T) {
	if simrun.Drive(t, 553) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(553)
		cfg.Faults = Faults{}
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if t.Failed() {
				explainSim(t, cfg, s)
			}
			s.shutdown()
		}()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		var term uint64
		if lead >= 0 {
			term = s.nodes[lead].proc.core.Term()
			for f := range s.nodes {
				if f == lead {
					continue
				}
				l := s.link(lead, f)
				l.netem, l.bw, l.hits = slow, 16<<10, 0
				l.netemUntil = s.now() + 60*Second
			}
		}
		s.mu.Unlock()
		if lead < 0 {
			t.Fatal("no leader after 3 s")
		}
		leader := s.nodes[lead]
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		if _, err := leader.proc.node.Do(ctx, &pb.PutRequest{Key: []byte("big"), Value: make([]byte, 48<<10)}); err != nil {
			t.Fatalf("a put of 48 KiB through node %d on slow links: %v", leader.id, err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if now := s.leaderIdx(); now != lead || leader.proc.core.Term() != term {
			t.Fatalf("the leader changed from node %d at term %d to index %d at term %d while the append crossed the slow links", leader.id, term, now, leader.proc.core.Term())
		}
	})
}
