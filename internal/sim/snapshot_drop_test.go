package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestSnapshotDroppedAtReload lets a follower fall behind the leader's
// window, so that it fetches a snapshot, and holds the sync of the
// install for 500 ms, during which the leader writes on and sends it a
// second snapshot. The follower's core starts again from its log after
// the first install and drops the second, whose fetch never starts. The
// leader must give up on it and send another within two heartbeats, so
// the follower catches up within 300 ms of its start from the log: an
// election timeout held it back about a second (#366).
func TestSnapshotDroppedAtReload(t *testing.T) {
	if simrun.Drive(t, 366) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(366)
		cfg.Faults = Faults{}
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
		leader, follower := s.nodes[lead], s.nodes[(lead+1)%len(s.nodes)]
		puts := func(from, to int) {
			for i := from; i < to; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				_, err := leader.proc.node.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "%04d%0128d", i, 0)})
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		// Gray, the follower gets no appends, and the leader writes past
		// its window.
		s.mu.Lock()
		s.gray = follower
		s.mu.Unlock()
		puts(0, 12)
		s.mu.Lock()
		s.gray = nil
		follower.stallUntil = s.now() + 500*Millisecond
		s.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		puts(12, 24)
		time.Sleep(250 * time.Millisecond)
		s.mu.Lock()
		installs := 0
		for _, j := range follower.pending {
			if j.install {
				installs++
			}
		}
		s.mu.Unlock()
		if installs < 2 {
			t.Fatalf("the follower holds %d snapshot installs before its sync, want 2", installs)
		}
		time.Sleep(200 * time.Millisecond)
		s.mu.Lock()
		target, reloads := leader.last.applied, follower.fromLog
		s.mu.Unlock()
		if len(reloads) == 0 {
			t.Fatal("the follower did not start again from its log")
		}
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			s.mu.Lock()
			applied := follower.last.applied
			s.mu.Unlock()
			if applied < target {
				continue
			}
			took := s.now() - reloads[0]
			t.Logf("node %d applied %d %d ms after its start from the log", follower.id, target, took/Millisecond)
			if took > 300*Millisecond {
				t.Fatalf("node %d caught up %d ms after its start from the log, want 300 ms at most", follower.id, took/Millisecond)
			}
			return
		}
		t.Fatalf("node %d did not apply %d within 2 s", follower.id, target)
	})
}
