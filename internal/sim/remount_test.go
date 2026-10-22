package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestRemountAfterExit holds a follower's syncs in a stall while it
// writes, and shuts its file system down: the held sync fails, and so does
// the cut that would drop what it did not write (#364). The follower
// exits and keeps its page cache, and its file system is mounted again
// while it is down. The remount must lose what the disk did not sync, as
// an unmount does; else the follower starts again on pages that no sync
// writes, and a power loss after it synced later entries finds a hole in
// its log (#371).
func TestRemountAfterExit(t *testing.T) {
	if simrun.Drive(t, 371) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(371)
		cfg.Faults = Faults{}
		cfg.syncFaultExits = true
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
		s.mu.Unlock()
		if lead < 0 {
			t.Fatal("no leader after 3 s")
		}
		leader, follower := s.nodes[lead], s.nodes[(lead+1)%len(s.nodes)]
		puts := func(from, to int) {
			for i := from; i < to; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := leader.proc.node.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "%04d", i)})
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		s.mu.Lock()
		follower.stallUntil = s.now() + 500*Millisecond
		s.mu.Unlock()
		puts(0, 5)
		s.mu.Lock()
		follower.disk.Shutdown()
		s.mu.Unlock()
		// The stall ends, the held sync fails, and the follower exits.
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
			s.mu.Lock()
			down := follower.proc == nil
			if down {
				s.remount(follower)
			}
			s.mu.Unlock()
			if down {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the follower did not stop on its shut-down disk")
			}
		}
		// It starts again, catches up and syncs later entries.
		time.Sleep(3 * time.Second)
		puts(5, 10)
		time.Sleep(500 * time.Millisecond)
		s.mu.Lock()
		if follower.proc == nil {
			s.mu.Unlock()
			t.Fatal("the follower did not start again")
		}
		follower.why = "the test cut its power"
		s.crash(follower)
		s.mu.Unlock()
		if err := s.failure(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestRemountTakesRunningNodeDown shuts a running follower's file system
// down and mounts it again at once, as healing can before the follower
// exits on its disk's error. The remount must take the follower down as a
// power loss, which drops its page cache, as fsDown does: else pages a
// failed sync left readable outlive the remount with the process (#371).
func TestRemountTakesRunningNodeDown(t *testing.T) {
	if simrun.Drive(t, 372) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(372)
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
		defer s.mu.Unlock()
		lead := s.leaderIdx()
		if lead < 0 {
			t.Fatal("no leader after 3 s")
		}
		follower := s.nodes[(lead+1)%len(s.nodes)]
		follower.disk.Shutdown()
		s.remount(follower)
		if follower.proc != nil {
			t.Fatalf("node %d still runs after its disk was mounted again", follower.id)
		}
		if follower.disk.Down() {
			t.Fatalf("node %d's disk is still down", follower.id)
		}
	})
}
