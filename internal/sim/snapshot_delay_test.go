package sim

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Prototype question: does a snapshot queued while an earlier install is
// syncing leave the leader waiting a second after the follower reloads?
// Run: scripts/sim.sh ./internal/sim '^TestSnapshotReloadCatchup$' -test.v
func TestSnapshotReloadCatchup(t *testing.T) {
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
		defer func() {
			t.Log(s.report())
			s.mu.Lock()
			defer s.mu.Unlock()
			t.Log(strings.Join(s.trace, "\n"))
		}()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		if lead < 0 {
			s.mu.Unlock()
			t.Fatal("no leader")
		}
		leader := s.nodes[lead]
		follower := s.nodes[(lead+1)%3]
		s.gray = follower
		s.mu.Unlock()
		put := func(round int) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if _, err := leader.proc.node.Do(ctx, &pb.PutRequest{Key: []byte("key"), Value: []byte(fmt.Sprintf("%04d%s", round, strings.Repeat("v", 128)))}); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 12 {
			put(i)
		}
		s.mu.Lock()
		// The snapshot sender finishes before the install sync. Heartbeats can
		// make it send a second snapshot to the old core during this interval.
		follower.stallUntil = s.now() + 500*Millisecond
		s.gray = nil
		s.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		for i := 12; i < 24; i++ {
			put(i)
		}
		time.Sleep(250 * time.Millisecond)
		s.mu.Lock()
		installs := 0
		for _, j := range follower.pending {
			if j.install {
				installs++
			}
		}
		t.Logf("before sync resumes: follower has %d pending installs", installs)
		s.mu.Unlock()
		if installs < 2 {
			t.Fatal("did not queue a second snapshot behind the first install")
		}
		time.Sleep(200 * time.Millisecond)
		s.mu.Lock()
		target := leader.last.applied
		fromLog := append([]int64(nil), follower.fromLog...)
		s.mu.Unlock()
		if len(fromLog) == 0 {
			t.Fatal("follower did not reload")
		}
		started := fromLog[0]
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			s.mu.Lock()
			applied := follower.last.applied
			s.mu.Unlock()
			if applied >= target {
				took := time.Duration(s.now() - started)
				t.Logf("follower %d caught up to %d after reload in %s", follower.id, target, took)
				if took > 300*time.Millisecond {
					t.Fatalf("catch-up took %s, want <=300ms", took)
				}
				return
			}
		}
		t.Fatal("follower did not catch up in 2s")
	})
}
