package sim

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestAnAuthChangeIsDurableBeforeItsAnswer is #823 in the simulator: of
// three nodes one is down, the second follower's log syncs take a second
// each, and a user is added through it. The leader commits the entry once
// the follower's sync of it ends, so the commit reaches the follower in an
// append of its own, after that sync. The follower answers once it
// applies the entry, crashes right after the answer, losing what it did
// not sync, and starts again. Its start must recover a commit that covers
// the auth change. Before, the follower applied and answered the entry as
// it learned the commit, a sync before the commit was durable: its start
// recovered the commit before the entry and replayed the auth of before,
// and the run's check failed at the apply (keptByReplay).
func TestAnAuthChangeIsDurableBeforeItsAnswer(t *testing.T) {
	if simrun.Drive(t, 823) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(823)
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
		if lead < 0 {
			s.mu.Unlock()
			t.Fatal("no leader after 3 s")
		}
		f := s.nodes[(lead+1)%len(s.nodes)]
		f.syncFor = Second
		s.crash(s.nodes[(lead+2)%len(s.nodes)])
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err = s.callWL(f.proc, &pb.AuthUserAddRequest{Name: "u", Options: &authpb.UserAddOptions{NoPassword: true}}, ctx)
		s.mu.Lock()
		if s.err != nil {
			s.mu.Unlock()
			t.Fatal(s.err)
		}
		if err != nil {
			s.mu.Unlock()
			t.Fatalf("add a user through node %d: %v", f.id, err)
		}
		answered := f.proc.wl.applied
		s.crash(f)
		s.mu.Unlock()

		if err := s.start(f); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			s.mu.Lock()
			up := f.proc != nil && f.proc.wl.recovered
			// Recovered set it; the start's first sync takes a second.
			commit := f.hs.commit
			s.mu.Unlock()
			if up {
				if commit < answered {
					t.Fatalf("node %d answered the auth change with entry %d applied, but its start recovered commit %d: it lost the change", f.id, answered, commit)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d did not start again in 10 s", f.id)
			}
		}
	})
}
