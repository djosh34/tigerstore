package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestColdRestartWaitsOnASlowSnapshot is #809 in the small: a cold restart
// whose node behind must take a snapshot that outlasts the 9 s the check
// gave a restart to agree. A follower stops; the leader takes 8 MB of
// puts and compacts its log past it (a catch-up of 4 entries, a disk
// budget of 4 MiB); a cold restart then starts the leader and the
// follower from their disks, erasing the third node. Each snapshot chunk
// of 1 MiB waits 2 s, so the snapshot takes about 16 s. The restart must
// wait on while the snapshot is served, and pass.
func TestColdRestartWaitsOnASlowSnapshot(t *testing.T) {
	if simrun.Drive(t, 809) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(809)
		// A ring of 2 MiB: the 4 entries of 128 KiB the leader keeps for the
		// stopped follower fit it, which a ring of 512 KiB did not (#814).
		cfg.Faults = Faults{DiskBudget: 4 << 20}
		cfg.Catchup = 4
		cfg.snapshotWait = 2 * Second
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
		behind, erased := (lead+1)%3, (lead+2)%3
		s.kill(s.nodes[behind])
		s.mu.Unlock()
		leader := s.nodes[lead]
		for i := range 64 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := s.callWL(leader.proc, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 128<<10)}, ctx)
			cancel()
			if err != nil {
				t.Fatalf("put %d through node %d: %v", i, leader.id, err)
			}
		}
		// Time for the leader to checkpoint and let its log go.
		time.Sleep(5 * time.Second)
		s.mu.Lock()
		c := s.newCheckpoint()
		s.mu.Unlock()
		if err := s.freeze(); err != nil {
			t.Fatal(err)
		}
		begin := s.now()
		if err := s.coldRestart(c, erased); err != nil {
			t.Fatalf("erasing node %d: %v", s.nodes[erased].id, err)
		}
		// A snapshot of the puts was served, and outlasted the old
		// window: the test tested the wait.
		s.mu.Lock()
		served := s.coldSnapsServed
		s.mu.Unlock()
		if d := s.now() - begin; d < 10*Second || served < 4<<20 {
			t.Fatalf("the cold restart caught up in %s, with %d bytes of snapshots served: no snapshot slow enough to test the wait", seconds(d), served)
		}
	})
}

// A snapshot stream sent again from its start is no progress until it
// passes where the last stream to that node got (the review of #810): a
// snapshot refused and sent again and again kept a cold restart waiting.
func TestSnapProgressCountsAStreamSentAgainOnce(t *testing.T) {
	var p snapProgress
	for range 3 {
		for i := range 4 {
			p.chunk(2, i == 0, 1<<20)
		}
	}
	if got := p.served(); got != 4<<20 {
		t.Fatalf("three streams of 4 MiB to one node served %d bytes, want 4 MiB", got)
	}
	for i := range 6 {
		p.chunk(2, i == 0, 1<<20)
	}
	p.chunk(3, true, 1<<20)
	if got := p.served(); got != 7<<20 {
		t.Fatalf("a stream of 6 MiB after them and 1 MiB to another node: %d bytes, want 7 MiB", got)
	}
}
