package sim

import (
	"testing"
	"testing/cryptotest"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestGrayHealingSeed keeps a schedule of the shape from #81: healing is
// at 20 s, but a gray fsync started before it ends at 22.29 s. The last
// call without a definite answer starts at 22.4117 s, after the election
// that follows; from 22.412 s every call gets one, long before the
// checker's recovery allowance ends at 26.29 s. Seed 745672193 had this
// shape until writes waited for a leader (#106), and seed 384 until
// design E changed the timing (#143). Replies that wait for values read
// from disk (#144) moved that last call from 22.407 s.
func TestGrayHealingSeed(t *testing.T) {
	t.Skip("seed 2507 lost this shape when the simulator got the FetchCrash and SlowFetch faults; #164 re-pins it")
	cfg := DefaultConfig(2507)
	a := run(t, cfg)
	b := run(t, cfg)
	if a.Trace != b.Trace {
		t.Fatalf("trace changed on replay: %016x, then %016x", a.Trace, b.Trace)
	}
	checkers(t, a)
	if a.grayHealedAt < cfg.Duration+2285*Millisecond || a.grayHealedAt > cfg.Duration+2286*Millisecond {
		t.Fatalf("last gray fsync at %d ms, want the fsync at 22285 ms", a.grayHealedAt/Millisecond)
	}
	calls := 0
	for _, r := range a.History {
		if r.Call < cfg.Duration+2412*Millisecond {
			continue
		}
		if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
			t.Fatalf("call to node %d at %d ms failed after the final election: %v", r.Node+1, r.Call/Millisecond, r.Err)
		}
		calls++
	}
	if calls < 50 {
		t.Fatalf("only %d calls after the final election", calls)
	}
	t.Logf("last gray fsync at %d ms; %d definite answers from 22412 ms; trace %016x", a.grayHealedAt/Millisecond, calls, a.Trace)
}

// TestGrayFsyncRecoveryBound checks the disk completion path, including a
// gray fsync that outlives the network fault. Neither a healthy fsync nor
// a completion canceled by a crash may move the liveness bound.
func TestGrayFsyncRecoveryBound(t *testing.T) {
	for _, tc := range []struct {
		name     string
		grayDisk bool
		crash    bool
	}{
		{"gray fsync", true, false},
		{"healthy fsync", false, false},
		{"canceled gray fsync", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cryptotest.SetGlobalRandom(t, 1)
			cfg := DefaultConfig(1)
			cfg.Heal = cfg.LiveAfter
			s := newSim(cfg)
			s.addNode(0, simfs.New())
			n := s.nodes[0]
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
			s.now = s.healAt - 1
			s.gray = n
			n.graySync = tc.grayDisk
			n.diskQ = []core.PersistJob{{}}
			s.startDisk(n)
			e := n.syncing
			if e == nil || e.grayDisk != tc.grayDisk {
				t.Fatal("fsync did not retain its gray fault marker")
			}
			s.now, s.healed = s.healAt, true
			s.gray, s.grayOnce = nil, false
			if tc.crash {
				s.crash(n)
			}
			s.now = e.at
			s.diskDone(e)
			want := s.healAt + cfg.LiveAfter
			if tc.grayDisk && !tc.crash {
				want = s.now + cfg.LiveAfter
				if s.stopAt != want+3*Second || s.endAt-s.stopAt != leaseGrace+2*Second {
					t.Fatal("moving the bound did not preserve client coverage and quiet time")
				}
			}
			if got := s.liveFrom(); got != want {
				t.Fatalf("liveness starts at %d, want %d", got, want)
			}
		})
	}
}

// TestGrayAppendRecoveryBound checks that the append drop which ends the
// guaranteed gray period after healing does not move the liveness rule:
// that drop can come only after an election, which the rule must bound.
func TestGrayAppendRecoveryBound(t *testing.T) {
	cfg := DefaultConfig(1)
	s := newSim(cfg)
	for i := range 3 {
		s.addNode(i, simfs.New())
		s.nodes[i].up = true
	}
	s.now, s.healed = s.healAt+Second, true
	s.gray, s.grayOnce = s.nodes[0], true
	s.send(s.nodes[1], &raftpb.Message{Type: raftpb.MessageType_MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))})
	if s.gray != nil || s.grayOnce || !s.grayDropped || s.stats.GrayNodes != 1 {
		t.Fatal("guaranteed gray period did not end on its first append drop")
	}
	if got, want := s.liveFrom(), s.healAt+cfg.LiveAfter; got != want {
		t.Fatalf("liveness starts at %d, want %d", got, want)
	}
}
