package sim

import (
	"testing"
	"testing/cryptotest"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestGrayFsyncOutlastsHealing builds the shape of #81 on purpose, so that
// it no longer depends on one seed's draws (#164): 1.5 s before healing
// the leader turns gray with slow fsyncs, and one of them, started before
// healing, ends after it. Its node's term and vote changes wait for it, so
// the liveness rule starts from its end (Sim.graySyncEnded). On every
// seed, every call that starts two election timeouts after it must get a
// definite answer, at least 50 calls must, and the checkers must pass;
// the seeds run until two had a gray fsync end 1.5 s or more after
// healing, as in #81.
func TestGrayFsyncOutlastsHealing(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	late := 0
	for seed := uint64(1); seed <= 40 && late < 2; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt, cfg.GrayLeaderSync = cfg.Duration-1500*Millisecond, true
		a := run(t, cfg)
		b := run(t, cfg)
		if a.Trace != b.Trace {
			t.Fatalf("seed %d: trace changed on replay: %016x, then %016x", seed, a.Trace, b.Trace)
		}
		checkers(t, a)
		if a.grayHealedAt <= cfg.Duration {
			t.Fatalf("seed %d: no gray fsync outlasted healing at %d ms", seed, cfg.Duration/Millisecond)
		}
		if a.grayHealedAt >= cfg.Duration+1500*Millisecond {
			late++
		}
		from := a.grayHealedAt + 2*election
		calls := 0
		for _, r := range a.History {
			if r.Call < from {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed after the gray fsync ended at %d ms: %v",
					seed, r.Node+1, r.Call/Millisecond, a.grayHealedAt/Millisecond, r.Err)
			}
			calls++
		}
		if calls < 50 {
			t.Fatalf("seed %d: only %d calls after %d ms", seed, calls, from/Millisecond)
		}
		t.Logf("seed %d: last gray fsync at %d ms; %d definite answers from %d ms", seed, a.grayHealedAt/Millisecond, calls, from/Millisecond)
	}
	if late < 2 {
		t.Fatalf("only %d seeds had a gray fsync end 1.5 s after healing", late)
	}
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
