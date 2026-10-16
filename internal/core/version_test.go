package core

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestAnnounceRaisesVersionOnce(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	for i, step := range []struct {
		id      uint64
		v, want uint32
		run     uint64
	}{
		{1, 2, 1, 1}, {2, 2, 1, 1}, // node 3 has not announced
		{3, 1, 1, 1},               // node 3 is older
		{2, 1, 1, 2}, {3, 2, 1, 2}, // node 2 rolls back before node 3 upgrades
		{2, 2, 1, 1},               // node 2's announcement of run 1 arrives late (#174)
		{4, 3, 1, 1},               // not a voter
		{2, 2, 2, 3},               // node 2 upgrades again: every voter supports 2
		{3, 1, 2, 3}, {1, 3, 2, 2}, // never goes down
	} {
		st.announce(step.id, step.v, step.run, true, voters)
		if st.version != step.want {
			t.Fatalf("step %d: node %d announced %d, version %d, want %d", i, step.id, step.v, st.version, step.want)
		}
	}
}

// TestLateAnnouncementAppliesFirst is #174's other order: node 2's
// announcement of version 2 from run 1 arrives late but applies before
// that of its rollback to version 1 in run 2. The rollback still counts,
// and node 3's upgrade does not activate version 2.
func TestLateAnnouncementAppliesFirst(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	st.announce(1, 2, 1, true, voters)
	st.announce(3, 1, 1, true, voters)
	st.announce(2, 2, 1, true, voters) // run 1, late
	st.announce(2, 1, 2, true, voters) // run 2, the rollback
	st.announce(3, 2, 2, true, voters)
	if st.version != 1 || st.supports[2] != 1 {
		t.Fatalf("version %d, node 2 supports %d; want 1 and 1", st.version, st.supports[2])
	}
}

func TestAnnouncementRun(t *testing.T) {
	b, err := proto.Marshal(announcement(3, 1234))
	if err != nil {
		t.Fatal(err)
	}
	if a, err := parseAnnouncement(b); err != nil || a.version != 3 || a.run != 1234 || !a.hasRun || a.hasEpoch {
		t.Fatalf("got %+v, %v", a, err)
	}
	// A build before #174 reads the version alone.
	var old wrapperspb.UInt32Value
	if err := proto.Unmarshal(b, &old); err != nil || old.Value != 3 {
		t.Fatalf("plain message: %v, %v", old.Value, err)
	}
	// And this one reads its announcements without a run.
	plain, err := proto.Marshal(wrapperspb.UInt32(2))
	if err != nil {
		t.Fatal(err)
	}
	if a, err := parseAnnouncement(plain); err != nil || a.version != 2 || a.hasRun || a.hasEpoch {
		t.Fatalf("got %+v, %v", a, err)
	}
	b, err = proto.Marshal(confirmation(3, 1234, 5678))
	if err != nil {
		t.Fatal(err)
	}
	if a, err := parseAnnouncement(b); err != nil || a.version != 3 || a.run != 1234 || a.epoch != 5678 || !a.hasRun || !a.hasEpoch {
		t.Fatalf("confirmation: %+v, %v", a, err)
	}
}

// TestLegacyAnnouncements replays announcements without a run, from a
// build before #174: each counts, as it did there, so versions rise and a
// rollback holds the cluster back; a later one with a run still works.
func TestLegacyAnnouncements(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	for i, step := range []struct {
		id      uint64
		v, want uint32
		legacy  bool
	}{
		{1, 2, 1, true}, {2, 2, 1, true}, {3, 1, 1, true}, // node 3 is older
		{2, 1, 1, true}, {3, 2, 1, true}, // node 2 rolls back
		{2, 2, 2, false}, // node 2 upgrades again with a new build
	} {
		st.announce(step.id, step.v, 1, !step.legacy, voters)
		if st.version != step.want {
			t.Fatalf("step %d: version %d, want %d", i, st.version, step.want)
		}
	}
}

// formats returns the format of every entry with data in a node's memory.
func formats(n *Node) []byte {
	var out []byte
	for i := n.storage.first; ; i++ {
		d, ok := n.storage.entryData(i)
		if !ok {
			return out
		}
		switch {
		case len(d) == 0:
		case d[0] == 0:
			out = append(out, d[1])
		default:
			out = append(out, 1)
		}
	}
}

// TestVersionWaitsForEveryVoter runs node 3 as an older binary. The
// cluster stays at version 1, and every entry in format 1, until every
// node supports version 2, a rollback of node 2 included; then every node
// moves to version 2 and later entries are in format 2.
func TestVersionWaitsForEveryVoter(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) {
		cfg.MaxVersion = 2
		if cfg.ID == 3 {
			cfg.MaxVersion = 1
		}
	})
	id := uint64(0)
	// run ticks every node until announcements apply, then writes a key.
	run := func(want uint32) {
		t.Helper()
		for range 50 {
			for _, n := range c.nodes {
				n.Tick()
			}
			c.step()
		}
		id++
		c.nodes[0].Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id), Value: []byte("v")}})
		for range 5 {
			c.step()
		}
		for i, n := range c.nodes {
			if n.ActiveVersion() != want {
				t.Fatalf("node %d runs version %d, want %d", i+1, n.ActiveVersion(), want)
			}
		}
		fs := formats(c.nodes[0])
		if !slices.IsSorted(fs) || fs[len(fs)-1] != byte(want) {
			t.Fatalf("entry formats %v at version %d", fs, want)
		}
	}
	run(1)
	// A restart with another binary, as far as the announcement goes.
	c.nodes[1].cfg.MaxVersion = 1
	run(1)
	c.nodes[2].cfg.MaxVersion = 2
	run(1)
	c.nodes[1].cfg.MaxVersion = 2
	run(2)
	if c.replies != int(id) {
		t.Fatalf("%d of %d writes answered", c.replies, id)
	}
}

// TestOldBinaryRefusesToStart activates MaxVersion on a single node, then
// restarts it with a binary that supports only version 1: from the log
// alone and from a checkpoint cut after activation.
func TestOldBinaryRefusesToStart(t *testing.T) {
	for _, ckpt := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint=%v", ckpt), func(t *testing.T) {
			change := func(cfg *Config) {
				if ckpt {
					cfg.CheckpointBytes, cfg.CheckpointRatio = 2048, 2
				}
			}
			disk := simfs.New()
			s := startSingleWith(t, disk, 1, change)
			for range 20 {
				s.n.Tick()
				s.run()
			}
			if s.n.ActiveVersion() != MaxVersion {
				t.Fatalf("version %d after the announcement", s.n.ActiveVersion())
			}
			for id := uint64(1); ckpt && s.n.DurableCheckpoint().Index == 0; id++ {
				if r := s.do(id, &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 100)}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			if err := s.log.Close(); err != nil {
				t.Fatal(err)
			}
			w, rec, err := wal.Open(disk)
			if err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig(1, []uint64{1})
			change(&cfg)
			cfg.MaxVersion = 1
			_, err = New(cfg, rec, w)
			if !errors.Is(err, ErrTooOld) {
				t.Fatalf("the old binary started with %v", err)
			}
			if ckpt != strings.Contains(err.Error(), "load the checkpoint") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			t.Log(err)
		})
	}
}

// TestNewRunAnnounces restarts a node with the same version: it still
// announces, so that the state holds its new run and a late announcement
// of the run before cannot count (#174).
func TestNewRunAnnounces(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) { cfg.Run = 1 }).capable()
	if s.n.state.runs[1] != 1 {
		t.Fatalf("run 1 announced, the state holds run %d", s.n.state.runs[1])
	}
	s = startSingleWith(t, disk, 2, func(cfg *Config) { cfg.Run = 2 })
	for range 100 {
		s.n.Tick()
		s.run()
		if s.n.state.runs[1] == 2 {
			return
		}
	}
	t.Fatalf("the restarted node never announced run 2; the state holds run %d", s.n.state.runs[1])
}
