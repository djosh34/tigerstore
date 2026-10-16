package core

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// #174: node 2 stops before its rollback can announce. Node 3 upgrades
// while it is down. An earlier announcement is not consent to that upgrade.
func TestVersionRollbackBeforeAnnouncement(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) {
		cfg.Run, cfg.MaxVersion = 1, 2
		if cfg.ID == 3 {
			cfg.MaxVersion = 1
		}
	})
	run := func(want uint32) {
		t.Helper()
		for range 100 {
			for i, n := range c.nodes {
				if !c.down[i] {
					n.Tick()
				}
			}
			c.step()
		}
		for i, n := range c.nodes {
			if !c.down[i] && n.ActiveVersion() != want {
				t.Fatalf("node %d: version %d, want %d", i+1, n.ActiveVersion(), want)
			}
		}
	}
	run(1)
	if c.nodes[0].state.supports[2] != 2 {
		t.Fatal("node 2 did not announce support for version 2")
	}
	c.down[1] = true
	c.nodes[2].cfg.MaxVersion, c.nodes[2].cfg.Run = 2, 2
	run(1)
	if c.nodes[0].state.supportedVersion([]uint64{1, 2, 3}) != 2 {
		t.Fatal("the last upgrade did not announce")
	}
	// Select the rollback binary before allowing it to run again.
	c.nodes[1].cfg.MaxVersion, c.nodes[1].cfg.Run = 1, 2
	c.down[1] = false
	run(1)
	c.nodes[1].cfg.MaxVersion, c.nodes[1].cfg.Run = 2, 3
	run(2)
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("after"), Value: []byte("upgrade")}})
	for range 20 {
		c.step()
	}
	if c.replies != 1 {
		t.Fatalf("%d replies after activation, want 1", c.replies)
	}
}

func TestVersionConfirmations(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	voters := []uint64{1, 2, 3}
	for _, id := range voters {
		st.members[id] = &pb.Member{ID: id}
	}
	var index uint64
	apply := func(id uint64, v uint32, run, epoch uint64) {
		t.Helper()
		b, err := proto.Marshal(confirmation(v, run, epoch))
		if err != nil {
			t.Fatal(err)
		}
		n := &Node{state: st}
		index++
		if err := n.applyVersion(index, id, b); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range voters {
		apply(id, 2, 1, st.versionEpoch)
	}
	if st.version != 1 {
		t.Fatalf("support alone activated version %d", st.version)
	}
	epoch := st.versionEpoch
	apply(1, 2, 1, epoch)
	apply(2, 2, 1, epoch)
	// A restart, even with the same version, invalidates confirmations.
	apply(2, 2, 2, epoch)
	apply(3, 2, 1, epoch)
	if st.version != 1 {
		t.Fatal("a delayed confirmation survived a new run")
	}
	epoch = st.versionEpoch
	apply(1, 2, 1, epoch)
	apply(3, 2, 1, epoch)
	apply(2, 2, 1, epoch) // superseded run, even with the current epoch
	if st.version != 1 {
		t.Fatal("a superseded run confirmed the upgrade")
	}
	// A checkpoint must retain both the epoch and partial confirmations.
	h := st.header(10, 2)
	b, err := h.encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := readHeader(bufio.NewReader(bytes.NewReader(b)))
	if err != nil {
		t.Fatal(err)
	}
	if got.versionEpoch != epoch || got.confirmed[0] != epoch || got.confirmed[1] != 0 || got.confirmed[2] != epoch {
		t.Fatalf("checkpoint lost confirmations: epoch %d, confirmations %v", got.versionEpoch, got.confirmed)
	}
	st.kv.Freeze()
	for {
		part, _, done := st.kv.CopyFrozen(nil, 1024)
		b = append(b, part...)
		if done {
			break
		}
	}
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	restored, _, err := readCheckpoint(DefaultConfig(1, voters), bytes.NewReader(b), wal.Root{Index: 10, Term: 2}, &mvcc.Heap{})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Hash() != st.Hash() {
		t.Fatal("checkpoint reload changed the replicated state")
	}
	st = restored
	apply(2, 2, 2, epoch-1)
	if st.version != 1 {
		t.Fatal("checkpoint reload accepted an earlier epoch")
	}
	apply(2, 2, 2, epoch)
	if st.version != 2 {
		t.Fatalf("unanimous confirmation left version %d", st.version)
	}
}

// A log index gives a new epoch the same name even when recovery started
// from a checkpoint that did not store confirmation state.
func TestVersionEpochAfterLegacyRecovery(t *testing.T) {
	voters := []uint64{1, 2, 3}
	states := []*State{NewState(2, &mvcc.Heap{}), NewState(2, &mvcc.Heap{})}
	for _, st := range states {
		for _, id := range voters {
			st.applyMembers(confAdd, &pb.Member{ID: id}, id)
			v := uint32(2)
			if id == 3 {
				v = 1
			}
			st.announce(id, v, 1, true, voters)
		}
		if st.versionEpoch != 0 {
			t.Fatal("legacy replay invented confirmation state absent from old checkpoints")
		}
	}
	// Also recover safely from an earlier confirmation epoch. The next
	// run must replace it with the entry index, not add to a local count.
	states[0].versionEpoch = 17
	for _, st := range states {
		st.confirmVersion(100, 3, versionAnnouncement{version: 2, run: 2, hasRun: true, hasEpoch: true}, voters)
		if st.versionEpoch != 100 || st.version != 1 {
			t.Fatalf("new run: epoch %d, version %d", st.versionEpoch, st.version)
		}
	}
	if states[0].Hash() != states[1].Hash() {
		t.Fatal("new run did not reconcile recovered confirmation state")
	}
	// Epoch zero is not unanimous confirmation by missing map entries.
	st := NewState(2, &mvcc.Heap{})
	st.members[1], st.supports[1], st.runs[1] = &pb.Member{ID: 1}, 2, 1
	st.confirmVersion(101, 1, versionAnnouncement{version: 2, run: 1, hasRun: true, hasEpoch: true}, []uint64{1})
	if st.version != 1 || st.versionEpoch != 101 {
		t.Fatalf("zero epoch activated version %d, epoch %d", st.version, st.versionEpoch)
	}
}

func TestVersionMembershipInvalidatesConfirmations(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	st.versionEpoch = 4
	st.confirmed[1], st.confirmed[2] = 4, 4
	st.applyMembers(confAdd, &pb.Member{ID: 3}, 9)
	if st.versionEpoch != 9 || len(st.confirmed) != 0 {
		t.Fatalf("membership kept epoch %d and confirmations %v", st.versionEpoch, st.confirmed)
	}
}
