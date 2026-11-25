package core

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestStateAtHoldsItsLeasesKeys starts a state at a revision near the
// store's bound, with a lease near the largest ID: its revoke deletes the
// key that names it at the next revision, the lease sum matches a walk of
// the leases, and a key that names a lease it lacks fails the start
// (#502).
func TestStateAtHoldsItsLeasesKeys(t *testing.T) {
	const at, id = int64(1<<62 - 10), int64(1<<63 - 1)
	seeds := []mvcc.Seed{
		{Key: []byte("held"), Value: []byte("v"), Create: at - 3, Mod: at - 3, Version: 1, Lease: id},
		{Key: []byte("kept"), Value: []byte("w"), Create: at, Mod: at, Version: 1},
	}
	st, err := NewStateAt(2, &mvcc.Heap{}, at, seeds, map[int64]int64{id: 60, id - 1: 30})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := st.leaseSum, st.sumLeases(); got != want {
		t.Fatalf("lease sum %x, the leases add up to %x", got, want)
	}
	if _, err := st.Apply(&pb.LeaseRevokeRequest{ID: id}); err != nil {
		t.Fatal(err)
	}
	all, err := st.Store().Range(&pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	if st.Store().Rev() != at+1 || len(all.Kvs) != 1 || string(all.Kvs[0].Key) != "kept" {
		t.Fatalf("after the revoke: revision %d, keys %v; want %d and only kept", st.Store().Rev(), all.Kvs, at+1)
	}
	if _, err := NewStateAt(2, &mvcc.Heap{}, at, seeds, nil); err == nil {
		t.Fatal("a key naming a lease the state lacks started")
	}
}

// TestLeaseSumKept checks that the running sum of the leases' hashes
// matches a walk of them after every write that touches a lease, and
// after a checkpoint reload.
func TestLeaseSumKept(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	st := NewState(2, &mvcc.Heap{})
	key := func() []byte { return fmt.Appendf(nil, "k%d", rng.IntN(8)) }
	id := func() int64 { return 1 + rng.Int64N(4) }
	for i := range 2000 {
		var op proto.Message
		switch rng.IntN(7) {
		case 0:
			op = &pb.LeaseGrantRequest{ID: id(), TTL: 10}
		case 1:
			op = &pb.LeaseRevokeRequest{ID: id()}
		case 2:
			op = &pb.LeaseKeepAliveRequest{ID: id()}
		case 3:
			op = &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: id(), Remaining_TTL: rng.Int64N(10)}}}
		case 4:
			op = &pb.DeleteRangeRequest{Key: key()}
		case 5:
			l := id()
			if st.leases[l] != nil {
				op = &pb.LeaseCheckpoint{ID: l, Remaining_TTL: st.leases[l].stamp}
			} else {
				op = &pb.PutRequest{Key: key()}
			}
		default:
			op = &pb.PutRequest{Key: key(), Value: []byte("v"), Lease: id()}
		}
		// Writes on a missing lease and grants of one that exists fail,
		// with etcd's answers, and leave the state unchanged.
		if _, err := st.Apply(op); err != nil && !errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) && !errors.Is(err, rpctypes.ErrGRPCLeaseExist) {
			t.Fatalf("write %d (%T): %v", i, op, err)
		}
		if got, want := st.leaseSum, st.sumLeases(); got != want {
			t.Fatalf("write %d (%T): lease sum %x, the leases add up to %x", i, op, got, want)
		}
	}
	if len(st.leases) == 0 {
		t.Fatal("no lease was left to reload")
	}
	b, err := st.header(10, 2).encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readHeader(bufio.NewReader(bytes.NewReader(b))); err != nil {
		t.Fatal(err)
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
	restored, _, err := readCheckpoint(DefaultConfig(1, []uint64{1}), bytes.NewReader(b), wal.Root{Index: 10, Term: 2}, &mvcc.Heap{})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Hash() != st.Hash() {
		t.Error("a checkpoint reload changed the state hash")
	}
	if got, want := restored.leaseSum, restored.sumLeases(); got != want {
		t.Errorf("after a reload: lease sum %x, the leases add up to %x", got, want)
	}
}
