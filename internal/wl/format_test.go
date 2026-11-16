package wl

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// gateNode is a node of three members at the cluster's first format,
// which each announced in run 1 and confirmed.
func gateNode(t *testing.T) *node {
	t.Helper()
	n, _ := testNode(t)
	n.format, n.formatEpoch = baseFormat, 1
	for id := uint64(1); id <= 3; id++ {
		n.members = append(n.members, member{id: id, format: baseFormat, run: 1, confirmed: 1})
	}
	return n
}

// announce applies member id's announcement of format in run, confirming
// epoch, at index.
func announce(t *testing.T, n *node, index, id uint64, format uint8, run, epoch uint64) error {
	t.Helper()
	return n.applyMemberAttrs(appendAnnouncement(nil, &member{id: id, format: format, run: run}, epoch), index)
}

// supportAndConfirm has every member announce this binary's newest
// format in run 2, then confirm the epoch that left.
func supportAndConfirm(t *testing.T, n *node, index *uint64) error {
	t.Helper()
	const format = maxFormat
	for id := uint64(1); id <= 3; id++ {
		*index++
		if err := announce(t, n, *index, id, format, 2, 0); err != nil {
			return err
		}
	}
	epoch := n.formatEpoch
	for id := uint64(1); id <= 3; id++ {
		*index++
		if err := announce(t, n, *index, id, format, 2, epoch); err != nil {
			return err
		}
	}
	return nil
}

func TestTheClusterTakesAFormatOnceEveryMemberConfirmedIt(t *testing.T) {
	n := gateNode(t)
	index := uint64(10)
	if err := supportAndConfirm(t, n, &index); err != nil {
		t.Fatal(err)
	}
	if n.format != maxFormat {
		t.Fatalf("the cluster uses format %d, want %d", n.format, maxFormat)
	}
	// A newer format than this binary reads: the binary stops, but only
	// once every member confirmed it.
	epoch := n.formatEpoch
	for id := uint64(1); id <= 3; id++ {
		index++
		if err := announce(t, n, index, id, maxFormat+1, 3, 0); err != nil {
			t.Fatalf("member %d's support of format %d: %v", id, maxFormat+1, err)
		}
	}
	if n.formatEpoch == epoch {
		t.Fatal("a change of support left the epoch")
	}
	epoch = n.formatEpoch
	for id := uint64(1); id <= 2; id++ {
		index++
		if err := announce(t, n, index, id, maxFormat+1, 3, epoch); err != nil {
			t.Fatalf("member %d's confirmation: %v", id, err)
		}
	}
	index++
	if err := announce(t, n, index, 3, maxFormat+1, 3, epoch); !errors.Is(err, ErrTooOld) {
		t.Fatalf("the last confirmation of format %d gave %v, want ErrTooOld", maxFormat+1, err)
	}
}

// #174: a member that supported a newer format and is down when the last
// member upgrades may come back on an older binary. Its earlier support
// is not consent.
func TestAMemberThatIsDownHoldsTheFormat(t *testing.T) {
	n := gateNode(t)
	index := uint64(10)
	// Members 1 and 2 upgrade; then 2 goes down and 3 upgrades.
	for _, id := range []uint64{1, 2} {
		index++
		if err := announce(t, n, index, id, maxFormat+1, 2, 0); err != nil {
			t.Fatal(err)
		}
	}
	index++
	if err := announce(t, n, index, 3, maxFormat+1, 2, 0); err != nil {
		t.Fatal(err)
	}
	epoch := n.formatEpoch
	for _, id := range []uint64{1, 3} {
		index++
		if err := announce(t, n, index, id, maxFormat+1, 2, epoch); err != nil {
			t.Fatalf("member %d's confirmation without member 2's: %v", id, err)
		}
	}
	if n.format != baseFormat {
		t.Fatalf("the cluster took format %d while member 2 was down", n.format)
	}
	// Member 2 comes back on the old binary, in a later run.
	index++
	if err := announce(t, n, index, 2, baseFormat, 3, n.formatEpoch); err != nil {
		t.Fatal(err)
	}
	if n.supportedFormat() != baseFormat || n.format != baseFormat {
		t.Fatalf("after the rollback: supported %d, the cluster's %d", n.supportedFormat(), n.format)
	}
}

func TestAStaleRunsAnnouncementChangesNothing(t *testing.T) {
	n := gateNode(t)
	index := uint64(10)
	index++
	if err := announce(t, n, index, 2, baseFormat, 5, 0); err != nil {
		t.Fatal(err)
	}
	epoch := n.formatEpoch
	// Run 4's support of a newer format, delayed past run 5's.
	index++
	if err := announce(t, n, index, 2, maxFormat+1, 4, epoch); err != nil {
		t.Fatal(err)
	}
	if m := n.members[1]; m.format != baseFormat || m.run != 5 || n.formatEpoch != epoch {
		t.Fatalf("a stale run's announcement changed member 2 to format %d run %d, epoch %d to %d", m.format, m.run, epoch, n.formatEpoch)
	}
}

// newTestRawNode is a Raft node 1 of voters 1, 2 and 3.
func newTestRawNode(t *testing.T) *raft.RawNode {
	t.Helper()
	ms := raft.NewMemoryStorage()
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	if err := ms.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	rn, err := raft.NewRawNode(raftConfig(1, ms, sizeFor(0), 3))
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func TestAMembershipChangeAsksForConfirmationsAgain(t *testing.T) {
	n := gateNode(t)
	n.rn = newTestRawNode(t)
	index := uint64(10)
	epoch := n.formatEpoch
	cc := &pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(3)), Context: ccContext(0, 0, &member{id: 3})}
	index++
	if err := n.applyConfChange(cc, index); err != nil {
		t.Fatal(err)
	}
	if n.formatEpoch == epoch {
		t.Fatal("a membership change left the epoch")
	}
}

func TestACheckpointKeepsTheGate(t *testing.T) {
	n := gateNode(t)
	index := uint64(10)
	if err := supportAndConfirm(t, n, &index); err != nil {
		t.Fatal(err)
	}
	b := n.encodeMembers(nil)
	cm, err := (&ckptReader{r: bufio.NewReader(bytes.NewReader(b))}).readMembers()
	if err != nil {
		t.Fatal(err)
	}
	if cm.format != n.format || cm.formatEpoch != n.formatEpoch || len(cm.members) != 3 {
		t.Fatalf("read back format %d epoch %d with %d members, want %d %d 3", cm.format, cm.formatEpoch, len(cm.members), n.format, n.formatEpoch)
	}
	for i, m := range cm.members {
		if w := n.members[i]; m.format != w.format || m.run != w.run || m.confirmed != w.confirmed {
			t.Fatalf("member %d read back as %+v, want %+v", m.id, m, w)
		}
	}
	r := gateNode(t)
	if err := r.takeMembers(&cm); err != nil {
		t.Fatal(err)
	}
	cm.format = maxFormat + 1
	if err := r.takeMembers(&cm); !errors.Is(err, ErrTooOld) {
		t.Fatalf("a checkpoint of format %d gave %v, want ErrTooOld", maxFormat+1, err)
	}
}

func TestTheApplyChecksABatchsFormat(t *testing.T) {
	n := gateNode(t)
	if err := n.checkFormat(baseFormat); err != nil {
		t.Fatal(err)
	}
	if err := n.checkFormat(maxFormat + 1); !errors.Is(err, ErrTooOld) {
		t.Fatalf("a batch past this binary's formats gave %v, want ErrTooOld", err)
	}
	if maxFormat > baseFormat {
		if err := n.checkFormat(maxFormat); !errors.Is(err, errFormatAhead) {
			t.Fatalf("a batch ahead of the cluster's format gave %v, want errFormatAhead", err)
		}
	}
}

func TestEachStartHasALaterRun(t *testing.T) {
	dir := t.TempDir()
	for want := uint64(1); want <= 3; want++ {
		run, err := nextRun(sysEnv{}, dir)
		if err != nil || run != want {
			t.Fatalf("start %d has run %d (%v)", want, run, err)
		}
	}
}

// namedAppend is a peer frame of an append from node 1 with one entry
// that names the batch seq of origin.
func namedAppend(origin, seq uint64) []byte {
	f := make([]byte, msgHdr)
	f[0] = byte(pb.MsgApp)
	binary.LittleEndian.PutUint64(f[2:], 1)
	binary.LittleEndian.PutUint32(f[58:], 1)
	var eh [21]byte
	eh[16] = byte(pb.EntryNormal) | namedEntry
	binary.LittleEndian.PutUint32(eh[17:], uint32(seq))
	f = append(f, eh[:]...)
	f = binary.LittleEndian.AppendUint32(f, uint32(origin))
	f = binary.LittleEndian.AppendUint32(f, uint32(seq>>32))
	return binary.LittleEndian.AppendUint32(f, 0) // no context
}

func TestEachRunNamesItsBatchesAfresh(t *testing.T) {
	var names []uint64
	for run := uint64(1); run <= 2; run++ {
		n, _ := testNode(t)
		n.startRun(run)
		n.addOp(noHandle, kPut, []byte("op"))
		_, seq, ok := batchOrigin(n.batch)
		if !ok {
			t.Fatal("the open batch is not a batch")
		}
		names = append(names, seq)
	}
	if names[0] == names[1] {
		t.Fatalf("runs 1 and 2 both named their first batch %#x", names[0])
	}
}

// A node's batch counts start again at each start: a named append of an
// earlier run must not find the batch of a later run that has the same
// count.
func TestANamedBatchOfAnEarlierRunIsNotALatersBatch(t *testing.T) {
	n, _ := testNode(t)
	const count = 5
	later := uint64(2)<<32 | count
	n.fwdOf(2)[later&(fwdSlots-1)] = fwdBatch{seq: later, data: []byte("run 2's batch")}
	if _, err := n.decodeMsg(namedAppend(2, uint64(1)<<32|count)); !errors.Is(err, errUnnamed) {
		t.Fatalf("an append naming run 1's batch %d gave %v, want errUnnamed", count, err)
	}
	m, err := n.decodeMsg(namedAppend(2, later))
	if err != nil || string(m.GetEntries()[0].GetData()) != "run 2's batch" {
		t.Fatalf("an append naming run 2's batch: %v", err)
	}
}

// A follower that restarted lacks the batches it proposed before: once it
// asks for batches to be carried, the leader carries its own to it too.
func TestALeaderCarriesAFollowersOwnBatchWhenAsked(t *testing.T) {
	n, _ := testNode(t)
	b := AppendBatch(nil, Batch{Origin: 2, Seq: 1<<32 | 7, Format: baseFormat})
	if !n.names(b, false) {
		t.Fatal("a follower's batch is carried while no peer asked for that")
	}
	if n.names(b, true) {
		t.Fatal("the leader names node 2's batch to node 2 after it asked for batches to be carried")
	}
}
