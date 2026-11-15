package wl

import (
	"bytes"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newTestRing(t *testing.T) *ring {
	t.Helper()
	r, err := newRing(4096)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// put writes p into the ring as a held span, or reports that the ring has
// no room.
func put(r *ring, p string) ([]byte, bool) {
	room := r.reserve(len(p))
	if room == nil {
		return nil, false
	}
	room = slices.Clip(append(room, p...))
	r.commit(room, span{kind: spanUnnamed})
	return room, true
}

func TestRingNeverOverwritesAHeldSpan(t *testing.T) {
	r := newTestRing(t)
	piece := string(bytes.Repeat([]byte("a"), 1000))
	var held [][]byte
	for {
		d, ok := put(r, piece)
		if !ok {
			break
		}
		held = append(held, d)
	}
	if len(held) != 4 {
		t.Fatalf("the ring took %d pieces of 1000 bytes in 4096, want 4", len(held))
	}
	// Letting go of the first makes room for one more, at the start.
	r.sweep(func(s *span) bool { return s.start == 0 })
	d, ok := put(r, string(bytes.Repeat([]byte("b"), 1000)))
	if !ok || r.offset(d) != 0 {
		t.Fatalf("after the first span went, a piece went at %d (%v), want 0", r.offset(d), ok)
	}
	for i, h := range held[1:] {
		if !bytes.Equal(h, []byte(piece)) {
			t.Fatalf("held piece %d was overwritten", i+1)
		}
	}
	if _, ok := put(r, "c"); ok {
		t.Fatal("the ring took a piece past its held spans")
	}
}

func TestRingFindsSpansAcrossLaps(t *testing.T) {
	r := newTestRing(t)
	put(r, string(bytes.Repeat([]byte("a"), 3000)))
	r.sweep(func(*span) bool { return true })
	b, _ := put(r, string(bytes.Repeat([]byte("b"), 2000))) // wraps to the start
	c, _ := put(r, "c")
	for _, p := range [][]byte{b, c} {
		sp := r.find(p)
		if sp == nil || sp.end-sp.start != int64(len(p)) {
			t.Fatalf("span of a %d-byte piece: %+v", len(p), sp)
		}
	}
	if sp := r.find(b); sp.start != 4096 {
		t.Fatalf("the piece after the wrap starts at %d, want 4096", sp.start)
	}
	if r.find([]byte("elsewhere")) != nil {
		t.Fatal("found a span for bytes outside the ring")
	}
}

func TestRingReserveKeepsItsStartUntilCommit(t *testing.T) {
	r := newTestRing(t)
	first := r.reserve(10)
	again := r.reserve(100)
	if r.offset(first) != r.offset(again) {
		t.Fatalf("a second reserve moved from %d to %d", r.offset(first), r.offset(again))
	}
}

// pushBehind writes more than half a ring after what r holds, so that a
// sweep looks at what came before.
func pushBehind(t *testing.T, r *ring) {
	t.Helper()
	n := len(r.mem)/2 + 1
	room := r.reserve(n)
	if room == nil {
		t.Fatal("no room to push the ring's spans behind")
	}
	r.commit(room[:n], span{kind: spanAppend}) // held until the test ends
}

// testNode is a node with the parts the rings' sweep looks at.
func testNode(t *testing.T) (*node, *memLog) {
	t.Helper()
	l := &memLog{}
	n := &node{id: 1, storage: newLogStorage(l), wal: &wal{}, fwd: make([]fwdBatch, fwdSlots), fwdPeers: map[uint64][]fwdBatch{}, metrics: &nodeMetrics{}, auth: newAuthStore()}
	n.ring, n.rx, n.side = newTestRing(t), newTestRing(t), newTestRing(t)
	return n, l
}

func TestSweepKeepsRecentSpans(t *testing.T) {
	n, _ := testNode(t)
	room := n.side.reserve(4)
	n.side.commit(append(room, "fail"...), span{kind: spanFree})
	n.sweepRings(time.Now())
	if n.side.head != 0 {
		t.Fatal("a span less than half a lap behind was let go")
	}
	pushBehind(t, n.side)
	n.sweepRings(time.Now())
	if n.side.head != 1 {
		t.Fatal("a free span half a lap behind was kept")
	}
}

func TestSweepLetsGoOfStableEntriesOnly(t *testing.T) {
	n, l := testNode(t)
	now := time.Now()
	d, _ := put(n.rx, "entry one")
	pushBehind(t, n.rx)
	e := &pb.Entry{Index: new(uint64(1)), Term: new(uint64(1)), Data: d}
	n.nameEntries([]*pb.Entry{e}, spanAppend)
	n.sweepRings(now)
	if n.rx.head != 0 {
		t.Fatal("a span in a waiting append was let go")
	}
	if err := n.storage.append([]*pb.Entry{e}, []int64{l.add("entry one")}); err != nil {
		t.Fatal(err)
	}
	n.nameEntries([]*pb.Entry{e}, spanStored)
	n.sweepRings(now)
	if n.rx.head != 0 {
		t.Fatal("an entry was let go before its append's responses were stepped")
	}
	n.wal.stable = int64(len(l.b))
	n.sweepRings(now)
	if n.rx.head != 1 {
		t.Fatal("a stable entry was not let go")
	}
	ents, err := n.storage.Entries(1, 2, 1<<20)
	if err != nil || string(ents[0].GetData()) != "entry one" || n.storage.stats.reads != 1 {
		t.Fatalf("the entry let go reads back as %q (%v), %d reads", ents[0].GetData(), err, n.storage.stats.reads)
	}
}

func TestSweepDropsAnUnnamedBatchFromItsForwardTable(t *testing.T) {
	n, _ := testNode(t)
	now := time.Now()
	room := n.rx.reserve(5)
	room = append(room, "batch"...)
	d := room
	n.rx.commit(d, span{kind: spanUnnamed, at: now, fwd: true, origin: 2, seq: 7})
	pushBehind(t, n.rx)
	f := &n.fwdOf(2)[7]
	f.seq, f.data = 7, d
	n.sweepRings(now.Add(unnamedFor / 2))
	if f.data == nil || n.rx.head != 0 {
		t.Fatal("a batch was let go before its time")
	}
	n.sweepRings(now.Add(unnamedFor))
	if f.data != nil || n.rx.head != 1 {
		t.Fatal("a batch no append named was kept past its time, or its forward slot still names it")
	}
}

func TestSweepKeepsAFailureBranchWhileItsStreamHasIt(t *testing.T) {
	n, _ := testNode(t)
	s := &stream{}
	room := n.side.reserve(4)
	room = append(room, "fail"...)
	s.fail = room
	s.failAt = n.side.commit(s.fail, span{kind: spanFail, st: s}).start
	pushBehind(t, n.side)
	n.sweepRings(time.Now())
	if n.side.head != 0 {
		t.Fatal("a failure branch its stream still has was let go")
	}
	s.fail = nil
	n.sweepRings(time.Now())
	if n.side.head != 1 {
		t.Fatal("a failure branch was kept after its stream let it go")
	}
}
