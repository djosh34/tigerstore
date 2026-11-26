package wl

import (
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A follower may apply no further than its newest checkpoint while the
// next append has no room. If that checkpoint cut nothing, another at
// its own index must take in the values and force the cut (#815).
func TestALowRingWhoseNewestCheckpointCutNothingCheckpointsAgain(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	n, _ := ckptNode(t)
	w, err := openWALRing(sysEnv{}, t.TempDir(), 256<<10, 8*segSize, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	n.wal, n.st.log, n.storage.log = w, w, w
	n.ckptLog = 1 << 30
	value := strings.Repeat("v", int(3*segSize))
	ents := []*pb.Entry{testEntry(1, 2, value)}
	for i := uint64(2); i <= 14; i++ {
		ents = append(ents, testEntry(i, 2, ""))
	}
	if err := w.write(ents, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	if err := n.storage.append(ents, w.offs); err != nil {
		t.Fatal(err)
	}
	id, err := n.st.intern([]byte("/kept"))
	if err != nil {
		t.Fatal(err)
	}
	n.st.rev++
	if _, err := n.st.put(id, []byte(value), 0, n.st.rev, w.offs[0], true); err != nil {
		t.Fatal(err)
	}
	n.st.endApply()
	n.applied = 14
	from := n.storage.offOf(n.applied) - walEntHdr
	if w.ringLow() {
		t.Fatal("the first checkpoint must start before the ring runs low")
	}
	if err := n.startCkpt(14, 2, from); err != nil {
		t.Fatal(err)
	}
	if n.ckpt.force {
		t.Fatal("the first checkpoint was forced")
	}
	// The follower accepts uncommitted entries while the checkpoint is
	// written, then has too little room for the next entry of 3 segments.
	ents = []*pb.Entry{testEntry(15, 2, strings.Repeat("u", int(2*segSize)))}
	if err := w.write(ents, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	if err := n.storage.append(ents, w.offs); err != nil {
		t.Fatal(err)
	}
	w.wanted = 3 * segSize
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	if !w.ringLow() || w.ringRoom() >= w.wanted || n.ckptCut != 0 || n.applied != n.storage.snap.GetMetadata().GetIndex() {
		t.Fatalf("not the wedge: low %v, room %d, wanted %d, cut %d, applied %d, snapshot %v", w.ringLow(), w.ringRoom(), w.wanted, n.ckptCut, n.applied, n.storage.snap)
	}
	if n.storage.offOf(n.applied) >= 0 {
		t.Fatal("the test needs the checkpoint's entry compacted")
	}
	if !n.ringDue() {
		t.Fatal("no checkpoint is due at the newest's own index after its cut freed nothing")
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt == nil || !n.ckpt.force || n.ckpt.logFrom != from || n.ckpt.meta.index != 14 || n.ckpt.meta.term != 2 {
		t.Fatalf("the same-index checkpoint was not forced at its saved place: %+v", n.ckpt)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	if w.ringLow() || w.ringRoom() < w.wanted || w.rbase != from/segSize {
		t.Fatalf("the forced checkpoint freed no room: low %v, room %d, wanted %d, base %d, start %d", w.ringLow(), w.ringRoom(), w.wanted, w.rbase, from)
	}
	if got, ok := valueAt(t, n.st, "/kept", n.st.rev); !ok || got != value {
		t.Fatal("the cut lost the value before it")
	}
	// Even if unapplied appends keep the ring low and the byte threshold
	// passed, the forced cut can free nothing more at this index.
	w.wanted = w.ringRoom() + 1
	if n.ringDue() {
		t.Fatal("another checkpoint is due after forcing the cut to its start")
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt != nil {
		n.abortCkpt()
		t.Fatal("the node started a useless checkpoint at the same index")
	}
}
