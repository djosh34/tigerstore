package wl

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// lowRingCheckpoint is #815's wedge: applied 14, newest checkpoint 14,
// cut 0 and a low ring with no room for the next append. It returns the
// node, its value and the record the checkpoint starts at.
func lowRingCheckpoint(t *testing.T) (*node, string, int64) {
	t.Helper()
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
	return n, value, from
}

// A follower may apply no further than its newest checkpoint while the
// next append has no room. If that checkpoint cut nothing, another at
// its own index must take in the values and force the cut (#815).
func TestALowRingWhoseNewestCheckpointCutNothingCheckpointsAgain(t *testing.T) {
	n, value, from := lowRingCheckpoint(t)
	w := n.wal
	id, _ := n.st.find([]byte("/kept"))
	// A sender may still hold the old checkpoint open. Its values lie
	// in the WAL, so an open file alone does not protect their segments.
	src, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	if err := selfContained(src, w, &bytes.Buffer{}); err != nil {
		t.Fatalf("copy before the forced cut: %v", err)
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
	// Read from the repointed checkpoint, not the Put's cached record.
	n.st.fit()
	if k := &n.st.keys[id]; k.rec != 0 {
		n.st.slab.free(k.rec, int(k.rlen))
		k.rec, k.rlen = 0, 0
	}
	if got, ok := valueAt(t, n.st, "/kept", n.st.rev); !ok || got != value {
		t.Fatal("the cut lost the value before it")
	}
	// As on main, a copy whose old file names a retired segment fails
	// cleanly, rather than returning a wrong value or hanging. Retrying
	// such copies belongs to #863, not the same-index checkpoint fix.
	copied := make(chan error, 1)
	go func() { copied <- selfContained(src, w, &bytes.Buffer{}) }()
	select {
	case err := <-copied:
		if err == nil || !errors.Is(err, ErrBadValue) && !strings.Contains(err.Error(), "does not hold") {
			t.Fatalf("the retired file's copy did not fail cleanly: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retired file's copy hung after its segment was cut")
	}
	// Even if unapplied appends keep the ring low and the byte threshold
	// passed, the forced cut can free nothing more at this index.
	w.wanted = w.ringRoom() + 1
	n.ckptLog = 1
	n.now = n.ckptNotBefore.Add(time.Second)
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
