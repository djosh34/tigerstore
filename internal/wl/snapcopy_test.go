package wl

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// An open checkpoint file does not hold the log its values name. A
// same-index cut may free that log while a client copies it; the failed
// copy must retry with a fresh checkpoint, not fail the backup or node.
func TestAClientSnapshotRetriesACopyWhoseLogWasCut(t *testing.T) {
	n, value, _ := lowRingCheckpoint(t)
	src, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	if err := selfContained(src, n.wal, &bytes.Buffer{}); err != nil {
		t.Fatalf("before the forced cut: %v", err)
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	f, err := n.sys.createTemp(n.ckptDir, "client-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	copyErr := selfContained(src, n.wal, f)
	if !errors.Is(copyErr, errLogCut) {
		t.Fatalf("the fixture did not cut the log the held-open file names: %v", copyErr)
	}
	cs := &clientSnap{h: noHandle, f: f}
	if err := n.clientSnapCopied(cs, src.Name(), copyErr); err != nil {
		t.Fatalf("the cut failed the client's backup: %v", err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("the failed copy stayed open: %v", err)
	}
	if len(n.clientSnaps) != 0 || len(n.snapWaiters) != 1 || n.snapWaiters[0] != noHandle || !n.storage.wantCkpt {
		t.Fatal("the failed copy was not queued for a fresh checkpoint")
	}
	// Even without another apply or ring pressure, the explicit retry
	// must make a checkpoint for the same waiter, at the same index.
	if n.ringDue() {
		t.Fatal("the forced cut should have cleared the ring pressure")
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt == nil || n.ckpt.meta.index != 14 || len(n.ckpt.snapWaiters) != 1 || n.ckpt.snapWaiters[0] != noHandle {
		t.Fatal("the retry did not start at the same index for the waiting client")
	}
	meta := n.ckpt.meta
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	replacement, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	f, err = n.sys.createTemp(n.ckptDir, "client-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := selfContained(replacement, n.wal, f); err != nil {
		t.Fatalf("copying the replacement: %v", err)
	}
	size, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loaded.close() }()
	if _, err := loadCkpt(f, 0, loaded); err != nil {
		t.Fatal(err)
	}
	if got, ok := valueAt(t, loaded, "/kept", loaded.rev); !ok || got != value {
		t.Fatal("the retried copy lost the value whose segment was cut")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	cs = &clientSnap{h: noHandle, f: f, head: clientSnapHead(n.clusterID, meta), size: size}
	if err := n.clientSnapCopied(cs, replacement.Name(), nil); err != nil || len(n.clientSnaps) != 1 {
		t.Fatalf("the retried copy was not ready to send: %v", err)
	}
	// Only copying a replaced source retries. Current corruption must
	// still stop the node, and temp-file failures must not loop.
	if err := n.clientSnapCopied(&clientSnap{h: noHandle}, replacement.Name(), ErrBadValue); !errors.Is(err, ErrBadValue) {
		t.Fatalf("the current checkpoint's damaged value was hidden: %v", err)
	}
	if err := n.clientSnapCopied(&clientSnap{h: noHandle}, src.Name(), io.ErrUnexpectedEOF); err != nil || len(n.snapWaiters) != 0 {
		t.Fatalf("an error outside the copy was retried: %v", err)
	}
}

// Raft already retries a failed send with its newest snapshot. A retired
// source may have read reused ring bytes; its checksum failure must be
// reported as a failed send rather than stopping the node. The current
// checkpoint's checksum failure must still stop it.
func TestAPeerSnapshotRetriesAReplacedCopyWithABadValue(t *testing.T) {
	n, _, _ := lowRingCheckpoint(t)
	before := n.ckptName
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	n.storage.cs = n.confState
	n.storage.hs = &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(14))}
	cfg := raftConfig(1, n.storage, sizeFor(0), 3)
	cfg.Applied = n.applied
	var err error
	if n.rn, err = raft.NewRawNode(cfg); err != nil {
		t.Fatal(err)
	}
	if err := n.snapshotSent(2, filepath.Join(n.ckptDir, before), ErrBadValue); err != nil {
		t.Fatalf("the retired checkpoint's copy stopped the node: %v", err)
	}
	if err := n.snapshotSent(2, filepath.Join(n.ckptDir, n.ckptName), ErrBadValue); !errors.Is(err, ErrBadValue) {
		t.Fatalf("the current checkpoint's damaged value was hidden: %v", err)
	}
}
