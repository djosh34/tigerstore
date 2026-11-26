package wl

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A client's snapshot (Maintenance.Snapshot) is tigerstore's format, as
// main's node writes it:
//
//	magic "tigersnp" | u32 format version | u64 cluster ID | u64 index |
//	u64 term | i64 revision | checkpoint | zero padding | SHA-256
//
// integers big-endian. The checkpoint is one the node takes for it, with
// every value in it. The padding makes the file up to the digest a
// multiple of 512 bytes, and the digest covers everything before it, so
// the file ends as etcd's does and `etcdctl snapshot save` accepts it.
// `tigerstore snapshot restore` makes a data dir of it (Restore).

const (
	clientSnapMagic   = "tigersnp"
	clientSnapVersion = 2
	clientSnapHeader  = len(clientSnapMagic) + 4 + 4*8
	clientSnapChunk   = 1 << 20
	// A busy ring must not keep one Snapshot retrying on end.
	clientSnapRetries = 3
)

// clientSnap is a client's snapshot being sent, a chunk a turn while its
// stream's window takes it.
type clientSnap struct {
	h          handle
	f          file // the checkpoint with every value in it
	head       []byte
	size, sent int64 // the whole file's, digest and all
	sum        hash.Hash
	buf        []byte
}

// snapshotRequest takes a client's Snapshot: the node takes a checkpoint
// for it, and sends it once written.
func (n *node) snapshotRequest(c *client, s *stream) {
	if s.snapRequested {
		n.replyError(c, s.id, codeInvalidArgument, "etcdserver: snapshot: only one request per stream")
		return
	}
	s.snapRequested = true
	n.snapWaiters = append(n.snapWaiters, mkHandle(c, s.id))
}

// pruneSnapWaiters drops clients that went away after asking for a
// checkpoint, without changing Raft's independent wantCkpt demand.
func (n *node) pruneSnapWaiters() {
	k := 0
	for _, h := range n.snapWaiters {
		if c, _ := n.clientOf(h); c != nil {
			n.snapWaiters[k] = h
			k++
		}
	}
	clear(n.snapWaiters[k:])
	n.snapWaiters = n.snapWaiters[:k]
}

// startClientSnaps hands the checkpoint just written to the clients that
// asked before it started: a goroutine writes each one's copy with every
// value, and posts it to the loop to send.
func (n *node) startClientSnaps(waiters []handle, name string) {
	if len(waiters) == 0 || n.shell == nil {
		return
	}
	for _, h := range waiters {
		if c, _ := n.clientOf(h); c == nil {
			continue
		}
		src, err := n.sys.openFile(filepath.Join(n.ckptDir, name), os.O_RDONLY, 0)
		if err != nil {
			n.replyErrorH(h, codeInternal, err.Error())
			continue
		}
		n.shell.prepareClientSnap(h, src)
	}
}

func clientSnapHead(cluster uint64, m ckptMeta) []byte {
	b := append([]byte(clientSnapMagic), 0, 0, 0, clientSnapVersion)
	b = binary.BigEndian.AppendUint64(b, cluster)
	b = binary.BigEndian.AppendUint64(b, m.index)
	b = binary.BigEndian.AppendUint64(b, m.term)
	return binary.BigEndian.AppendUint64(b, uint64(m.rev))
}

// prepareClientSnap writes the copy in a goroutine of its own and posts
// it to the loop, which sends it.
func (l *loop) prepareClientSnap(h handle, src file) {
	strays.Add(1)
	go func() {
		defer strays.Add(-1)
		defer func() { _ = src.Close() }()
		f, err := l.n.sys.createTemp(l.n.ckptDir, "client-*.tmp")
		if err == nil {
			// The name goes now; the file stays open until it is sent.
			_ = l.n.sys.remove(f.Name())
			err = selfContained(src, l.n.wal, f)
		}
		cs := &clientSnap{h: h, f: f}
		if err == nil {
			cs.size, err = f.Seek(0, io.SeekCurrent)
		}
		if err == nil {
			_, err = f.Seek(0, io.SeekStart)
		}
		if err == nil {
			// The verified copy is the authority for its header, even
			// when the caller reused the newest quiet checkpoint.
			cs.head, err = copiedClientSnapHead(f, l.n.clusterID)
		}
		if err == nil {
			_, err = f.Seek(0, io.SeekStart)
		}
		l.post(func() error { return l.n.clientSnapCopied(cs, err) })
	}()
}

// copiedClientSnapHead reads the metadata of a verified, self-contained
// checkpoint, not the store whose revision may since have changed.
func copiedClientSnapHead(f file, cluster uint64) ([]byte, error) {
	cr := &ckptReader{r: bufio.NewReader(f)}
	if err := cr.skip(uint64(len(ckptMagic))); err != nil {
		return nil, err
	}
	var v [3]uint64
	for i := range v {
		var err error
		if v[i], err = cr.uvarint(); err != nil {
			return nil, err
		}
	}
	return clientSnapHead(cluster, ckptMeta{index: v[0], term: v[1], rev: int64(v[2])}), nil
}

// snapshotDamage reports errors that must stop the node, not retry a
// copy or report a failed send to Raft as if the disk were sound.
func snapshotDamage(err error) bool {
	return errors.Is(err, ErrBadValue) || errors.Is(err, ErrCorruptCheckpoint) || errors.Is(err, ErrCorruptLog)
}

// clientSnapCopied takes back a client's copy. Only a retired log segment
// retries through a fresh checkpoint. A replaced checkpoint can still
// name live log bytes: a bad value is damage, whatever the file's name.
// Retries belong to a live stream, and stop once its limit is reached.
func (n *node) clientSnapCopied(cs *clientSnap, err error) error {
	c, s := n.clientOf(cs.h)
	if err != nil {
		if cs.f != nil {
			_ = cs.f.Close()
		}
		if snapshotDamage(err) {
			n.replyErrorH(cs.h, codeInternal, fmt.Sprintf("etcdserver: snapshot: %v", err))
			return err
		}
		if c == nil {
			return nil
		}
		if errors.Is(err, errLogCut) {
			if s.snapRetries < clientSnapRetries {
				s.snapRetries++
				n.snapWaiters = append(n.snapWaiters, cs.h)
				return nil
			}
			err = fmt.Errorf("retry limit (%d) reached after log cuts; retry the snapshot request", clientSnapRetries)
		}
		n.replyError(c, s.id, codeInternal, fmt.Sprintf("etcdserver: snapshot: %v", err))
		return nil
	}
	if c == nil {
		_ = cs.f.Close()
		return nil
	}
	body := int64(len(cs.head)) + cs.size
	cs.size = body + clientSnapPad(body) + sha256.Size
	cs.sum = sha256.New()
	n.clientSnaps = append(n.clientSnaps, cs)
	return nil
}

// clientSnapPad is the zero padding after n bytes that makes them a
// multiple of 512.
func clientSnapPad(n int64) int64 { return (512 - n%512) % 512 }

// sendClientSnaps sends each client's snapshot its next chunk, if its
// stream holds nothing back, and ends the streams sent whole.
func (n *node) sendClientSnaps() {
	k := 0
	for _, cs := range n.clientSnaps {
		if done := n.sendClientSnap(cs); done {
			_ = cs.f.Close()
			continue
		}
		n.clientSnaps[k] = cs
		k++
	}
	clear(n.clientSnaps[k:])
	n.clientSnaps = n.clientSnaps[:k]
}

func (n *node) sendClientSnap(cs *clientSnap) bool {
	c, s := n.clientOf(cs.h)
	if c == nil {
		return true
	}
	if s.holds {
		return false
	}
	// The chunk: the header first, then the checkpoint, the padding and
	// the digest.
	b := cs.buf[:0]
	if cs.sent == 0 {
		b = append(b, cs.head...)
	}
	digestAt := cs.size - sha256.Size
	room := clientSnapChunk - len(b)
	if cs.sent+int64(len(b)) < digestAt {
		if cap(cs.buf) < clientSnapChunk {
			cs.buf = make([]byte, 0, clientSnapChunk)
			b = append(cs.buf, b...)
		}
		k, err := io.ReadFull(cs.f, b[len(b):len(b)+room])
		b = b[:len(b)+k]
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			n.replyError(c, s.id, codeInternal, fmt.Sprintf("etcdserver: snapshot: %v", err))
			return true
		}
		if k < room {
			// The checkpoint ended: the padding follows.
			pad := digestAt - cs.sent - int64(len(b))
			b = append(b, make([]byte, pad)...)
		}
	}
	if cs.sent == digestAt {
		// As etcd's, the digest comes alone, last.
		b = cs.sum.Sum(b[:0])
	} else {
		cs.sum.Write(b)
	}
	cs.buf = b
	cs.sent += int64(len(b))
	n.openStream(c, s)
	// etcd's: no header, the bytes left before the digest, and the
	// storage version.
	left := uint64(max(digestAt-cs.sent, 0))
	ver := []byte(storageVersion)
	size := sizeVarintField(2, left) + sizeBytesField(3, len(b)) + sizeBytesField(4, len(ver))
	var w msgW
	w.begin(c, s, size)
	writeVarintNZ(&w, 2, left)
	w.bytesField(3, b)
	w.bytesField(4, ver)
	w.end()
	n.markDirty(&c.sock)
	if cs.sent < cs.size {
		return false
	}
	n.stats.replies++
	n.callEnded(s, 0)
	n.endStreamOK(c, s)
	return true
}

// ErrDataDirNotEmpty is a restore into a data dir that holds files.
var ErrDataDirNotEmpty = errors.New("the data directory exists and is not empty")

// storageVersion is the storage version etcd v3.7.2 names.
const storageVersion = "3.7.0"

// Restore makes dataDir the data dir of a node of a new cluster from a
// client's snapshot at path: its checkpoint, with members and voters
// replacing the snapshot's, is the newest, and the log is empty. It
// returns the snapshot's index, term and revision.
func Restore(path, dataDir string, members []Member, skipHash bool) (index, term uint64, rev int64, err error) {
	// The data dir is checked before the snapshot is read, as etcdutl
	// does, and again by restore.
	if err := restorable(sysEnv{}, dataDir); err != nil {
		return 0, 0, 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	return restore(sysEnv{}, f, fi.Size(), dataDir, members, skipHash)
}

// restorable checks that dataDir is missing or empty: as etcdutl and
// main's restore, a restore never goes into a node's data, whose old log
// would go on after the new checkpoint.
func restorable(sys sysEnv, dataDir string) error {
	entries, err := sys.readDir(dataDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %s", ErrDataDirNotEmpty, dataDir)
	}
	return nil
}

// missingDirs are the parents of the directories that a MkdirAll of
// dataDir makes, outermost first: the directory each new one is in, whose
// sync makes it durable.
func missingDirs(sys sysEnv, dataDir string) []string {
	var parents []string
	for d := filepath.Clean(dataDir); ; {
		if _, err := sys.readDir(d); !errors.Is(err, fs.ErrNotExist) {
			break
		}
		p := filepath.Dir(d)
		if p == d {
			break
		}
		parents = append([]string{p}, parents...)
		d = p
	}
	return parents
}

// restore is Restore of the snapshot of size bytes that f holds, written
// through sys.
func restore(sys sysEnv, f io.ReaderAt, size int64, dataDir string, members []Member, skipHash bool) (index, term uint64, rev int64, err error) {
	if err := restorable(sys, dataDir); err != nil {
		return 0, 0, 0, err
	}
	if size < int64(clientSnapHeader)+sha256.Size {
		return 0, 0, 0, errors.New("not a tigerstore snapshot: too short")
	}
	if !skipHash {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, size-sha256.Size)); err != nil {
			return 0, 0, 0, err
		}
		want := make([]byte, sha256.Size)
		if _, err := f.ReadAt(want, size-sha256.Size); err != nil {
			return 0, 0, 0, err
		}
		if string(h.Sum(nil)) != string(want) {
			return 0, 0, 0, errors.New("the snapshot's SHA-256 does not match; --skip-hash-check skips the check")
		}
	}
	head := make([]byte, clientSnapHeader)
	if _, err := f.ReadAt(head, 0); err != nil {
		return 0, 0, 0, err
	}
	if string(head[:len(clientSnapMagic)]) != clientSnapMagic {
		return 0, 0, 0, errors.New("not a tigerstore snapshot")
	}
	if v := binary.BigEndian.Uint32(head[len(clientSnapMagic):]); v != clientSnapVersion {
		return 0, 0, 0, fmt.Errorf("a snapshot of format %d; this build restores format %d", v, clientSnapVersion)
	}
	b := head[len(clientSnapMagic)+4:]
	index, term, rev = binary.BigEndian.Uint64(b[8:]), binary.BigEndian.Uint64(b[16:]), int64(binary.BigEndian.Uint64(b[24:]))
	cm := ckptMembers{removed: map[uint64]bool{}}
	cs := &pb.ConfState{}
	for _, m := range members {
		mem, err := m.member()
		if err != nil {
			return 0, 0, 0, err
		}
		mem.clientURLs = nil
		cm.members = append(cm.members, mem)
		cs.Voters = append(cs.Voters, m.ID)
	}
	// The directories the restore makes are made durable, each by a sync
	// of the one it is in: the data dir's two, and the data dir and its
	// parents if it makes them too. Without that, a power loss after the
	// restore could leave the data dir empty or gone, and the node would
	// start a new cluster instead (#842).
	made := missingDirs(sys, dataDir)
	dir := filepath.Join(dataDir, "ckpt")
	if err := sys.mkdirAll(dir); err != nil {
		return 0, 0, 0, err
	}
	if err := sys.mkdirAll(filepath.Join(dataDir, "wal")); err != nil {
		return 0, 0, 0, err
	}
	for _, d := range append(made, dataDir) {
		if err := syncDir(sys, d); err != nil {
			return 0, 0, 0, err
		}
	}
	name := ckptName(index)
	out, err := sys.openFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, 0, 0, err
	}
	src := io.NewSectionReader(f, int64(clientSnapHeader), size-int64(clientSnapHeader)-sha256.Size)
	err = rewriteCkpt(src, out, &ckptHeader{cs: cs, members: cm})
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, 0, 0, err
	}
	hs := &pb.HardState{Term: new(term), Commit: new(index)}
	return index, term, rev, writePointer(sys, dir, &ckptPointer{name: name, hs: hs})
}
