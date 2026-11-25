package wl

import (
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
	n.snapWaiters = append(n.snapWaiters, mkHandle(c, s.id))
	n.storage.wantCkpt = true
}

// startClientSnaps hands the checkpoint just written to the clients that
// asked before it started: a goroutine writes each one's copy with every
// value, and posts it to the loop to send.
func (n *node) startClientSnaps(waiters []handle, name string, meta ckptMeta) {
	if len(waiters) == 0 || n.shell == nil {
		return
	}
	head := clientSnapHead(n.clusterID, meta)
	for _, h := range waiters {
		src, err := n.sys.openFile(filepath.Join(n.ckptDir, name), os.O_RDONLY, 0)
		if err != nil {
			n.replyErrorH(h, codeInternal, err.Error())
			continue
		}
		n.shell.prepareClientSnap(h, src, head)
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
func (l *loop) prepareClientSnap(h handle, src file, head []byte) {
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
		var size int64
		if err == nil {
			size, err = f.Seek(0, io.SeekCurrent)
		}
		if err == nil {
			_, err = f.Seek(0, io.SeekStart)
		}
		l.post(func() error {
			if err != nil {
				if f != nil {
					_ = f.Close()
				}
				l.n.replyErrorH(h, codeInternal, fmt.Sprintf("etcdserver: snapshot: %v", err))
				if errors.Is(err, ErrBadValue) {
					return err
				}
				return nil
			}
			body := int64(len(head)) + size
			total := body + clientSnapPad(body) + sha256.Size
			l.n.clientSnaps = append(l.n.clientSnaps, &clientSnap{h: h, f: f, head: head, size: total, sum: sha256.New()})
			return nil
		})
	}()
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
	// As etcdutl and main's restore, never into a node's data: its old
	// log would go on after the new checkpoint.
	entries, err := os.ReadDir(dataDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, 0, 0, err
	}
	if len(entries) > 0 {
		return 0, 0, 0, fmt.Errorf("%w: %s", ErrDataDirNotEmpty, dataDir)
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
	if fi.Size() < int64(clientSnapHeader)+sha256.Size {
		return 0, 0, 0, errors.New("not a tigerstore snapshot: too short")
	}
	if !skipHash {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, fi.Size()-sha256.Size)); err != nil {
			return 0, 0, 0, err
		}
		want := make([]byte, sha256.Size)
		if _, err := f.ReadAt(want, fi.Size()-sha256.Size); err != nil {
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
	dir := filepath.Join(dataDir, "ckpt")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, 0, 0, err
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "wal"), 0o700); err != nil {
		return 0, 0, 0, err
	}
	name := ckptName(index)
	out, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return 0, 0, 0, err
	}
	src := io.NewSectionReader(f, int64(clientSnapHeader), fi.Size()-int64(clientSnapHeader)-sha256.Size)
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
	return index, term, rev, writePointer(sysEnv{}, dir, &ckptPointer{name: name, hs: hs})
}
