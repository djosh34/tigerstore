package node

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Restore writes the state of the snapshot file at path into a new data
// directory, for a node of a new cluster with these members, as etcdutl
// snapshot restore does: the members replace the snapshot's, and the
// node starts at the snapshot's index and term, from a checkpoint there.
// Every node of the new cluster restores the same file with the same
// members. Unless skipHash is set, the file's SHA-256 must match. The
// data directory must not exist yet, or be empty.
func Restore(path, dataDir string, members []*pb.Member, skipHash bool) (SnapshotInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return SnapshotInfo{}, err
	}
	defer f.Close() //nolint:errcheck // read only
	st, err := f.Stat()
	if err != nil {
		return SnapshotInfo{}, err
	}
	size := st.Size()
	if size%512 != sha256.Size || size < int64(SnapshotHeaderBytes)+sha256.Size {
		return SnapshotInfo{}, fmt.Errorf("node: %s has %d bytes, not a multiple of 512 and a SHA-256", path, size)
	}
	if !skipHash {
		h := sha256.New()
		if _, err := io.CopyN(h, f, size-sha256.Size); err != nil {
			return SnapshotInfo{}, err
		}
		sum := make([]byte, sha256.Size)
		if _, err := io.ReadFull(f, sum); err != nil {
			return SnapshotInfo{}, err
		}
		if !bytes.Equal(sum, h.Sum(nil)) {
			return SnapshotInfo{}, fmt.Errorf("node: the SHA-256 of %s does not match", path)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return SnapshotInfo{}, err
		}
	}
	r := bufio.NewReaderSize(f, 256<<10)
	hdr := make([]byte, SnapshotHeaderBytes)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return SnapshotInfo{}, err
	}
	info, err := ParseSnapshotHeader(hdr)
	if err != nil {
		return info, err
	}
	if entries, err := os.ReadDir(dataDir); err == nil && len(entries) > 0 {
		return info, fmt.Errorf("node: data directory %q exists and is not empty", dataDir)
	}
	if len(members) == 0 {
		return info, errors.New("node: a restored cluster needs members")
	}
	// The snapshot goes into a log of its own first, which a core opens
	// to give the state its new members; their checkpoint starts the new
	// log.
	parent := filepath.Dir(filepath.Clean(dataDir))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return info, err
	}
	tmp, err := os.MkdirTemp(parent, filepath.Base(dataDir)+".restore-")
	if err != nil {
		return info, err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck // only a temporary copy
	if err := receive(r, filepath.Join(tmp, "wal"), &core.Install{Index: info.Index, Term: info.Term}); err != nil {
		return info, err
	}
	dir, err := osfs.New(filepath.Join(tmp, "wal"))
	if err != nil {
		return info, err
	}
	old, rec, err := wal.Open(dir)
	if err != nil {
		return info, err
	}
	defer old.Close() //nolint:errcheck // read only by now
	cfg := core.DefaultConfig(members[0].ID, nil)
	cfg.Join = true
	n, err := core.New(cfg, rec, old)
	if err != nil {
		return info, err
	}
	root, ckpt, err := n.RestoredCheckpoint(members)
	if err != nil {
		return info, err
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(core.ServeCheckpoint(restoredLog{WAL: old, ckpt: ckpt}, root, pw)) }()
	err = receive(pr, filepath.Join(dataDir, "wal"), &core.Install{Index: root.Index, Term: root.Term})
	return info, errors.Join(err, pr.Close())
}

// receive writes a snapshot stream into a new log in dir, with everything
// up to its index committed.
func receive(r io.Reader, dir string, in *core.Install) error {
	d, err := osfs.New(dir)
	if err != nil {
		return err
	}
	w, _, err := wal.Open(d)
	if err != nil {
		return err
	}
	sink := &restoreSink{w: w}
	err = core.ReceiveSnapshot(r, sink, w, in)
	if err == nil {
		w.Append(&wal.HardState{Term: in.Term, Commit: in.Index}, nil)
		err = w.Sync()
	}
	return errors.Join(err, sink.close(), w.Close())
}

// restoredLog reads a restored log, with the checkpoint that gives the
// state its new members in place of any on disk.
type restoredLog struct {
	*wal.WAL
	ckpt []byte
}

func (l restoredLog) Checkpoint(uint64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.ckpt)), nil
}

// restoreSink writes a snapshot into a new log.
type restoreSink struct {
	w       *wal.WAL
	ckpt    wal.File
	pending int
}

func (s *restoreSink) wrote(n int) error {
	if s.pending += n; s.pending < syncBytes {
		return nil
	}
	s.pending = 0
	return s.w.Sync()
}

func (s *restoreSink) Entries(ents []*raftpb.Entry) error {
	s.w.Append(nil, ents)
	size := 0
	for _, e := range ents {
		size += len(e.GetData())
	}
	return s.wrote(size)
}

func (s *restoreSink) Value(index uint64, off uint32, value []byte) error {
	s.w.AppendValue(index, off, value)
	return s.wrote(len(value))
}

func (s *restoreSink) CheckpointChunk(index uint64, data []byte) error {
	if s.ckpt == nil {
		f, err := s.w.CreateCheckpoint(index)
		if err != nil {
			return err
		}
		s.ckpt = f
	}
	_, err := s.ckpt.Write(data)
	return err
}

func (s *restoreSink) CheckpointDone(uint64) error {
	f := s.ckpt
	s.ckpt = nil
	if f == nil {
		return errors.New("node: the snapshot's checkpoint has no data")
	}
	return errors.Join(f.Sync(), f.Close(), s.w.SyncDir())
}

func (s *restoreSink) Root(r wal.Root) error {
	s.w.AppendRoot(r)
	return nil
}

func (s *restoreSink) close() error {
	if s.ckpt == nil {
		return nil
	}
	return s.ckpt.Close()
}
