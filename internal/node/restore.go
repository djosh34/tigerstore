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

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Restore writes the state of the snapshot file at path into a new data
// directory, for a node of a cluster with these voters, as etcdutl
// snapshot restore does: the node starts at the snapshot's index and
// term, with everything up to it committed. Every node of the new cluster
// restores the same file. Unless skipHash is set, the file's SHA-256
// must match. The data directory must not exist yet, or be empty.
func Restore(path, dataDir string, voters []uint64, skipHash bool) (SnapshotInfo, error) {
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
	dir, err := osfs.New(filepath.Join(dataDir, "wal"))
	if err != nil {
		return info, err
	}
	w, _, err := wal.Open(dir)
	if err != nil {
		return info, err
	}
	sink := &restoreSink{w: w}
	in := &core.Install{Index: info.Index, Term: info.Term}
	err = core.ReceiveSnapshot(r, sink, w, voters, in)
	if err == nil {
		w.Append(&wal.HardState{Term: info.Term, Commit: info.Index}, nil)
		err = w.Sync()
	}
	return info, errors.Join(err, sink.close(), w.Close())
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
