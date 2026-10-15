package server

import (
	"crypto/sha256"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// snapshotChunk is how many bytes of the file each message carries, as
// etcd's snapshotSendBufferSize.
const snapshotChunk = 32 << 10

// snapshot serves Maintenance.Snapshot as etcd v3.7.2 does: the file in
// messages of 32 KiB, each with the bytes still to come and the storage
// version, then the SHA-256 digest in a message of its own. The file is
// tigerstore's own format (node.Snapshot).
func (s *Server) snapshot(st *stream) error {
	var r pb.SnapshotRequest
	if err := st.RecvOne(&r); err != nil {
		return err
	}
	if err := s.n.Authorize(st.ctx, st.c.callerOf(st), &r); err != nil {
		return toStatus(err)
	}
	w := &snapshotWriter{st: st}
	_, sum, err := s.n.Snapshot(st.ctx, w, func(size int64) error {
		// RemainingBytes counts the file without its digest.
		w.left = size - sha256.Size
		return nil
	})
	if err == nil {
		err = w.flush()
	}
	if err != nil {
		return toStatus(err)
	}
	return st.Send(&pb.SnapshotResponse{Blob: sum, Version: core.StorageVersion})
}

// snapshotWriter sends what is written to it in messages of
// snapshotChunk bytes.
type snapshotWriter struct {
	st   *stream
	buf  []byte
	left int64
}

func (w *snapshotWriter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		k := min(len(p), snapshotChunk-len(w.buf))
		w.buf = append(w.buf, p[:k]...)
		p = p[k:]
		if len(w.buf) == snapshotChunk {
			if err := w.flush(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

// flush sends what is buffered. Send keeps the message, so the next one
// gets a new buffer.
func (w *snapshotWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	w.left -= int64(len(w.buf))
	err := w.st.Send(&pb.SnapshotResponse{RemainingBytes: uint64(w.left), Blob: w.buf, Version: core.StorageVersion})
	w.buf = make([]byte, 0, snapshotChunk)
	return err
}
