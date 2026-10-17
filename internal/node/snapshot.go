package node

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"slices"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
)

// A client's snapshot (Maintenance.Snapshot) is tigerstore's own format:
//
//	magic "tigersnp" | u32 format version | u64 cluster ID | u64 index |
//	u64 term | i64 revision | snapshot stream | zero padding | SHA-256
//
// all integers big-endian. The stream is what a follower fetches
// (core.ServeSnapshot) from index 1 to the node's applied index: its
// newest checkpoint with every value it names and the entries after it,
// or every entry when the log still starts at 1. The padding makes the
// file up to the digest a multiple of 512 bytes, and the digest covers
// everything before it, so the file ends as etcd's does and `etcdctl
// snapshot save` accepts it.

const (
	snapshotMagic   = "tigersnp"
	snapshotVersion = 1
	// SnapshotHeaderBytes is the size of a snapshot file's header.
	SnapshotHeaderBytes = len(snapshotMagic) + 4 + 4*8
)

// SnapshotInfo describes a snapshot: the cluster it came from and the
// applied index, its term and the revision there.
type SnapshotInfo struct {
	ClusterID, Index, Term uint64
	Revision               int64
}

func (s SnapshotInfo) header() []byte {
	b := append([]byte(snapshotMagic), 0, 0, 0, snapshotVersion)
	b = binary.BigEndian.AppendUint64(b, s.ClusterID)
	b = binary.BigEndian.AppendUint64(b, s.Index)
	b = binary.BigEndian.AppendUint64(b, s.Term)
	return binary.BigEndian.AppendUint64(b, uint64(s.Revision))
}

// ParseSnapshotHeader reads a snapshot file's header.
func ParseSnapshotHeader(b []byte) (SnapshotInfo, error) {
	if len(b) < SnapshotHeaderBytes || string(b[:len(snapshotMagic)]) != snapshotMagic {
		return SnapshotInfo{}, errors.New("node: not a tigerstore snapshot")
	}
	b = b[len(snapshotMagic):]
	if v := binary.BigEndian.Uint32(b); v != snapshotVersion {
		return SnapshotInfo{}, fmt.Errorf("node: snapshot format %d; this build reads format %d", v, snapshotVersion)
	}
	return SnapshotInfo{
		ClusterID: binary.BigEndian.Uint64(b[4:]),
		Index:     binary.BigEndian.Uint64(b[12:]),
		Term:      binary.BigEndian.Uint64(b[20:]),
		Revision:  int64(binary.BigEndian.Uint64(b[28:])),
	}, nil
}

// snapshotPad is the zero padding after n bytes that makes them a
// multiple of 512.
func snapshotPad(n int64) int64 { return (512 - n%512) % 512 }

// Snapshot writes a snapshot file of the node's state at its applied
// index to w, all but the digest, which it returns. Before it writes
// anything it calls start with the size of the whole file. Nothing is
// removed from the log while it runs.
func (n *Node) Snapshot(ctx context.Context, w io.Writer, start func(size int64) error) (SnapshotInfo, []byte, error) {
	n.log.mu.Lock()
	n.log.serving++
	ckpts := slices.Clone(n.log.ckpts)
	n.log.mu.Unlock()
	defer func() {
		n.log.mu.Lock()
		n.log.serving--
		n.log.mu.Unlock()
		n.log.wake()
	}()
	ch := make(chan snapshotPoint, 1)
	if err := n.put(ctx, input{snapshotPoint: ch}); err != nil {
		return SnapshotInfo{}, nil, err
	}
	var p snapshotPoint
	select {
	case p = <-ch:
	case <-ctx.Done():
		return SnapshotInfo{}, nil, ctx.Err()
	case <-n.done:
		return SnapshotInfo{}, nil, rpctypes.ErrGRPCStopped
	}
	if p.err != nil {
		return SnapshotInfo{}, nil, p.err
	}
	info := p.info
	info.ClusterID = n.ClusterID()
	// A first pass counts the bytes without reading any value.
	var count counter
	if err := n.serveBackup(sizeOnly{n.log}, ckpts, info.Index, &count); err != nil {
		return info, nil, err
	}
	body := int64(SnapshotHeaderBytes) + count.n
	if err := start(body + snapshotPad(body) + sha256.Size); err != nil {
		return info, nil, err
	}
	sw := &shaWriter{w: w, h: sha256.New()}
	if _, err := sw.Write(info.header()); err != nil {
		return info, nil, err
	}
	if err := n.serveBackup(n.log, ckpts, info.Index, sw); err != nil {
		return info, nil, err
	}
	if sw.n != body {
		return info, nil, fmt.Errorf("node: the snapshot has %d bytes, the count was %d", sw.n, body)
	}
	if _, err := sw.Write(make([]byte, snapshotPad(body))); err != nil {
		return info, nil, err
	}
	return info, sw.h.Sum(nil), nil
}

// serveBackup writes the stream of a backup up to index. Damage to the
// node's own log that it finds stops the node, as when it serves a peer
// (core.Node.SnapshotSent); the backup fails either way.
func (n *Node) serveBackup(log core.Log, ckpts []wal.Root, index uint64, w io.Writer) error {
	err := core.ServeSnapshot(log, ckpts, 1, index, w)
	if core.FailureOf(err) == core.Fence {
		n.tell(input{served: true, serveErr: err})
	}
	return err
}

// snapshotPoint is the committer's answer to a snapshot's start.
type snapshotPoint struct {
	info SnapshotInfo
	err  error
}

// sizeOnly reads the log as it is, but hands out values as zeros of
// their length, so that counting a stream reads no value.
type sizeOnly struct{ *diskLog }

func (s sizeOnly) Value(_ uint64, _, n uint32) ([]byte, error) { return make([]byte, n), nil }

type counter struct{ n int64 }

func (c *counter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }

// shaWriter writes to w and hashes what it wrote.
type shaWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (s *shaWriter) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	s.h.Write(p[:n])
	s.n += int64(n)
	return n, err
}
