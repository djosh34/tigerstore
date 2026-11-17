package node

import (
	"encoding/binary"
	"errors"
	"fmt"
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
