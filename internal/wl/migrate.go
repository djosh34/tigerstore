package wl

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The step from main's engine to this one is offline: a mixed cluster
// cannot run. `etcdctl snapshot save` on the old cluster writes main's
// snapshot (format 1), and `tigerstore snapshot restore` with this binary
// reads it with main's code, which hands its store to WriteSnapshot:
// every key as a read at the compacted revision or later sees it, the
// changes from the compacted revision on, the leases, the alarms and the
// auth store. Restore then makes the new data dir of that. As after an
// etcd compaction, what came before the compacted revision is gone. A
// node never starts on a data dir of main's engine.

// SnapStore is a store a snapshot is written of, at revision Rev,
// compacted at CompactRev.
type SnapStore struct {
	Cluster, Index, Term uint64
	Rev, CompactRev      int64
	// Leases are the leases' TTLs by their IDs.
	Leases map[int64]int64
	// Alarms are the raised alarms: a member and an alarm type each.
	Alarms [][2]uint64
	Auth   SnapAuth
	// Keys come in name order.
	Keys iter.Seq2[SnapKey, error]
	// Changes are the changes from the compacted revision on, in the
	// order they were made.
	Changes []SnapChange
}

// SnapAuth is an auth store: whether auth is on, its revision, and the
// users and roles, each etcd's message, a user's password its bcrypt hash.
type SnapAuth struct {
	Enabled  bool
	Revision uint64
	Users    []*authpb.User
	Roles    []*authpb.Role
}

// SnapKey is a key with the states a read at the compacted revision or
// later can see, oldest first: the newest at or before the compacted
// revision, unless a deletion before it, and every one after.
type SnapKey struct {
	Key    []byte
	States []SnapState
}

// SnapState is one state of a key: a deletion at Mod, or a value.
type SnapState struct {
	Deleted                     bool
	Value                       []byte
	Create, Mod, Version, Lease int64
}

// SnapChange is a change of Key at Rev, a deletion or a put.
type SnapChange struct {
	Rev     int64
	Key     []byte
	Deleted bool
}

// WriteSnapshot writes to w a client's snapshot of s.
func WriteSnapshot(w io.Writer, s *SnapStore) error {
	sum := sha256.New()
	bw := bufio.NewWriterSize(io.MultiWriter(w, sum), 1<<20)
	written := int64(0)
	crc := uint32(0)
	out := func(b []byte, ckpt bool) {
		if ckpt {
			crc = crc32.Update(crc, castagnoli, b)
		}
		written += int64(len(b))
		_, _ = bw.Write(b)
	}
	out(clientSnapHead(s.Cluster, ckptMeta{index: s.Index, term: s.Term, rev: s.Rev}), false)
	b := append([]byte(nil), ckptMagic...)
	for _, v := range []uint64{s.Index, s.Term, uint64(s.Rev), uint64(s.CompactRev)} {
		b = binary.AppendUvarint(b, v)
	}
	cs, err := proto.Marshal(&pb.ConfState{})
	if err != nil {
		return err
	}
	b = binary.AppendUvarint(b, uint64(len(cs)))
	b = append(b, cs...)
	auth := newAuthStore()
	auth.enabled, auth.revision = s.Auth.Enabled, max(s.Auth.Revision, 1)
	for _, u := range s.Auth.Users {
		auth.users[string(u.Name)] = u
	}
	for _, r := range s.Auth.Roles {
		auth.roles[string(r.Name)] = r
	}
	n := &node{format: baseFormat, auth: auth}
	for _, a := range s.Alarms {
		n.alarms = append(n.alarms, alarm{member: a[0], typ: a[1]})
	}
	b = n.encodeMembers(b)
	b = binary.AppendUvarint(b, uint64(len(s.Leases)))
	for _, id := range slices.Sorted(maps.Keys(s.Leases)) {
		b = binary.AppendUvarint(b, uint64(id))
		b = binary.AppendUvarint(b, uint64(s.Leases[id]))
	}
	b = binary.AppendUvarint(b, 0) // no values ahead of the keys
	out(b, true)
	ids := map[string]uint64{}
	var prev []byte
	for k, err := range s.Keys {
		if err != nil {
			return err
		}
		if prev != nil && string(k.Key) <= string(prev) {
			return fmt.Errorf("the snapshot's keys are out of order: %q after %q", k.Key, prev)
		}
		prev = append(prev[:0], k.Key...)
		id := uint64(len(ids))
		ids[string(k.Key)] = id
		b = append(b[:0], ckptKey)
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, uint64(len(k.Key)))
		b = append(b, k.Key...)
		b = binary.AppendUvarint(b, uint64(len(k.States)))
		out(b, true)
		for i := len(k.States) - 1; i >= 0; i-- {
			st := &k.States[i]
			flags := uint64(0)
			if st.Deleted {
				flags = ckptDeleted
			}
			b = b[:0]
			for _, v := range []uint64{flags, uint64(st.Create), uint64(st.Mod), uint64(st.Version), uint64(st.Lease), uint64(len(st.Value))} {
				b = binary.AppendUvarint(b, v)
			}
			out(append(b, st.Value...), true)
		}
	}
	for _, c := range s.Changes {
		id, held := ids[string(c.Key)]
		if !held {
			// A key no read sees: the change names it.
			id = uint64(len(ids))
			ids[string(c.Key)] = id
		}
		b = append(b[:0], ckptChange)
		b = binary.AppendUvarint(b, uint64(c.Rev))
		b = binary.AppendUvarint(b, id)
		del := byte(0)
		if c.Deleted {
			del = 1
		}
		b = append(b, del)
		b = binary.AppendUvarint(b, uint64(len(c.Key)))
		out(append(b, c.Key...), true)
	}
	out([]byte{ckptEnd}, true)
	out(binary.LittleEndian.AppendUint32(nil, crc), false)
	out(make([]byte, (512-written%512)%512), false)
	if err := bw.Flush(); err != nil {
		return err
	}
	_, err = w.Write(sum.Sum(nil))
	return err
}

// SnapshotFormat reads the format of the client's snapshot at path: 1 is
// main's engine's, 2 this one's.
func SnapshotFormat(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, len(clientSnapMagic)+4)
	if _, err := io.ReadFull(f, head); err != nil || string(head[:len(clientSnapMagic)]) != clientSnapMagic {
		return 0, errors.New("not a tigerstore snapshot")
	}
	return binary.BigEndian.Uint32(head[len(clientSnapMagic):]), nil
}

// ErrMainDataDir stops a node on a data dir of main's engine.
var ErrMainDataDir = errors.New("the data dir was written by tigerstore's previous engine, which this binary cannot run")

// checkNotMain refuses a data dir whose log is main's: segments named
// *.wal, where this engine's are *.seg.
func checkNotMain(dataDir string) error {
	ents, err := os.ReadDir(filepath.Join(dataDir, "wal"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".wal") {
			return fmt.Errorf("%w (%s): upgrade offline: save a snapshot of the old cluster with `etcdctl snapshot save`, stop every node, restore it on each with this binary's `tigerstore snapshot restore` into a new data dir, and start the nodes on those", ErrMainDataDir, dataDir)
		}
	}
	return nil
}
