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

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The step from main's engine to this one is offline: a mixed cluster
// cannot run. `etcdctl snapshot save` on the old cluster writes main's
// snapshot (format 1), and `tigerstore snapshot restore` with this binary
// reads it with main's code, which hands each live key to WriteSnapshot;
// Restore then makes the new data dir of that. A node never starts on a
// data dir of main's engine.

// SnapKey is a live key of a store at a snapshot's revision.
type SnapKey struct {
	Key, Value                  []byte
	Create, Mod, Version, Lease int64
}

// WriteSnapshot writes to w a client's snapshot of a store at revision
// rev: leases, by ID with their TTLs, and keys, in name order. The store
// keeps no history: the snapshot's revision is its compacted one.
func WriteSnapshot(w io.Writer, cluster, index, term uint64, rev int64, leases map[int64]int64, keys iter.Seq2[SnapKey, error]) error {
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
	out(clientSnapHead(cluster, ckptMeta{index: index, term: term, rev: rev}), false)
	b := append([]byte(nil), ckptMagic...)
	for _, v := range []uint64{index, term, uint64(rev), uint64(rev)} {
		b = binary.AppendUvarint(b, v)
	}
	cs, err := proto.Marshal(&pb.ConfState{})
	if err != nil {
		return err
	}
	b = binary.AppendUvarint(b, uint64(len(cs)))
	b = append(b, cs...)
	b = (&node{format: baseFormat}).encodeMembers(b)
	b = binary.AppendUvarint(b, uint64(len(leases)))
	for _, id := range slices.Sorted(maps.Keys(leases)) {
		b = binary.AppendUvarint(b, uint64(id))
		b = binary.AppendUvarint(b, uint64(leases[id]))
	}
	b = binary.AppendUvarint(b, 0) // no values ahead of the keys
	out(b, true)
	// The keys written at the revision itself: a watch from it gets them.
	type atRev struct {
		id  uint64
		key []byte
	}
	var last []atRev
	var prev []byte
	id := uint64(0)
	for k, err := range keys {
		if err != nil {
			return err
		}
		if prev != nil && string(k.Key) <= string(prev) {
			return fmt.Errorf("the snapshot's keys are out of order: %q after %q", k.Key, prev)
		}
		prev = append(prev[:0], k.Key...)
		b = append(b[:0], ckptKey)
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, uint64(len(k.Key)))
		b = append(b, k.Key...)
		b = binary.AppendUvarint(b, 1)
		for _, v := range []uint64{0, uint64(k.Create), uint64(k.Mod), uint64(k.Version), uint64(k.Lease), uint64(len(k.Value))} {
			b = binary.AppendUvarint(b, v)
		}
		out(append(b, k.Value...), true)
		if k.Mod == rev {
			last = append(last, atRev{id, slices.Clone(k.Key)})
		}
		id++
	}
	for _, c := range last {
		b = append(b[:0], ckptChange)
		b = binary.AppendUvarint(b, uint64(rev))
		b = binary.AppendUvarint(b, c.id)
		b = append(b, 0)
		b = binary.AppendUvarint(b, uint64(len(c.key)))
		out(append(b, c.key...), true)
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
