package mvcc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// A checkpoint holds the index as it was at one revision, copied in
// chunks while writes go on. Freeze marks the revision. Until the copy is
// done, compaction leaves versions in place, so the B-tree only gains
// keys and versions, and the copy leaves out every version after the
// frozen revision. The chunks therefore add up to the index at that
// revision, however the copy is paced.
//
// Each key is
//
//	uvarint key length | key | uvarint version count | versions
//
// and each version
//
//	varint main | varint sub | varint create | varint version | varint lease |
//	uvarint entry index | uvarint offset | uvarint length | u32 checksum
//
// A key length of 0 ends the keys; etcd has no empty keys.

// frozen is a checkpoint copy under way.
type frozen struct {
	rev int64
	// next is the first key not copied yet; nil before the first chunk.
	next    []byte
	started bool
	// compact is the newest compaction that waits for the copy, or 0.
	compact int64
}

// Freeze starts a checkpoint copy of the index at the current revision.
func (s *Store) Freeze() { s.frozen = &frozen{rev: s.rev} }

// Copying reports whether a checkpoint copy is under way.
func (s *Store) Copying() bool { return s.frozen != nil }

// CopyFrozen appends the next keys of the frozen index to b until b holds
// at least limit bytes, and the lowest entry index that a value among
// them is in, 0 for none. done is true once every key is out; then the
// end mark is appended and the compactions that waited are done.
func (s *Store) CopyFrozen(b []byte, limit int) (_ []byte, minRef uint64, done bool) {
	f := s.frozen
	more := false
	copyKey := func(k *keyIndex) bool {
		if len(b) >= limit {
			f.next, more = k.key, true
			return false
		}
		n := 0
		for n < len(k.vers) && k.vers[n].mod.main <= f.rev {
			n++
		}
		if n == 0 {
			return true
		}
		b = binary.AppendUvarint(b, uint64(len(k.key)))
		b = append(b, k.key...)
		b = binary.AppendUvarint(b, uint64(n))
		for i := range n {
			v := &k.vers[i]
			b = binary.AppendVarint(b, v.mod.main)
			b = binary.AppendVarint(b, v.mod.sub)
			b = binary.AppendVarint(b, v.create)
			b = binary.AppendVarint(b, v.ver)
			b = binary.AppendVarint(b, v.lease)
			b = binary.AppendUvarint(b, v.value.Index)
			b = binary.AppendUvarint(b, uint64(v.value.Off))
			b = binary.AppendUvarint(b, uint64(v.value.Len))
			b = binary.LittleEndian.AppendUint32(b, v.value.Sum)
			if v.value.Len > 0 && (minRef == 0 || v.value.Index < minRef) {
				minRef = v.value.Index
			}
		}
		return true
	}
	if f.started {
		s.keys.AscendGreaterOrEqual(&keyIndex{key: f.next}, copyKey)
	} else {
		f.started = true
		s.keys.Ascend(copyKey)
	}
	if more {
		return b, minRef, false
	}
	b = binary.AppendUvarint(b, 0)
	s.frozen = nil
	if f.compact != 0 {
		s.drop(f.compact)
	}
	return b, minRef, true
}

var errBadCheckpoint = errors.New("mvcc: malformed checkpoint")

// MaxKeyBytes bounds a key in a checkpoint; a request is smaller.
const MaxKeyBytes = 4 << 20

// Load reads the keys of a checkpoint, up to the end mark, into an empty
// store and sets its revisions. It returns the lowest entry index that a
// value is in, 0 for none.
func (s *Store) Load(r *bufio.Reader, rev, compactRev int64) (minRef uint64, err error) {
	s.rev, s.compactRev = rev, compactRev
	var prev []byte
	for {
		k, err := readKey(r, rev)
		if err != nil || k == nil {
			return minRef, err
		}
		if prev != nil && string(prev) >= string(k.key) {
			return 0, fmt.Errorf("%w: key %q after %q", errBadCheckpoint, k.key, prev)
		}
		prev = k.key
		for i := range k.vers {
			v := &k.vers[i]
			if v.value.Len > 0 && (minRef == 0 || v.value.Index < minRef) {
				minRef = v.value.Index
			}
			if s.track {
				s.sum ^= versionHash(k.key, v)
			}
		}
		s.keys.ReplaceOrInsert(k)
	}
}

// WalkRefs calls fn for the value of every version in the keys of a
// checkpoint, without building an index.
func WalkRefs(r *bufio.Reader, fn func(Ref) error) error {
	for {
		k, err := readKey(r, -1)
		if err != nil || k == nil {
			return err
		}
		for _, v := range k.vers {
			if v.value.Len > 0 {
				if err := fn(v.value); err != nil {
					return err
				}
			}
		}
	}
}

// readKey reads one key of a checkpoint, or nil at the end mark. A key
// has at most rev versions unless rev is negative.
func readKey(r *bufio.Reader, rev int64) (*keyIndex, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, loadError(err)
	}
	if n == 0 {
		return nil, nil
	}
	if n > MaxKeyBytes {
		return nil, fmt.Errorf("%w: key of %d bytes", errBadCheckpoint, n)
	}
	k := &keyIndex{key: make([]byte, n)}
	if _, err := io.ReadFull(r, k.key); err != nil {
		return nil, loadError(err)
	}
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, loadError(err)
	}
	if count == 0 || (rev >= 0 && count > uint64(rev)) || count > 1<<32 {
		return nil, fmt.Errorf("%w: %d versions of key %q", errBadCheckpoint, count, k.key)
	}
	k.vers = make([]version, 0, min(count, 1024))
	for range count {
		var v version
		for _, x := range [...]*int64{&v.mod.main, &v.mod.sub, &v.create, &v.ver, &v.lease} {
			if *x, err = binary.ReadVarint(r); err != nil {
				return nil, loadError(err)
			}
		}
		var off, length uint64
		for _, x := range [...]*uint64{&v.value.Index, &off, &length} {
			if *x, err = binary.ReadUvarint(r); err != nil {
				return nil, loadError(err)
			}
		}
		var sum [4]byte
		if _, err := io.ReadFull(r, sum[:]); err != nil {
			return nil, loadError(err)
		}
		if off > 1<<32-1 || length > 1<<32-1 {
			return nil, fmt.Errorf("%w: value at %d+%d", errBadCheckpoint, off, length)
		}
		v.value.Off, v.value.Len, v.value.Sum = uint32(off), uint32(length), binary.LittleEndian.Uint32(sum[:])
		k.vers = append(k.vers, v)
	}
	return k, nil
}

func loadError(err error) error {
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("%w: %w", errBadCheckpoint, err)
}

// EachLeased calls fn for every key whose newest version is live and has
// a lease, to rebuild the leases' key sets after a load.
func (s *Store) EachLeased(fn func(key []byte, lease int64)) {
	s.keys.Ascend(func(k *keyIndex) bool {
		if v := &k.vers[len(k.vers)-1]; v.ver != 0 && v.lease != 0 {
			fn(k.key, v.lease)
		}
		return true
	})
}

// EachRef calls fn for the value of every version whose value is in an
// entry from index from on, in key order.
func (s *Store) EachRef(from uint64, fn func(r Ref) error) error {
	var err error
	s.keys.Ascend(func(k *keyIndex) bool {
		for i := range k.vers {
			if r := k.vers[i].value; r.Len > 0 && r.Index >= from {
				if err = fn(r); err != nil {
					return false
				}
			}
		}
		return true
	})
	return err
}
