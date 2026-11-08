package mvcc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/djosh34/tigerstore/internal/wal"
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
	// next is the key the next chunk starts at, nil before the first
	// chunk. keyOff and ver are how far the copy of next got: the bytes
	// of its key, and its versions, both 0 if it is not begun. A chunk can
	// stop inside a key's bytes or its versions (#435).
	next        []byte
	keyOff, ver int
	started     bool
	// compact is the newest compaction that waits for the copy, or 0.
	compact int64
}

// Freeze starts a checkpoint copy of the index at the current revision.
func (s *Store) Freeze() { s.frozen = &frozen{rev: s.rev} }

// CopyFrozen appends the next keys of the frozen index to b until b holds
// at least limit bytes, and the lowest entry index that a value among
// them is in, 0 for none. It stops inside a key's bytes or between two
// of its versions, so a chunk passes limit by at most one version, one
// byte of a key and the uvarints around a key. done is true once every
// key is out; then the end mark is appended and the compactions that
// waited are done.
func (s *Store) CopyFrozen(b []byte, limit int) (_ []byte, minRef uint64, done bool) {
	f := s.frozen
	more, first := false, true
	copyKey := func(k *keyIndex) bool {
		keyOff, from := 0, 0
		if first {
			// The walk starts at the key the last chunk stopped in.
			keyOff, from = f.keyOff, f.ver
			f.keyOff, f.ver, first = 0, 0, false
		}
		// The versions at or below the frozen revision come first.
		n := k.at(f.rev) + 1
		if n == 0 {
			return true
		}
		begun := keyOff > 0 || from > 0
		if !begun && len(b) >= limit {
			f.next, more = k.key, true
			return false
		}
		if from == 0 {
			if !begun {
				b = binary.AppendUvarint(b, uint64(len(k.key)))
			}
			// At least one byte of the key goes, so that the chunk stops
			// inside it, with its length written, even when the length
			// filled the chunk.
			take := len(k.key) - keyOff
			if room := limit - len(b); take > room {
				take = max(room, 1)
			}
			b = append(b, k.key[keyOff:keyOff+take]...)
			if keyOff += take; keyOff < len(k.key) {
				f.next, f.keyOff, more = k.key, keyOff, true
				return false
			}
			b = binary.AppendUvarint(b, uint64(n))
		}
		for i := from; i < n; i++ {
			if i > from && len(b) >= limit {
				f.next, f.ver, more = k.key, i, true
				return false
			}
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

// ErrBadCheckpoint means that the keys of a checkpoint do not parse.
var ErrBadCheckpoint = errors.New("mvcc: malformed checkpoint")

// MaxKeyBytes is the one bound on a key: a put refuses a longer one
// (CheckPutOp), and a checkpoint's readers take every key up to it
// (#438). A key comes in a Raft entry, so no request the node takes can
// carry a longer one.
const MaxKeyBytes = wal.MaxEntryBytes

// maxLoadRev bounds the store revision a checkpoint may name (Load).
const maxLoadRev = 1 << 62

// Load reads the keys of a checkpoint, up to the end mark, into an empty
// store and sets its revisions. It returns the lowest entry index that a
// value is in, 0 for none. A compaction above the store's revision is
// malformed: every read would fail as compacted.
func (s *Store) Load(r *bufio.Reader, rev, compactRev int64) (minRef uint64, err error) {
	// A store revision of 2^62 or more leaves too little room before the
	// largest int64 for the writes and history arithmetic that follow, and
	// no store gets there (2^62 writes at a million a second take 146,000
	// years); a compaction below -1, a store's "never", would let a later
	// one wrap.
	if rev < 1 || rev >= maxLoadRev || compactRev < -1 || compactRev > rev {
		return 0, fmt.Errorf("%w: revision %d compacted at %d", ErrBadCheckpoint, rev, compactRev)
	}
	s.rev, s.compactRev = rev, compactRev
	var prev []byte
	for {
		k, err := readKey(r, rev)
		if err != nil {
			return minRef, err
		}
		if k == nil {
			return minRef, s.hist.rebuild(s, max(compactRev, 0))
		}
		if prev != nil && string(prev) >= string(k.key) {
			return 0, fmt.Errorf("%w: key %q after %q", ErrBadCheckpoint, k.key, prev)
		}
		prev = k.key
		s.versions += int64(cap(k.vers))
		s.keyBytes += int64(len(k.key))
		if k.vers[len(k.vers)-1].ver != 0 {
			s.counts.Keys++
		}
		for i := range k.vers {
			v := &k.vers[i]
			if v.value.Len > 0 && (minRef == 0 || v.value.Index < minRef) {
				minRef = v.value.Index
			}
			s.size += rowBytes(k.key, v)
			if s.track {
				s.sum ^= versionHash(k.key, v)
			}
		}
		s.keys.ReplaceOrInsert(k)
	}
}

// WalkRefs calls fn for the value of every version in the keys of a
// checkpoint, without building an index. It reads one version at a time
// and skips the key's bytes, so it holds no key and no list of versions
// (#435).
func WalkRefs(r *bufio.Reader, fn func(Ref) error) error {
	for {
		n, err := readKeyHead(r)
		if err != nil || n == 0 {
			return err
		}
		if _, err := r.Discard(int(n)); err != nil {
			return loadError(err)
		}
		count, err := readCount(r, -1, nil)
		if err != nil {
			return err
		}
		for range count {
			v, err := readVersion(r)
			if err != nil {
				return err
			}
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
	n, err := readKeyHead(r)
	if err != nil || n == 0 {
		return nil, err
	}
	k := &keyIndex{key: make([]byte, n)}
	if _, err := io.ReadFull(r, k.key); err != nil {
		return nil, loadError(err)
	}
	count, err := readCount(r, rev, k.key)
	if err != nil {
		return nil, err
	}
	k.vers = make([]version, 0, min(count, 1024))
	for range count {
		v, err := readVersion(r)
		if err != nil {
			return nil, err
		}
		k.vers = append(k.vers, v)
	}
	return k, nil
}

// readKeyHead reads the length of the next key, 0 at the end mark, and
// checks it against MaxKeyBytes.
func readKeyHead(r *bufio.Reader) (uint64, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, loadError(err)
	}
	if n > MaxKeyBytes {
		return 0, fmt.Errorf("%w: key of %d bytes", ErrBadCheckpoint, n)
	}
	return n, nil
}

// readCount reads how many versions the key has: at least one, and at
// most rev unless rev is negative.
func readCount(r *bufio.Reader, rev int64, key []byte) (uint64, error) {
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, loadError(err)
	}
	if count == 0 || (rev >= 0 && count > uint64(rev)) || count > 1<<32 {
		return 0, fmt.Errorf("%w: %d versions of key %q", ErrBadCheckpoint, count, key)
	}
	return count, nil
}

// readVersion reads one version of a key.
func readVersion(r *bufio.Reader) (version, error) {
	var v version
	var err error
	for _, x := range [...]*int64{&v.mod.main, &v.mod.sub, &v.create, &v.ver, &v.lease} {
		if *x, err = binary.ReadVarint(r); err != nil {
			return v, loadError(err)
		}
	}
	var off, length uint64
	for _, x := range [...]*uint64{&v.value.Index, &off, &length} {
		if *x, err = binary.ReadUvarint(r); err != nil {
			return v, loadError(err)
		}
	}
	// Peek reads the checksum in place, so a version allocates nothing.
	sum, err := r.Peek(4)
	if err != nil {
		return v, loadError(err)
	}
	if off > 1<<32-1 || length > 1<<32-1 {
		return v, fmt.Errorf("%w: value at %d+%d", ErrBadCheckpoint, off, length)
	}
	v.value.Off, v.value.Len, v.value.Sum = uint32(off), uint32(length), binary.LittleEndian.Uint32(sum)
	_, err = r.Discard(4)
	return v, err
}

func loadError(err error) error {
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("%w: %w", ErrBadCheckpoint, err)
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
