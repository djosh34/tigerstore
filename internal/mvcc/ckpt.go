package mvcc

// Prototype E (#133): a metadata-only checkpoint of the index. The
// committer copies the frozen view in chunks, a few thousand keys per
// turn; versions newer than the freeze are left out and physical
// compaction waits, so the chunks add up to the state at the freeze.

import (
	"encoding/binary"
	"errors"
)

// BeginCheckpoint freezes the view at the current revision.
func (s *Store) BeginCheckpoint() (rev, compactRev int64) {
	s.freezing, s.freezeRev, s.cursor, s.cursorStarted = true, s.rev, nil, false
	return s.rev, s.compactRev
}

// CheckpointChunk appends up to max keys of the frozen view to b. done is
// true once every key is out; physical compaction then catches up.
func (s *Store) CheckpointChunk(b []byte, max int) (_ []byte, done bool) {
	n := 0
	var next []byte
	fn := func(k *keyIndex) bool {
		if n == max {
			next = k.key
			return false
		}
		nv := 0
		for i := range k.vers {
			if k.vers[i].mod.main <= s.freezeRev {
				nv++
			}
		}
		if nv == 0 {
			return true
		}
		n++
		b = binary.AppendUvarint(b, uint64(len(k.key)))
		b = append(b, k.key...)
		b = binary.AppendUvarint(b, uint64(nv))
		for i := range k.vers {
			v := &k.vers[i]
			if v.mod.main > s.freezeRev {
				continue
			}
			b = binary.AppendVarint(b, v.mod.main)
			b = binary.AppendVarint(b, v.mod.sub)
			b = binary.AppendVarint(b, v.create)
			b = binary.AppendVarint(b, v.ver)
			b = binary.AppendVarint(b, v.lease)
			b = binary.AppendUvarint(b, v.value.Idx)
			b = binary.AppendUvarint(b, uint64(v.value.Off))
			b = binary.AppendUvarint(b, uint64(v.value.N))
		}
		return true
	}
	if !s.cursorStarted {
		s.cursorStarted = true
		s.keys.Ascend(fn)
	} else {
		s.keys.AscendGreaterOrEqual(&keyIndex{key: s.cursor}, fn)
	}
	if next != nil {
		s.cursor = next
		return b, false
	}
	s.freezing, s.cursor = false, nil
	if s.pendingCompact != 0 {
		s.physicalCompact(s.pendingCompact)
		s.pendingCompact = 0
	}
	return b, true
}

var errBadCkpt = errors.New("mvcc: bad checkpoint chunk")

// WalkCheckpoint calls key for every key and ver for every version in a
// chunk.
func WalkCheckpoint(b []byte, key func([]byte), ver func(modMain, modSub, create, ver, lease int64, r Ref)) error {
	u := func() uint64 {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			panic(errBadCkpt)
		}
		b = b[n:]
		return v
	}
	i := func() int64 {
		v, n := binary.Varint(b)
		if n <= 0 {
			panic(errBadCkpt)
		}
		b = b[n:]
		return v
	}
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errBadCkpt
			}
		}()
		for len(b) > 0 {
			kl := u()
			k := b[:kl]
			b = b[kl:]
			if key != nil {
				key(k)
			}
			nv := u()
			for range nv {
				mm, ms, c, vv, l := i(), i(), i(), i(), i()
				r := Ref{Idx: u()}
				r.Off = uint32(u())
				r.N = uint32(u())
				if ver != nil {
					ver(mm, ms, c, vv, l, r)
				}
			}
		}
	}()
	return err
}

// LoadCheckpointChunk adds the keys of a chunk, which come in key order
// after every key loaded before.
func (s *Store) LoadCheckpointChunk(b []byte) error {
	var k *keyIndex
	flush := func() {
		if k != nil {
			s.keys.ReplaceOrInsert(k)
		}
	}
	err := WalkCheckpoint(b, func(key []byte) {
		flush()
		k = &keyIndex{key: append([]byte(nil), key...)}
	}, func(mm, ms, c, vv, l int64, r Ref) {
		k.vers = append(k.vers, version{mod: rev{mm, ms}, create: c, ver: vv, lease: l, value: r})
	})
	flush()
	return err
}

// SetRevs sets the revisions after a checkpoint load.
func (s *Store) SetRevs(rev, compactRev int64) { s.rev, s.compactRev = rev, compactRev }

// EachLease calls fn for the lease of every key's newest live version, to
// rebuild the lease key sets after a load.
func (s *Store) EachLease(fn func(key []byte, lease int64)) {
	s.keys.Ascend(func(k *keyIndex) bool {
		v := &k.vers[len(k.vers)-1]
		if v.ver != 0 && v.lease != 0 {
			fn(k.key, v.lease)
		}
		return true
	})
}
