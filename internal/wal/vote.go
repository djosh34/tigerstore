package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	iofs "io/fs"
	"slices"
)

// The term and vote are also kept outside the log, in two copies: the
// files vote-0 and vote-1. Recovery takes damage to the log's last record
// for a torn write and cuts it off, which could lose a synced term and
// vote and let the node vote twice in a term (#357). A copy is
//
//	magic | u64 term | u64 vote | u32 crc
//
// with the crc a CRC-32C of what comes before it. A copy is written and
// synced before the other one is written, so a crash tears at most one of
// them, and the other holds the newest term and vote that counted. A copy
// file takes its name only once it holds a whole copy, so copies on disk
// of which none verifies are damage.
const (
	voteMagic = "tigervot"
	voteSize  = len(voteMagic) + 8 + 8 + 4
	votePart  = ".new"
)

// voteNames are the files of the two copies.
var voteNames = [2]string{"vote-0", "vote-1"}

// IsVoteFile reports whether name is a copy of the term and vote, or one
// being made.
func IsVoteFile(name string) bool {
	return slices.ContainsFunc(voteNames[:], func(v string) bool { return name == v || name == v+votePart })
}

func encodeVote(term, vote uint64) []byte {
	b := make([]byte, 0, voteSize)
	b = append(b, voteMagic...)
	b = binary.LittleEndian.AppendUint64(b, term)
	b = binary.LittleEndian.AppendUint64(b, vote)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeVote returns the term and vote of a copy, and whether it
// verifies. Zeros may follow it: LazyFS can give a new file the size of a
// removed one (#240).
func decodeVote(b []byte) (HardState, bool) {
	if len(b) < voteSize || slices.ContainsFunc(b[voteSize:], func(c byte) bool { return c != 0 }) ||
		string(b[:len(voteMagic)]) != voteMagic ||
		crc32.Checksum(b[:voteSize-4], castagnoli) != binary.LittleEndian.Uint32(b[voteSize-4:]) {
		return HardState{}, false
	}
	return HardState{Term: binary.LittleEndian.Uint64(b[len(voteMagic):]), Vote: binary.LittleEndian.Uint64(b[len(voteMagic)+8:])}, true
}

// newerVote reports whether term and vote a are newer than b: a later
// term, or a vote in a term that b holds no vote in. Raft never moves a
// term back, nor a vote within a term.
func newerVote(a, b HardState) bool {
	return a.Term > b.Term || a.Term == b.Term && b.Vote == 0 && a.Vote != 0
}

// SaveVote writes term and vote into both copies, which Open made, each
// synced before the next is written. After an error the node must stop,
// as after a failed sync of the log.
func SaveVote(fs FS, term, vote uint64) error {
	for _, name := range voteNames {
		if err := writeVote(fs, name, term, vote); err != nil {
			return err
		}
	}
	return nil
}

// writeVote writes term and vote into the copy in file name and syncs it.
func writeVote(fs FS, name string, term, vote uint64) error {
	f, err := fs.Open(name)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(encodeVote(term, vote), 0)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// voteCopy is what a copy file holds: whether it is on disk, and its term
// and vote if it verifies.
type voteCopy struct {
	found, ok bool
	hs        HardState
}

// readVotes reads the copies, of the files in names. It is ErrCorrupt if
// copies are on disk and none of them verifies.
func readVotes(fs FS, names []string) ([2]voteCopy, error) {
	var copies [2]voteCopy
	for i, name := range voteNames {
		if !slices.Contains(names, name) {
			continue
		}
		f, err := fs.Open(name)
		if err != nil {
			return copies, err
		}
		b, err := f.ReadAll()
		if err := errors.Join(err, f.Close()); err != nil {
			return copies, err
		}
		copies[i].found = true
		copies[i].hs, copies[i].ok = decodeVote(b)
	}
	if (copies[0].found || copies[1].found) && !copies[0].ok && !copies[1].ok {
		return copies, fmt.Errorf("%w: no copy of the term and vote verifies", ErrCorrupt)
	}
	return copies, nil
}

// newestVote is the newest term and vote of the copies that verify.
func newestVote(copies [2]voteCopy) HardState {
	var hs HardState
	for _, c := range copies {
		if c.ok && newerVote(c.hs, hs) {
			hs = c.hs
		}
	}
	return hs
}

// writeVotes writes hs into both copies, which held copies, and syncs
// each. A copy that does not hold hs goes first, so that the other one
// keeps the newest term and vote that verifies until it is synced; one
// that holds it is written again, as what recovery read may not be
// synced. A copy not on disk is made under another name, renamed once it
// is synced, and its name made durable. Leftovers of such a copy, which a
// crash left, are removed first.
func writeVotes(fs FS, names []string, copies [2]voteCopy, hs HardState) error {
	for _, name := range names {
		if IsVoteFile(name) && !slices.Contains(voteNames[:], name) {
			if err := fs.Remove(name); err != nil && !errors.Is(err, iofs.ErrNotExist) {
				return err
			}
		}
	}
	order := []int{0, 1}
	if copies[0].ok && copies[0].hs == hs {
		order = []int{1, 0}
	}
	for _, i := range order {
		name := voteNames[i]
		if copies[i].found {
			if err := writeVote(fs, name, hs.Term, hs.Vote); err != nil {
				return err
			}
			continue
		}
		f, err := fs.Create(name + votePart)
		if err != nil {
			return err
		}
		_, err = f.WriteAt(encodeVote(hs.Term, hs.Vote), 0)
		if err == nil {
			err = f.Sync()
		}
		if err := errors.Join(err, f.Close()); err != nil {
			return err
		}
		if err := fs.Rename(name+votePart, name); err != nil {
			return err
		}
		if err := fs.SyncDir(); err != nil {
			return err
		}
	}
	return nil
}
