// Package committed reads the Raft log from the WALs of stopped nodes, for
// the checks of the chaos and rolling-upgrade runs: the log of
// internal/wl, which the node runs, and main's, which the previous
// release ran and the offline upgrade from it reads.
package committed

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/internal/osfs"
	tigerwal "github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
	"go.etcd.io/raft/v3/raftpb"
)

// Node is the log that one stopped node's WAL holds.
type Node struct {
	Dir    string
	Commit uint64
	// Entries[i] is the entry at index First+i.
	First   uint64
	Entries []*raftpb.Entry
}

// Last is the index of the node's last entry.
func (n Node) Last() uint64 { return n.First + uint64(len(n.Entries)) - 1 }

// Validate checks that the node holds every entry up to its commit index.
func (n Node) Validate() error {
	if n.Commit < n.First || n.Commit > n.Last() {
		return fmt.Errorf("missing committed entries: commit %d, entries %d to %d", n.Commit, n.First, n.Last())
	}
	return nil
}

// Prefix returns the longest committed log, which must start at index
// first, after checking that every node's committed entries agree with
// it. A node that installed a snapshot holds a shorter log.
func Prefix(ns []Node, first uint64) ([]*raftpb.Entry, error) {
	if len(ns) == 0 {
		return nil, errors.New("no recovered WALs")
	}
	best := ns[0]
	for _, n := range ns {
		if n.Commit > best.Commit {
			best = n
		}
	}
	if best.First != first {
		return nil, fmt.Errorf("the longest committed log, of %s, starts at index %d, not %d", best.Dir, best.First, first)
	}
	for _, n := range ns {
		for i := max(n.First, first); i <= n.Commit; i++ {
			a, b := n.Entries[i-n.First], best.Entries[i-first]
			if a.GetTerm() != b.GetTerm() || a.GetType() != b.GetType() || !bytes.Equal(a.Data, b.Data) {
				return nil, fmt.Errorf("conflicting committed entries at index %d in %s and %s", i, n.Dir, best.Dir)
			}
		}
	}
	return best.Entries[:best.Commit-first+1], nil
}

// Read reads the log of the stopped internal/wl node whose data dir is dir.
func Read(dir string) (Node, error) {
	l, err := wl.ReadLog(nil, dir)
	return Node{Dir: dir, Commit: l.Commit, First: l.Base + 1, Entries: l.Entries}, err
}

// ReadMain reads the log of the stopped node of main's engine in dir from
// a private copy of its files, so that recovery's repairs never touch
// them.
func ReadMain(dir string) (Node, error) {
	src := filepath.Join(dir, "wal")
	names, err := os.ReadDir(src)
	if err != nil {
		return Node{}, err
	}
	tmp, err := os.MkdirTemp("", "committed-wal-")
	if err != nil {
		return Node{}, err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck // a leftover temporary copy is harmless
	for _, n := range names {
		b, e := os.ReadFile(filepath.Join(src, n.Name()))
		if e != nil {
			return Node{}, e
		}
		if e = os.WriteFile(filepath.Join(tmp, n.Name()), b, 0o600); e != nil {
			return Node{}, e
		}
	}
	fs, err := osfs.New(tmp)
	if err != nil {
		return Node{}, err
	}
	w, r, err := tigerwal.Open(fs.WithoutSync())
	if err != nil {
		return Node{}, err
	}
	n := Node{Commit: r.HardState.Commit, First: r.Root.Index + 1, Dir: dir}
	// An archive of every segment, as the chaos runs' -env keeps, holds
	// the log from its first entry, below the checkpoint too.
	if w.Has(1) {
		n.First = 1
	}
	for i := n.First; i <= r.Last; i++ {
		e, err := w.Entry(i)
		if err != nil {
			return Node{}, errors.Join(err, w.Close())
		}
		n.Entries = append(n.Entries, e)
	}
	return n, w.Close()
}
