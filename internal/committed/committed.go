// Package committed reads the Raft log from the WALs of stopped nodes, for
// the checks of the chaos and rolling-upgrade runs.
package committed

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/internal/osfs"
	tigerwal "github.com/djosh34/tigerstore/internal/wal"
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

// Prefix returns the longest committed log, which must start at index 1,
// after checking that every node's committed entries agree with it. A node
// that installed a snapshot holds a shorter log.
func Prefix(ns []Node) ([]*raftpb.Entry, error) {
	if len(ns) == 0 {
		return nil, errors.New("no recovered WALs")
	}
	best := ns[0]
	for _, n := range ns {
		if n.Commit > best.Commit {
			best = n
		}
	}
	if best.First != 1 {
		return nil, fmt.Errorf("the longest committed log, of %s, starts at index %d after a checkpoint", best.Dir, best.First)
	}
	for _, n := range ns {
		for i := n.First; i <= n.Commit; i++ {
			a, b := n.Entries[i-n.First], best.Entries[i-1]
			// Each node writes the bootstrap membership changes itself,
			// with the peer addresses of its own --initial-cluster, which
			// name the proxies; only their terms and types must agree.
			sameData := a.GetType() != raftpb.EntryNormal || bytes.Equal(a.Data, b.Data)
			if a.GetTerm() != b.GetTerm() || a.GetType() != b.GetType() || !sameData {
				return nil, fmt.Errorf("conflicting committed entries at index %d in %s and %s", i, n.Dir, best.Dir)
			}
		}
	}
	return best.Entries[:best.Commit], nil
}

// Read reads the log of the stopped node in dir from a private copy of its
// files, so that recovery's repairs never touch them.
func Read(dir string) (Node, error) {
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
