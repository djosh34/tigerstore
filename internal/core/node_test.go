package core

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// single runs a one-node cluster on a simulated disk.
type single struct {
	n *Node
}

// newMember starts node 1 of peers on an empty simulated disk, without
// ticking it.
func newMember(t *testing.T, peers []uint64) *single {
	t.Helper()
	disk := simfs.New()
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(DefaultConfig(1, peers), rec, w)
	if err != nil {
		t.Fatal(err)
	}
	return &single{n: n}
}
