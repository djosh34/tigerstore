package committed

import (
	"path/filepath"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/node"
)

// ReadMain reads a data dir of the previous release's engine, as the
// release test reads the old cluster's: here the one main's own restore
// makes of a snapshot v0.1.0 saved (cmd/tigerstore's upgrade fixture),
// which holds its checkpoint at the snapshot's index and no entry after
// it.
func TestReadMainReadsADataDirOfThePreviousRelease(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "main")
	members := []*pb.Member{{ID: 1, Name: "n1", PeerURLs: []string{"https://127.0.0.1:2380"}}}
	info, err := node.Restore(filepath.Join("..", "..", "cmd", "tigerstore", "testdata", "v010.snapshot"), dir, members, false)
	if err != nil {
		t.Fatal(err)
	}
	n, err := ReadMain(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n.Commit != info.Index || n.First != info.Index+1 || len(n.Entries) != 0 {
		t.Fatalf("the restored data dir reads as commit %d, entries from %d, %d of them; want commit %d and none after it", n.Commit, n.First, len(n.Entries), info.Index)
	}
	if _, err := ReadMain(filepath.Join(t.TempDir(), "none")); err == nil {
		t.Fatal("a data dir with no log read")
	}
}

// Prefix is the longest committed log, once every node's committed
// entries agree with it; it refuses nodes that disagree, a longest log
// that starts elsewhere, and no nodes at all. Validate refuses a node
// whose commit index lies outside its entries.
func TestPrefixIsTheLongestLogTheNodesAgreeOn(t *testing.T) {
	ent := func(term uint64, data string) *raftpb.Entry {
		return &raftpb.Entry{Term: new(term), Type: raftpb.EntryNormal.Enum(), Data: []byte(data)}
	}
	a := Node{Dir: "a", First: 1, Commit: 3, Entries: []*raftpb.Entry{ent(1, "x"), ent(1, "y"), ent(2, "z"), ent(2, "uncommitted")}}
	b := Node{Dir: "b", First: 1, Commit: 2, Entries: []*raftpb.Entry{ent(1, "x"), ent(1, "y")}}
	// c installed a snapshot of the first entry.
	c := Node{Dir: "c", First: 2, Commit: 3, Entries: []*raftpb.Entry{ent(1, "y"), ent(2, "z")}}
	log, err := Prefix([]Node{b, a, c}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 3 || string(log[2].Data) != "z" {
		t.Fatalf("the committed log is %v, want a's first three entries", log)
	}
	for _, n := range []Node{a, b, c} {
		if err := n.Validate(); err != nil {
			t.Errorf("%s: %v", n.Dir, err)
		}
	}
	other := Node{Dir: "d", First: 1, Commit: 2, Entries: []*raftpb.Entry{ent(1, "x"), ent(1, "other")}}
	if _, err := Prefix([]Node{a, other}, 1); err == nil || !strings.Contains(err.Error(), "conflicting committed entries at index 2") {
		t.Errorf("nodes that disagree at index 2: %v", err)
	}
	if _, err := Prefix([]Node{a}, 2); err == nil {
		t.Error("a longest log that starts at 1, asked from 2, was taken")
	}
	if _, err := Prefix(nil, 1); err == nil {
		t.Error("no nodes gave a log")
	}
	if err := (Node{Dir: "e", First: 5, Commit: 9, Entries: []*raftpb.Entry{ent(1, "x")}}).Validate(); err == nil {
		t.Error("a node missing committed entries passed Validate")
	}
}
