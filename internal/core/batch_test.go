package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// writesIn counts the client writes in the entries of persist jobs.
func writesIn(t *testing.T, jobs []PersistJob) []int {
	t.Helper()
	var out []int
	for _, j := range jobs {
		for _, e := range j.Entries {
			if len(e.GetData()) == 0 {
				continue
			}
			n := 0
			if err := DecodeBatch(e.GetData(), func(_, _, _ uint64, _ proto.Message) error { n++; return nil }); err != nil {
				t.Fatal(err)
			}
			out = append(out, n)
		}
	}
	return out
}

// TestWritesWaitForTheLeadersAppend holds the leader's append on its way
// to disk: writes that arrive meanwhile go out as one entry once it is
// durable, and a write with no append in flight goes out at once.
func TestWritesWaitForTheLeadersAppend(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("0")}})
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if got := writesIn(t, eff.Persist); fmt.Sprint(got) != "[1]" {
		t.Fatalf("first write went out as %v, want at once in one entry", got)
	}
	held := eff.Persist
	for i := range 50 {
		s.n.Submit(Request{ID: uint64(i + 2), Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("x")}})
		e, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		if got := writesIn(t, e.Persist); len(got) != 0 {
			t.Fatalf("write %d went out while the append was in flight: %v", i, got)
		}
	}
	for _, j := range held {
		s.log.Append(j.HardState, j.Entries)
	}
	if err := s.log.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, j := range held {
		s.n.Persisted(j)
	}
	eff, err = s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if got := writesIn(t, eff.Persist); fmt.Sprint(got) != "[50]" {
		t.Fatalf("held writes went out as %v, want one entry of 50", got)
	}
	// Finish everything; every write is answered.
	for _, j := range eff.Persist {
		s.log.Append(j.HardState, j.Entries)
	}
	if err := s.log.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, j := range eff.Persist {
		s.n.Persisted(j)
	}
	replies := len(eff.Replies)
	for _, r := range s.run() {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		replies++
	}
	if replies != 51 {
		t.Fatalf("%d replies, want 51", replies)
	}
}
