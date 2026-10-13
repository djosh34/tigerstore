package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
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

// persistAll makes jobs durable in order, as the driver does.
func (s *single) persistAll(jobs []PersistJob) {
	s.t.Helper()
	for _, j := range jobs {
		s.log.Append(j.HardState, j.Entries)
	}
	if err := s.log.Sync(); err != nil {
		s.t.Fatal(err)
	}
	for _, j := range jobs {
		s.n.Persisted(j)
	}
}

// TestForwardedWritesDoNotHoldLocalOnes keeps forwarded proposals arriving
// while each append is on its way: a local write must still go out after
// the append of the batch it waited behind.
func TestForwardedWritesDoNotHoldLocalOnes(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	forward := func(i uint64) {
		data, err := encodeBatch(2, 9, []*pending{{req: Request{ID: i, Op: &pb.PutRequest{Key: []byte("f"), Value: []byte("x")}}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.n.Step(&raftpb.Message{
			Type: raftpb.MsgProp.Enum(), From: new(uint64(1)), To: new(uint64(1)),
			Entries: []*raftpb.Entry{{Data: data}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}})
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	held := eff.Persist
	s.n.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}})
	for round := range 5 {
		// The forwarded proposal is processed at once, so its append goes
		// out before the earlier append is durable, as in the driver.
		forward(uint64(100 + round))
		first, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		s.persistAll(held)
		second, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range append(first.Replies, second.Replies...) {
			if r.ID == 2 {
				if r.Err != nil {
					t.Fatalf("local write: %v", r.Err)
				}
				return
			}
		}
		held = append(first.Persist, second.Persist...)
		s.n.Tick()
	}
	t.Fatal("a local write never went out while forwarded proposals kept arriving")
}

// TestByteCapSendsAnEntryEarly fills the byte cap while the leader's
// append is in flight: a full entry goes out at once.
func TestByteCapSendsAnEntryEarly(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.n.cfg.MaxBatchBytes = 400
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}})
	if _, err := s.n.Process(); err != nil {
		t.Fatal(err)
	}
	var sent []int
	for i := range 20 {
		s.n.Submit(Request{ID: uint64(i + 2), Op: &pb.LeaseKeepAliveRequest{}})
		eff, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		sent = append(sent, writesIn(t, eff.Persist)...)
	}
	if len(sent) == 0 {
		t.Fatal("20 empty writes waited without limit behind one append")
	}
	for _, n := range sent {
		if n*(batchFraming+2) > 400+batchFraming+2 {
			t.Fatalf("an entry of %d writes went past the cap", n)
		}
	}
}

// TestHardStateAppendHoldsNothing checks that a write is not held behind an
// append that only moves the commit index.
func TestHardStateAppendHoldsNothing(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}})
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	s.persistAll(eff.Persist)
	eff, err = s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range eff.Persist {
		if len(j.Entries) > 0 {
			t.Fatalf("expected only a hard state, got %d entries", len(j.Entries))
		}
	}
	s.n.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}})
	eff, err = s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if got := writesIn(t, eff.Persist); fmt.Sprint(got) != "[1]" {
		t.Fatalf("write behind a hard-state append went out as %v, want at once", got)
	}
}
