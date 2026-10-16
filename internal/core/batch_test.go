package core

import (
	"fmt"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
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

// TestWritesWaitForTheLeadersAppends holds the leader's appends on their
// way to disk: a write with fewer than ownDepth appends in flight goes out
// at once, and writes that arrive while ownDepth are in flight go out as
// one entry once the oldest is durable.
func TestWritesWaitForTheLeadersAppends(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	var held [][]PersistJob
	for i := range ownDepth {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("0")}})
		eff, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		if got := writesIn(t, eff.Persist); fmt.Sprint(got) != "[1]" {
			t.Fatalf("write %d went out as %v, want at once in one entry", i, got)
		}
		held = append(held, eff.Persist)
	}
	for i := range 50 {
		s.n.Submit(Request{ID: uint64(ownDepth + i + 1), Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("x")}})
		e, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		if got := writesIn(t, e.Persist); len(got) != 0 {
			t.Fatalf("write %d went out while %d appends were in flight: %v", i, ownDepth, got)
		}
	}
	s.persistAll(held[0])
	eff, err := s.n.Process()
	if err != nil {
		t.Fatal(err)
	}
	if got := writesIn(t, eff.Persist); fmt.Sprint(got) != "[50]" {
		t.Fatalf("held writes went out as %v, want one entry of 50", got)
	}
	// Finish everything; every write is answered.
	replies := len(eff.Replies)
	for _, jobs := range append(held[1:], eff.Persist) {
		s.persistAll(jobs)
	}
	for _, r := range s.run() {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		replies++
	}
	if replies != 50+ownDepth {
		t.Fatalf("%d replies, want %d", replies, 50+ownDepth)
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
		data, err := encodeBatch(1, 2, 9, []*pending{{req: Request{ID: i, Op: &pb.PutRequest{Key: []byte("f"), Value: []byte("x")}}}}, 0)
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
		held = slices.Concat(first.Persist, second.Persist)
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
	// An empty keepalive counts only its framing, so 9 fit in 400 bytes.
	for _, n := range sent {
		if n*batchFraming > s.n.cfg.MaxBatchBytes {
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

// TestMustSync syncs every job but one that only moves the commit index.
func TestMustSync(t *testing.T) {
	for _, c := range []struct {
		job  PersistJob
		want bool
	}{
		{PersistJob{HardState: &wal.HardState{Term: 2, Commit: 9}}, false},
		{PersistJob{HardState: &wal.HardState{Term: 2, Commit: 9}, newVote: true}, true},
		{PersistJob{Entries: []*raftpb.Entry{{}}}, true},
		{PersistJob{Root: &wal.Root{Index: 5}}, true},
		{PersistJob{Install: &Install{}}, true},
	} {
		if got := c.job.MustSync(); got != c.want {
			t.Errorf("MustSync of %+v = %v, want %v", c.job, got, c.want)
		}
	}
}
