package node

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The prototype asks whether holding Ready and Submit through an append
// produces one next physical group without releasing storage responses early.
type testTransport struct{ sent chan *raftpb.Message }

func (t *testTransport) Send(m *raftpb.Message) { t.sent <- m }
func (*testTransport) Close() error             { return nil }

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for driver")
		var zero T
		return zero
	}
}

func testNode(t *testing.T, peers []uint64) (*Node, *simfs.Disk) {
	t.Helper()
	disk := simfs.New()
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := core.DefaultConfig(1, peers)
	cfg.MaxBatchBytes = 64
	c, err := core.New(cfg, rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) == 1 {
		for range 50 {
			c.Tick()
			for {
				eff, err := c.Process()
				if err != nil {
					t.Fatal(err)
				}
				if len(eff.Persist) == 0 {
					break
				}
				for _, j := range eff.Persist {
					w.Append(j.HardState, j.Entries)
				}
				if err := w.Sync(); err != nil {
					t.Fatal(err)
				}
				for _, j := range eff.Persist {
					c.Persisted(j)
				}
			}
			if c.Leader() == 1 {
				break
			}
		}
		if c.Leader() != 1 {
			t.Fatal("single node did not elect itself")
		}
	}
	return &Node{
		cfg: Config{Core: cfg, Tick: time.Hour}, core: c, wal: w,
		inbox: make(chan input), persistQ: make(chan []core.PersistJob, 1),
		persisted: make(chan persistDone), done: make(chan struct{}),
	}, disk
}

func startCommit(t *testing.T, n *Node) (*testTransport, context.Context, func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	tr := &testTransport{sent: make(chan *raftpb.Message, 100)}
	result := make(chan error, 1)
	go func() { result <- n.commit(ctx, tr) }()
	stopped := false
	stop := func() error {
		cancel()
		stopped = true
		return receive(t, result)
	}
	t.Cleanup(func() {
		if !stopped {
			if err := stop(); err != nil {
				t.Error(err)
			}
		}
		if err := n.wal.Close(); err != nil {
			t.Error(err)
		}
	})
	return tr, ctx, stop
}

func submit(t *testing.T, n *Node, ctx context.Context, id uint64) *request {
	t.Helper()
	r := &request{id: id, op: &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 20)}, reply: make(chan core.Reply, 1)}
	if err := n.put(ctx, input{req: r}); err != nil {
		t.Fatal(err)
	}
	return r
}

func complete(t *testing.T, n *Node, jobs []core.PersistJob) {
	t.Helper()
	for _, j := range jobs {
		n.wal.Append(j.HardState, j.Entries)
	}
	if err := n.wal.Sync(); err != nil {
		t.Fatal(err)
	}
	n.persisted <- persistDone{jobs: jobs}
}

func TestStalledAppendHoldsSubmit(t *testing.T) {
	n, _ := testNode(t, []uint64{1})
	_, ctx, stop := startCommit(t, n)
	submit(t, n, ctx, 1)
	receive(t, n.persistQ)
	// These requests cross the byte cap. Submit must not see them yet.
	for id := uint64(2); id < 9; id++ {
		submit(t, n, ctx, id)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	eff, err := n.core.Process()
	if err != nil || len(eff.Persist) != 0 || len(eff.Replies) != 0 {
		t.Fatalf("work escaped the append gate: persist=%d replies=%d err=%v", len(eff.Persist), len(eff.Replies), err)
	}
}

func TestAppendCompletionBatchesAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "error"}[fail], func(t *testing.T) {
			n, _ := testNode(t, []uint64{1})
			_, ctx, stop := startCommit(t, n)
			first := submit(t, n, ctx, 1)
			jobs := receive(t, n.persistQ)
			var requests []*request
			for id := uint64(2); id < 9; id++ {
				requests = append(requests, submit(t, n, ctx, id))
			}
			if len(first.reply) != 0 || len(n.persistQ) != 0 {
				t.Fatal("ack or next append before durability")
			}
			if fail {
				want := errors.New("fsync failed")
				n.persisted <- persistDone{err: want}
				// The unbuffered completion was accepted before cancellation.
				select {
				case <-first.reply:
					t.Fatal("reply after failed durability")
				default:
				}
				if err := stop(); !errors.Is(err, want) {
					t.Fatalf("commit error = %v", err)
				}
				if n.core.Applied() != 1 || len(first.reply) != 0 || len(n.persistQ) != 0 {
					t.Fatal("failed append applied, replied or issued more work")
				}
				return
			}
			complete(t, n, jobs)
			jobs = receive(t, n.persistQ)
			count, entries := 0, 0
			for _, j := range jobs {
				for _, e := range j.Entries {
					entries++
					batch := 0
					if err := core.DecodeBatch(e.Data, func(_, _, _ uint64, _ proto.Message) error {
						batch++
						count++
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					if batch > 2 {
						t.Fatalf("entry exceeded request byte cap: %d operations", batch)
					}
				}
			}
			// Several capped entries share one physical group. There is no
			// global MaxBatchBytes cap on the bytes sent to one fsync.
			if count != 7 || entries != 4 {
				t.Fatalf("accumulated group = %d operations in %d entries", count, entries)
			}
			for _, r := range requests {
				if len(r.reply) != 0 {
					t.Fatal("next group replied before durability")
				}
			}
			complete(t, n, jobs)
			for _, r := range append(requests, first) {
				if rep := receive(t, r.reply); rep.Err != nil {
					t.Fatal(rep.Err)
				}
			}
		})
	}
}

func TestFollowerHoldsReadyThroughAppend(t *testing.T) {
	n, _ := testNode(t, []uint64{1, 2, 3})
	tr, ctx, _ := startCommit(t, n)
	appendEntry := func(index uint64) {
		t.Helper()
		m := &raftpb.Message{
			Type: raftpb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)),
			Term: new(uint64(1)), Index: new(index - 1),
			LogTerm: new(uint64(1)), Commit: new(index),
			Entries: []*raftpb.Entry{{Index: new(index), Term: new(uint64(1)), Type: raftpb.EntryNormal.Enum()}},
		}
		if index == 1 {
			m.LogTerm = new(uint64(0))
		}
		if err := n.put(ctx, input{msg: m}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(1)
	jobs := receive(t, n.persistQ)
	appendEntry(2)
	appendEntry(3)
	if len(n.persistQ) != 0 || len(tr.sent) != 0 {
		t.Fatal("follower append or ack before durability")
	}
	complete(t, n, jobs)
	jobs = receive(t, n.persistQ)
	var indices []uint64
	for _, j := range jobs {
		for _, e := range j.Entries {
			indices = append(indices, e.GetIndex())
		}
	}
	if len(indices) != 2 || indices[0] != 2 || indices[1] != 3 {
		t.Fatalf("follower accumulated entries = %v", indices)
	}
	if m := receive(t, tr.sent); m.GetType() != raftpb.MsgAppResp || m.GetIndex() != 1 {
		t.Fatalf("first durable ack = %v", m)
	}
	if len(tr.sent) != 0 {
		t.Fatal("follower acknowledged the unsynced group")
	}
	complete(t, n, jobs)
	for _, index := range []uint64{2, 3} {
		if m := receive(t, tr.sent); m.GetType() != raftpb.MsgAppResp || m.GetIndex() != index {
			t.Fatalf("durable ack = %v, want index %d", m, index)
		}
	}
}

func TestPersistCoalescesHardStateAndReplacesSuffix(t *testing.T) {
	n, disk := testNode(t, []uint64{1, 2, 3})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); n.persist(ctx) }()
	t.Cleanup(func() {
		cancel()
		receive(t, finished)
		if err := n.wal.Close(); err != nil {
			t.Error(err)
		}
	})
	entry := func(index, term uint64) *raftpb.Entry {
		return &raftpb.Entry{Index: new(index), Term: new(term), Type: raftpb.EntryNormal.Enum()}
	}
	jobs := []core.PersistJob{
		{HardState: &wal.HardState{Term: 1, Vote: 2, Commit: 1}, Entries: []*raftpb.Entry{entry(1, 1), entry(2, 1)}},
		{HardState: &wal.HardState{Term: 2, Vote: 3, Commit: 2}, Entries: []*raftpb.Entry{entry(2, 2)}},
		{Entries: []*raftpb.Entry{entry(3, 2)}},
	}
	n.persistQ <- jobs
	done := receive(t, n.persisted)
	if done.err != nil || len(done.jobs) != 3 {
		t.Fatalf("persist completion = %v, %d jobs", done.err, len(done.jobs))
	}
	cancel()
	receive(t, finished)
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if rec.HardState != *jobs[1].HardState || len(rec.Entries) != 3 || rec.Entries[1].GetTerm() != 2 {
		t.Fatalf("recovered suffix/state = %+v", rec)
	}
	names, err := disk.List()
	if err != nil {
		t.Fatal(err)
	}
	f, err := disk.Open(names[0])
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	states := 0
	for off := 20; off < len(data); {
		if data[off+8] == 2 {
			states++
		}
		off += 9 + int(binary.LittleEndian.Uint32(data[off:]))
	}
	if states != 1 {
		t.Fatalf("hard-state records = %d, want one", states)
	}
}

func TestPersistFailureDoesNotCompleteJobs(t *testing.T) {
	n, disk := testNode(t, []uint64{1, 2, 3})
	want := errors.New("fsync failed")
	disk.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpSync {
			return want
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	finished := make(chan struct{})
	go func() { defer close(finished); n.persist(ctx) }()
	t.Cleanup(func() {
		cancel()
		receive(t, finished)
		if err := n.wal.Close(); err != nil {
			t.Error(err)
		}
	})
	n.persistQ <- []core.PersistJob{{HardState: &wal.HardState{Term: 1, Vote: 2}}}
	done := receive(t, n.persisted)
	if !errors.Is(done.err, want) || len(done.jobs) != 0 {
		t.Fatalf("failed sync completed jobs: %+v", done)
	}
	receive(t, finished)
}
