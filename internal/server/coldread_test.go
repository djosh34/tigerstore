package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// readGate holds every value read from a log while it is shut.
type readGate struct {
	mu    sync.Mutex
	shut  chan struct{}
	waits atomic.Int64
}

func (g *readGate) wait() {
	g.mu.Lock()
	shut := g.shut
	g.mu.Unlock()
	if shut != nil {
		g.waits.Add(1)
		<-shut
	}
}

func (g *readGate) hold() {
	g.mu.Lock()
	g.shut = make(chan struct{})
	g.mu.Unlock()
}

// release lets every held read go on; it does nothing if none is held.
func (g *readGate) release() {
	g.mu.Lock()
	if g.shut != nil {
		close(g.shut)
		g.shut = nil
	}
	g.mu.Unlock()
}

// stall is how long the test holds value reads in each phase.
const stall = 2 * time.Second

// result is the answer to one request sent while reads are held.
type result struct {
	name string
	resp proto.Message
	err  error
	at   time.Time
}

// TestColdReadsStall holds every value read from disk on every node for
// 2 s at a time, with every value the requests need on disk only.
//   - Every kind of request whose response carries values, sent to a
//     follower, gets no answer while its reads are held. Meanwhile
//     compare-and-swap writes on the revision finish, the cluster
//     compacts, and no node starts an election. Afterwards every answer
//     is right, watch events included.
//   - A transaction that compares a value on disk holds up ordered apply,
//     but not heartbeats: no election, and every node answers Status.
//   - With the leader stopped while reads are held, the others elect a
//     new one.
func TestColdReadsStall(t *testing.T) {
	gate := &readGate{}
	c := newClusterWith(t, func(c *cluster) {
		c.beforeRead = gate.wait
		c.segmentBytes = 16 << 10
		c.coreConfig = func(cfg *core.Config) {
			cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
			cfg.CheckpointBytes, cfg.CheckpointRatio = 32<<10, 2
		}
	})
	// Before the nodes stop, so that a failed test does not leave them
	// waiting for reads.
	t.Cleanup(gate.release)
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	conns := map[uint64]*grpc.ClientConn{}
	for id := uint64(1); id <= 3; id++ {
		conns[id] = c.dial(id, cert)
	}
	status := func(id uint64) (*pb.StatusResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		return pb.NewMaintenanceClient(conns[id]).Status(ctx, &pb.StatusRequest{})
	}
	st := retry(t, func(context.Context) (*pb.StatusResponse, error) {
		st, err := status(1)
		if err == nil && st.Leader == 0 {
			err = errors.New("no leader")
		}
		return st, err
	})
	leader, term := st.Leader, st.RaftTerm
	f := leader%3 + 1
	kv := pb.NewKVClient(conns[f])
	// Every call has a deadline, so that a node whose committer waits
	// for a read fails the test instead of hanging it.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	timed := func() context.Context {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	// Values on disk only: a to e, then 64 KiB of other keys.
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte(k), Value: []byte("value-" + k)})
		})
	}
	fill := func(prefix string) {
		for i := range 64 {
			if _, err := kv.Put(timed(), &pb.PutRequest{Key: fmt.Appendf(nil, "%s%02d", prefix, i), Value: make([]byte, 1<<10)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	fill("f")
	watch := func(c *pb.WatchCreateRequest) pb.Watch_WatchClient {
		w, err := pb.NewWatchClient(conns[f]).Watch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: c}}); err != nil {
			t.Fatal(err)
		}
		if r, err := w.Recv(); err != nil || !r.Created {
			t.Fatalf("watch: %v, %v", r, err)
		}
		return w
	}
	prevs := watch(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("d"), PrevKv: true})
	noTerm := func() {
		t.Helper()
		for id := uint64(1); id <= 3; id++ {
			st, err := status(id)
			if err != nil || st.RaftTerm != term || st.Leader != leader {
				t.Fatalf("node %d: %v, %v; want term %d and leader %d", id, st, err, term, leader)
			}
		}
	}

	// Phase 1: every kind of response with values.
	gate.hold()
	held := time.Now()
	results := make(chan result, 32)
	send := func(name string, do func() (proto.Message, error)) {
		go func() {
			resp, err := do()
			results <- result{name: name, resp: resp, err: err, at: time.Now()}
		}()
	}
	all := &pb.RangeRequest{Key: []byte("d"), RangeEnd: []byte("f")}
	mod := func(rev int64) []*pb.Compare {
		return []*pb.Compare{{Key: []byte("zz"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}}
	}
	ops := func(k string, write bool) []*pb.RequestOp {
		var out []*pb.RequestOp
		if write {
			out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("t"), Value: []byte("1")}}})
		}
		return append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(k)}}})
	}
	send("get", func() (proto.Message, error) { return kv.Range(ctx, &pb.RangeRequest{Key: []byte("d")}) })
	send("list", func() (proto.Message, error) { return kv.Range(ctx, all) })
	send("rangestream", func() (proto.Message, error) {
		s, err := kv.RangeStream(ctx, all)
		if err != nil {
			return nil, err
		}
		out := &pb.RangeResponse{}
		for {
			r, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			out.Kvs = append(out.Kvs, r.RangeResponse.Kvs...)
		}
	})
	send("put", func() (proto.Message, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("new"), PrevKv: true})
	})
	send("delete", func() (proto.Message, error) {
		return kv.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("c"), PrevKv: true})
	})
	send("txn read", func() (proto.Message, error) {
		return kv.Txn(ctx, &pb.TxnRequest{Compare: mod(0), Success: ops("d", false), Failure: ops("e", false)})
	})
	send("txn success", func() (proto.Message, error) {
		return kv.Txn(ctx, &pb.TxnRequest{Compare: mod(0), Success: ops("d", true), Failure: ops("e", true)})
	})
	send("txn failure", func() (proto.Message, error) {
		return kv.Txn(ctx, &pb.TxnRequest{Compare: mod(9), Success: ops("d", true), Failure: ops("e", true)})
	})
	const kinds = 8
	// A watch from the start catches up with a value on disk.
	catchUp := watch(&pb.WatchCreateRequest{Key: []byte("e"), StartRevision: 1})

	// Meanwhile: compare-and-swap writes on the revision, a compaction and
	// more writes, which cut checkpoints and drop old segments.
	var rev int64
	for i := range 5 {
		start := time.Now()
		cas := &pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("m"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("m"), Value: fmt.Appendf(nil, "%d", i)}}}},
		}
		r, err := kv.Txn(timed(), cas)
		if err != nil || !r.Succeeded {
			t.Fatalf("compare-and-swap %d: %v, %v", i, r, err)
		}
		if d := time.Since(start); d > stall/4 {
			t.Fatalf("compare-and-swap %d took %v", i, d)
		}
		rev = r.Header.Revision
	}
	if _, err := kv.Compact(timed(), &pb.CompactionRequest{Revision: rev}); err != nil {
		t.Fatal(err)
	}
	fill("g")
	noTerm()
	time.Sleep(time.Until(held.Add(stall)))
	noTerm()
	select {
	case r := <-results:
		t.Fatalf("%s answered while its values were held: %v, %v", r.name, r.resp, r.err)
	default:
	}
	if gate.waits.Load() == 0 {
		t.Fatal("no value read waited")
	}
	gate.release()
	got := map[string]proto.Message{}
	for range kinds {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("%s: %v", r.name, r.err)
			}
			got[r.name] = r.resp
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d answers after the reads were released", len(got))
		}
	}
	values := func(kvs []*mvccpb.KeyValue) string {
		var out []string
		for _, kv := range kvs {
			out = append(out, string(kv.Key)+"="+string(kv.Value))
		}
		return fmt.Sprint(out)
	}
	lastRange := func(m proto.Message) []*mvccpb.KeyValue {
		rs := m.(*pb.TxnResponse).Responses
		return rs[len(rs)-1].GetResponseRange().Kvs
	}
	for name, want := range map[string]string{
		"get":         "[d=value-d]",
		"list":        "[d=value-d e=value-e]",
		"rangestream": "[d=value-d e=value-e]",
		"put":         "[a=value-a]",
		"delete":      "[c=value-c]",
		"txn read":    "[d=value-d]",
		"txn success": "[d=value-d]",
		"txn failure": "[e=value-e]",
	} {
		var kvs []*mvccpb.KeyValue
		switch r := got[name].(type) {
		case *pb.RangeResponse:
			kvs = r.Kvs
		case *pb.PutResponse:
			kvs = []*mvccpb.KeyValue{r.PrevKv}
		case *pb.DeleteRangeResponse:
			kvs = r.PrevKvs
		case *pb.TxnResponse:
			kvs = lastRange(r)
		}
		if v := values(kvs); v != want {
			t.Fatalf("%s: %s, want %s", name, v, want)
		}
	}
	var evs []*mvccpb.Event
	for len(evs) < 2 {
		r, err := prevs.Recv()
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, r.Events...)
	}
	put := evs[0]
	if put.Kv.Key[0] != 'a' {
		put = evs[1]
	}
	del := evs[0]
	if del == put {
		del = evs[1]
	}
	if string(put.Kv.Value) != "new" || string(put.PrevKv.Value) != "value-a" || del.Type != mvccpb.Event_DELETE || string(del.PrevKv.Value) != "value-c" {
		t.Fatalf("watch events with previous values: %v", evs)
	}
	if r, err := catchUp.Recv(); err != nil || len(r.Events) != 1 || string(r.Events[0].Kv.Value) != "value-e" {
		t.Fatalf("catch-up: %v, %v", r, err)
	}

	// Phase 2: a value compare on disk holds up ordered apply, not the
	// committer.
	gate.hold()
	held = time.Now()
	send("value compare", func() (proto.Message, error) {
		return kv.Txn(ctx, &pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("b"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("value-b")}}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("swapped")}}}},
		})
	})
	for time.Since(held) < stall {
		noTerm()
		time.Sleep(100 * time.Millisecond)
	}
	gate.release()
	select {
	case r := <-results:
		// The request may time out while apply waits, as in etcd, and
		// still apply.
		if r.err != nil && !errors.Is(rpctypes.Error(r.err), rpctypes.ErrTimeout) {
			t.Fatalf("value compare: %v", r.err)
		}
		if r.err == nil && !r.resp.(*pb.TxnResponse).Succeeded {
			t.Fatalf("value compare failed: %v", r.resp)
		}
		if r.err == nil && r.at.Before(held.Add(stall)) {
			t.Fatal("the value compare answered while its value was held")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no answer to the value compare")
	}
	if r, err := kv.Range(timed(), &pb.RangeRequest{Key: []byte("b")}); err != nil || values(r.Kvs) != "[b=swapped]" {
		t.Fatalf("b after the value compare: %v, %v", r, err)
	}

	// Phase 3: an election while reads are held.
	gate.hold()
	held = time.Now()
	send("list", func() (proto.Message, error) { return kv.Range(ctx, all) })
	c.running[leader].stop()
	delete(c.running, leader)
	for {
		st, err := status(f)
		if err == nil && st.Leader != 0 && st.Leader != leader {
			break
		}
		if time.Since(held) > stall {
			t.Fatalf("no new leader while reads were held: %v, %v", st, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case r := <-results:
		t.Fatalf("%s answered while its values were held: %v, %v", r.name, r.resp, r.err)
	default:
	}
	gate.release()
	select {
	case r := <-results:
		if r.err != nil || values(r.resp.(*pb.RangeResponse).Kvs) != "[d=value-d e=value-e]" {
			t.Fatalf("list after the election: %v, %v", r.resp, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no answer to the list after the election")
	}
}
