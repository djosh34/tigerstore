package batchfront

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/server"
	"go.etcd.io/raft/v3/raftpb"
)

type discardPeer struct{}

func (discardPeer) Send(*raftpb.Message) {}
func (discardPeer) Close() error         { return nil }

func testConn(t *testing.T) (*conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := New(nil, server.Config{}, "full")
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{Conn: a, s: s, ctx: ctx, cancel: cancel, out: make(chan []encoded, 64), control: make(chan control, 256), budgets: make(map[uint32]*budget)}
	if s.memory != nil {
		c.budgetSlots = make([]budget, MaxStreams)
	}
	t.Cleanup(func() { c.close(); _ = b.Close() })
	return c, b
}

func TestPrebuiltResponseOwnedAndFramed(t *testing.T) {
	for _, size := range []int{0, 1, 16380, 16384, 32768, 70000} {
		c, _ := testConn(t)
		if !c.start(1) || !c.receive(1, 100) {
			t.Fatal("start")
		}
		value := bytes.Repeat([]byte("x"), size)
		resp := &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: value}}}
		expected, err := proto.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		c.Complete([]node.Completion{{Tag: 1, Resp: resp}})
		for i := range value {
			value[i] = 'z'
		}
		e := (<-c.out)[0]
		if len(e.wire) != e.reserved {
			t.Fatalf("size=%d wire=%d reserved=%d", size, len(e.wire), e.reserved)
		}
		f := http2.NewFramer(io.Discard, bytes.NewReader(e.wire))
		f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
		var data []byte
		var trailers bool
		for {
			frame, err := f.ReadFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			switch v := frame.(type) {
			case *http2.DataFrame:
				data = append(data, v.Data()...)
			case *http2.MetaHeadersFrame:
				if v.StreamEnded() {
					trailers = true
					if v.Fields[0].Value != "0" {
						t.Fatal(v.Fields)
					}
				}
			}
		}
		if !trailers || len(data) < 5 || int(binary.BigEndian.Uint32(data[1:5])) != len(expected) || !bytes.Equal(data[5:], expected) {
			t.Fatalf("owned wire mismatch size=%d", size)
		}
		c.mu.Lock()
		input := c.inputBytes
		output := c.outputBytes
		c.mu.Unlock()
		if input != 0 || output != len(e.wire) {
			t.Fatalf("input=%d output=%d", input, output)
		}
	}
}

func TestSlowClientCapBeforeLargeEncode(t *testing.T) {
	c, _ := testConn(t)
	c.start(1)
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Value: make([]byte, MaxResponseBytes)}}}}})
	if c.ctx.Err() == nil || len(c.out) != 0 || c.s.Stats().OutputHighwater != 0 {
		t.Fatal("huge response was allocated/enqueued instead of rejected")
	}
}

func TestQueueFullNeverBlocksCompletion(t *testing.T) {
	c, _ := testConn(t)
	c.out = make(chan []encoded, 1)
	c.start(1)
	c.start(3)
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.PutResponse{}}})
	done := make(chan struct{})
	go func() { c.Complete([]node.Completion{{Tag: 3, Resp: &pb.PutResponse{}}}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owner blocked on writer")
	}
	if c.ctx.Err() == nil {
		t.Fatal("full queue did not close slow client")
	}
}

func TestFlowBlockedBytesStayReserved(t *testing.T) {
	c, peer := testConn(t)
	c.start(1)
	done := make(chan error, 1)
	go func() { done <- c.writeLoop() }()
	f := http2.NewFramer(peer, peer)
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := f.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	}
	c.sendControl(control{setInitial: true, initial: 0, ackSettings: true, open: true, id: 1})
	if _, err := f.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 2}}}})
	if _, err := f.ReadFrame(); err != nil {
		t.Fatal(err)
	} // initial response headers
	c.mu.Lock()
	before := c.outputBytes
	c.mu.Unlock()
	if before == 0 {
		t.Fatal("writer forgot flow-blocked bytes")
	}
	if c.s.memory != nil && c.s.MemoryStats().Output.InUseBytes == 0 {
		t.Fatal("writer recycled flow-blocked chunk")
	}
	c.sendControl(control{id: 1, delta: 3})
	frame, err := f.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := frame.(*http2.DataFrame); !ok || len(d.Data()) != 3 {
		t.Fatalf("partial credit produced %T %v", frame, frame)
	}
	c.mu.Lock()
	after := c.outputBytes
	c.mu.Unlock()
	if after != before {
		t.Fatal("partial send released reserved buffer")
	}
	c.sendControl(control{id: 1, delta: 65535})
	for {
		frame, err = f.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if h, ok := frame.(*http2.MetaHeadersFrame); ok && h.StreamEnded() {
			break
		}
	}
	until := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		remaining := c.outputBytes
		c.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("flushed response retains bytes")
		}
		time.Sleep(time.Millisecond)
	}
	if c.s.memory != nil && c.s.MemoryStats().Output.InUseBytes != 0 {
		t.Fatal("flushed reply retained chunk")
	}
	c.close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPartialAndAdmissionCaps(t *testing.T) {
	c, _ := testConn(t)
	for i := range MaxStreams {
		if !c.start(uint32(i*2 + 1)) {
			t.Fatal(i)
		}
	}
	if c.start(9999) {
		t.Fatal("partial stream cap")
	}
	c.reset(1)
	if !c.start(9999) {
		t.Fatal("partial reset did not return slot")
	}
	c.admit(3)
	if !c.receive(3, maxMessage+5) {
		t.Fatal("max accepted request")
	}
	c.reset(3)
	c.mu.Lock()
	retained := c.inputBytes
	c.mu.Unlock()
	if retained == 0 {
		t.Fatal("RST freed admitted payload before owner completion")
	}
	c.Complete([]node.Completion{{Tag: 3, Err: status.Error(codes.Canceled, "reset")}})
	c.mu.Lock()
	retained = c.inputBytes
	c.mu.Unlock()
	if retained != 0 || len(c.out) != 0 {
		t.Fatal("reset completion retained body or enqueued response")
	}
}

func startNode(t *testing.T) *node.Node {
	t.Helper()
	n, err := node.New(node.Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, discardPeer{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	until := time.Now().Add(2 * time.Second)
	for n.Leader() == 0 {
		if time.Now().After(until) {
			t.Fatal("no leader")
		}
		time.Sleep(time.Millisecond)
	}
	return n
}

func TestGRPCModes(t *testing.T) {
	for _, mode := range []string{"rpc", "admit", "full"} {
		t.Run(mode, func(t *testing.T) {
			n := startNode(t)
			s, err := New(n, server.Config{ClusterID: 7, Members: []server.Member{{ID: 1, Name: "one"}}}, mode)
			if err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- s.Serve(ln) }()
			t.Cleanup(func() {
				if err := s.Stop(); err != nil {
					t.Error(err)
				}
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cc.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			kv := pb.NewKVClient(cc)
			const clients = 16
			var wg sync.WaitGroup
			failures := make(chan error, clients)
			for i := range clients {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, err := kv.Put(ctx, &pb.PutRequest{Key: []byte{byte(i + 1)}, Value: []byte("v")})
					failures <- err
				}(i)
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte{1}, RangeEnd: []byte{17}})
			if err != nil || len(r.GetKvs()) != clients {
				t.Fatalf("range %v %v", r, err)
			}
			tx, err := kv.Txn(ctx, &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte{1}, Target: pb.Compare_VALUE, TargetUnion: &pb.Compare_Value{Value: []byte("v")}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte{1}, Value: []byte("new"), PrevKv: true}}}}})
			if err != nil || !tx.GetSucceeded() {
				t.Fatalf("txn %v %v", tx, err)
			}
			st, err := pb.NewMaintenanceClient(cc).Status(ctx, &pb.StatusRequest{})
			if err != nil || st.Leader != 1 {
				t.Fatalf("status %v %v", st, err)
			}
			members, err := pb.NewClusterClient(cc).MemberList(ctx, &pb.MemberListRequest{})
			if err != nil || len(members.GetMembers()) != 1 {
				t.Fatalf("members %v %v", members, err)
			}
			_, err = pb.NewAuthClient(cc).Authenticate(ctx, &pb.AuthenticateRequest{})
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("unsupported %v", err)
			}
			if s.Stats().Commands != clients+2 {
				t.Fatalf("stats %+v", s.Stats())
			}
		})
	}
}
