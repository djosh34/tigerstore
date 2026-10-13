package batchfront

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/batchmem"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/server"
)

func memoryConn(t *testing.T, mode string) (*conn, net.Conn) {
	t.Helper()
	t.Setenv("TIGERSTORE_BATCH_MEMORY", mode)
	c, peer := testConn(t)
	c.budgetSlots = make([]budget, MaxStreams)
	return c, peer
}

func TestMemoryFlowBlockedWriter(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) { t.Setenv("TIGERSTORE_BATCH_MEMORY", mode); TestFlowBlockedBytesStayReserved(t) })
	}
}

func TestMemoryReceiveDescriptorLifetime(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := memoryConn(t, mode)
			if !c.start(1) || !c.receive(1, 1024) {
				t.Fatal("start")
			}
			body, ok := c.receiveBuffer(1, nil, 1024)
			if !ok {
				t.Fatal("buffer")
			}
			body = body[:1024]
			for i := range body {
				body[i] = 0xa5
			}
			var a admission
			if !c.prepareAdmission(&a, 1) {
				t.Fatal("descriptors")
			}
			descriptor := a.arena
			c.admit(1)
			// Simulate the reader relinquishing a submitted batch. The owner
			// has not completed it yet, so both allocations remain pinned.
			a.arena.Release()
			a.arena = nil
			c.inputArena.Release()
			c.inputArena = nil
			c.reset(1)
			if c.s.MemoryStats().Input.InUseBytes == 0 {
				t.Fatal("reset recycled submitted input")
			}
			var held []*batchmem.Arena
			for {
				x := c.s.memory.admissions.Acquire(0)
				if x == nil {
					break
				}
				if x == descriptor {
					t.Fatal("submitted descriptors reused")
				}
				held = append(held, x)
			}
			if len(held) != memorySlots-1 {
				t.Fatal(len(held))
			}
			if !bytes.Equal(body, bytes.Repeat([]byte{0xa5}, 1024)) {
				t.Fatal("input overwritten")
			}
			c.Complete([]node.Completion{{Tag: 1, Resp: &pb.PutResponse{}}})
			if c.s.MemoryStats().Input.InUseBytes != 0 || len(c.out) != 0 {
				t.Fatal("reset completion retained input or replied")
			}
			for _, x := range held {
				x.Release()
			}
		})
	}
}

func TestMemorySlowClientAndClosedCompletion(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := memoryConn(t, mode)
			c.out = make(chan []encoded, 1)
			c.start(1)
			c.start(3)
			c.Complete([]node.Completion{{Tag: 1, Resp: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Value: bytes.Repeat([]byte{0x4d}, 128<<10)}}}}})
			pinned := (<-c.out)[0]
			before := append([]byte(nil), pinned.wire...)
			// Simulate zero HTTP/2 credit: the writer retains this reply.
			c.Complete([]node.Completion{{Tag: 3, Resp: &pb.PutResponse{}}})
			if !bytes.Equal(pinned.wire, before) {
				t.Fatal("slow reply overwritten")
			}
			c.start(5)
			c.Complete([]node.Completion{{Tag: 5, Resp: &pb.PutResponse{}}})
			if c.ctx.Err() == nil {
				t.Fatal("full queue did not reject")
			}
			for _, e := range <-c.out {
				c.releaseEncoded(e)
			}
			c.releaseEncoded(pinned)
			if c.s.MemoryStats().Output.InUseBytes != 0 {
				t.Fatal("closed writer retained chunks")
			}
			c.start(7)
			c.receive(7, 1024)
			if _, ok := c.receiveBuffer(7, nil, 1024); !ok {
				t.Fatal("receive")
			}
			c.admit(7)
			c.inputArena.Release()
			c.inputArena = nil
			c.Complete([]node.Completion{{Tag: 7, Resp: &pb.PutResponse{}}})
			if c.s.MemoryStats().Input.InUseBytes != 0 {
				t.Fatal("late completion after close leaked input")
			}
		})
	}
}

func TestMemoryLargeResponseClassAndCap(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := memoryConn(t, mode)
			value := make([]byte, 6<<20)
			for i := range 3 {
				id := uint32(i*2 + 1)
				if !c.start(id) {
					t.Fatal("start")
				}
				c.Complete([]node.Completion{{Tag: id, Resp: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Value: value}}}}})
			}
			if c.ctx.Err() == nil || len(c.out) != 2 || c.s.Stats().OutputHighwater > MaxOutputBytes {
				t.Fatal("large response cap")
			}
			if c.s.MemoryStats().Output.InUseBytes != 2*MaxResponseBytes {
				t.Fatalf("large class %+v", c.s.MemoryStats())
			}
			for range 2 {
				for _, e := range <-c.out {
					c.releaseEncoded(e)
				}
			}
			if c.s.MemoryStats().Output.InUseBytes != 0 {
				t.Fatal("large chunks leaked")
			}
		})
	}
}

func TestMemoryHealthyMultiplexedClients(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TIGERSTORE_BATCH_MEMORY", mode)
			n := startNode(t)
			s, err := New(n, server.Config{}, "full")
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connections := make([]*grpc.ClientConn, 16)
			for i := range connections {
				cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				connections[i] = cc
				t.Cleanup(func() {
					if err := cc.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			value := make([]byte, 1024)
			for round := range 2 {
				var wg sync.WaitGroup
				for i := range 256 {
					wg.Go(func() {
						_, err := pb.NewKVClient(connections[i%16]).Put(ctx, &pb.PutRequest{Key: []byte{byte(round + 1), byte(i)}, Value: value})
						if err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
			}
			st := s.MemoryStats()
			if st.Input.Rejected != 0 || st.Output.Rejected != 0 || st.AdmissionRejected != 0 || st.ReplyRejected != 0 {
				t.Fatalf("healthy clients exhausted banks: %+v", st)
			}
		})
	}
}

func TestMemoryUnsubmittedReaderFailure(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := memoryConn(t, mode)
			c.start(1)
			c.receive(1, 1024)
			if _, ok := c.receiveBuffer(1, nil, 1024); !ok {
				t.Fatal("buffer")
			}
			var a admission
			if !c.prepareAdmission(&a, 1) {
				t.Fatal("descriptors")
			}
			a.tags = append(a.tags, 1)
			c.admit(1)
			// A framing error ends the reader before flush. There will be no
			// core completion for this prepared, but unsent, command.
			c.readerCleanup(&a)
			if c.s.MemoryStats().Input.InUseBytes != 0 || len(c.budgets) != 0 {
				t.Fatal("unsent batch leaked")
			}
			var held []*batchmem.Arena
			for {
				x := c.s.memory.admissions.Acquire(0)
				if x == nil {
					break
				}
				held = append(held, x)
			}
			if len(held) != memorySlots {
				t.Fatal("unsent descriptors leaked")
			}
			for _, x := range held {
				x.Release()
			}
		})
	}
}

func TestMemoryLargeValueAndRepeatedConnections(t *testing.T) {
	for _, mode := range []string{"ring", "pool"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TIGERSTORE_BATCH_MEMORY", mode)
			n := startNode(t)
			s, err := New(n, server.Config{}, "full")
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			value := bytes.Repeat([]byte{0x71}, core.MaxRequestBytes-128)
			for round := range 3 {
				cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				kv := pb.NewKVClient(cc)
				if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("large"), Value: value}); err != nil {
					t.Fatal(err)
				}
				// Exact 1.5 MiB values cannot fit the wrapper and must reject.
				_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("too-large"), Value: make([]byte, core.MaxRequestBytes)})
				if status.Code(err) != status.Code(rpctypes.ErrGRPCRequestTooLarge) {
					t.Fatalf("large error %v", err)
				}
				var wg sync.WaitGroup
				for i := range 16 {
					wg.Go(func() {
						if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte{byte(round + 1), byte(i + 1)}, Value: bytes.Repeat([]byte{byte(i)}, 1024)}); err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
				got, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("large")})
				if err != nil || len(got.Kvs) != 1 || !bytes.Equal(got.Kvs[0].Value, value) {
					t.Fatalf("immutable log round %d: %v", round, err)
				}
				if err := cc.Close(); err != nil {
					t.Fatal(err)
				}
			}
			until := time.Now().Add(time.Second)
			for s.MemoryStats().Input.InUseBytes != 0 || s.MemoryStats().Output.InUseBytes != 0 {
				if time.Now().After(until) {
					t.Fatalf("connections retain chunks %+v", s.MemoryStats())
				}
				time.Sleep(time.Millisecond)
			}
			if s.MemoryStats().Input.Rejected != 0 || s.MemoryStats().Output.Rejected != 0 {
				t.Fatalf("healthy workload rejected %+v", s.MemoryStats())
			}
		})
	}
}
