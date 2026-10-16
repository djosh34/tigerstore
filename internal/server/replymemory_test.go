package server

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/node"
)

func TestAnswerMemoryStreamLifetime(t *testing.T) {
	for i, phase := range []string{"sent", "reset during write", "close during write", "drop queued watch", "refused send"} {
		t.Run(phase, func(t *testing.T) {
			c := testConn(t)
			a, b := net.Pipe()
			c.raw = a
			t.Cleanup(func() {
				c.close()
				if err := b.Close(); err != nil {
					t.Error(err)
				}
			})
			st := testCall(c, uint32(2*i+1))
			st.rpc = methods["/etcdserverpb.Watch/Watch"]
			var freed atomic.Int64
			const bytes = 1 << 20
			hold := node.NewReplyHold(bytes, func(n int64) { freed.Add(n) })
			if phase == "refused send" {
				c.resetStream(st.id, 0)
			}
			r := &pb.WatchResponse{Events: []*mvccpb.Event{{Kv: &mvccpb.KeyValue{Key: []byte("k"), Value: make([]byte, 128<<10)}}}}
			err := st.SendHeld(r, hold)
			if (err != nil) != (phase == "refused send") {
				t.Fatalf("send: %v", err)
			}
			hold.Release()
			if phase == "refused send" {
				if freed.Load() != bytes {
					t.Fatal("refused send retained memory")
				}
				return
			}
			if freed.Load() != 0 {
				t.Fatal("queue did not retain the reservation")
			}
			if phase == "drop queued watch" {
				st.finish(status.Error(codes.Unavailable, "closed"))
			} else {
				c.mu.Lock()
				if phase != "sent" {
					st.sendWin = 1
				}
				frames := c.take(nil)
				c.mu.Unlock()
				if len(frames) == 0 {
					t.Fatal("no socket frames")
				}
				switch phase {
				case "reset during write":
					c.resetStream(st.id, 0)
				case "close during write":
					c.close()
				}
				if freed.Load() != 0 {
					t.Fatal("socket frames lost their reservation")
				}
				releaseFrames(frames)
			}
			if freed.Load() != bytes {
				t.Fatalf("released %d bytes, want %d", freed.Load(), bytes)
			}
		})
	}
}

func TestRangeReplyReservationLifetime(t *testing.T) {
	const reserved = 1 << 20
	for _, phase := range []string{"sent", "reset during write", "close during write", "deadline before completion", "reset before completion", "close before completion", "close with completion queued", "close with answer queued"} {
		t.Run(phase, func(t *testing.T) {
			c := testConn(t)
			a, b := net.Pipe()
			c.raw = a
			t.Cleanup(func() {
				c.close()
				if err := b.Close(); err != nil {
					t.Error(err)
				}
			})
			st := testCall(c, 1)
			var freed atomic.Int64
			hold := node.NewReplyHold(reserved, func(n int64) { freed.Add(n) })
			cp := node.Completion{
				Tag:   st,
				Hold:  hold,
				Range: mvcc.RangeResult{KVs: []mvcc.KV{{Key: []byte("k"), Value: make([]byte, 128<<10)}}, Bytes: 1 + 128<<10},
			}
			switch phase {
			case "deadline before completion":
				st.expire()
			case "reset before completion":
				c.resetStream(st.id, 0)
			case "close before completion":
				c.close()
			}
			c.Complete([]node.Completion{cp})
			if phase == "close with completion queued" {
				c.close()
			}
			c.completeWaiting()
			switch phase {
			case "close with answer queued":
				if freed.Load() != 0 {
					t.Fatal("queued answer released its reservation")
				}
				c.close()
			case "sent", "reset during write", "close during write":
				if freed.Load() != 0 {
					t.Fatal("encoding released the reservation")
				}
				c.mu.Lock()
				if phase != "sent" {
					st.sendWin = 1 // a frame still retains the whole buffer
				}
				frames := c.take(nil)
				c.mu.Unlock()
				if len(frames) == 0 {
					t.Fatal("writer took no frames")
				}
				switch phase {
				case "reset during write":
					c.resetStream(st.id, 0)
				case "close during write":
					c.close()
				}
				if freed.Load() != 0 {
					t.Fatal("reservation released while socket frames still reference the answer")
				}
				// The writer does this after Flush, or on a write error.
				releaseFrames(frames)
			}
			if got := freed.Load(); got != reserved {
				t.Fatalf("released %d bytes, want %d exactly once", got, reserved)
			}
		})
	}
}

func TestRangeReplyCompletionRacesClose(t *testing.T) {
	for range 100 {
		c := testConn(t)
		a, b := net.Pipe()
		c.raw = a
		st := testCall(c, 1)
		var freed atomic.Int64
		var wg sync.WaitGroup
		wg.Go(func() {
			c.Complete([]node.Completion{{Tag: st, Hold: node.NewReplyHold(1, func(n int64) { freed.Add(n) })}})
		})
		wg.Go(c.close)
		wg.Wait()
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if freed.Load() != 1 || c.waitingReplies.Load() != 0 || c.completions.Load() != closedCompletions {
			t.Fatalf("late completion retained memory: freed=%d waiting=%d", freed.Load(), c.waitingReplies.Load())
		}
	}
}
