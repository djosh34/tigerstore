package server

import (
	"context"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/node"
)

// testConn is a connection with no socket, for the accounting of calls.
func testConn() *conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &conn{ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), streams: map[uint32]*stream{}, maxFrame: 16384, sendWin: 1 << 30}
}

func testCall(c *conn, id uint32) *stream {
	st := &stream{c: c, id: id, rpc: methods["/etcdserverpb.KV/Range"], reqSize: 100, sendWin: 1 << 30, inEOF: true}
	c.streams[id] = st
	return st
}

// drain runs the writer's side: it takes all queued output.
func drain(c *conn) {
	c.mu.Lock()
	c.take(nil)
	c.mu.Unlock()
}

// TestCallCharges checks that every order of reply, deadline and reset
// leaves a connection holding nothing once its calls are over.
func TestCallCharges(t *testing.T) {
	reply := func(c *conn, st *stream) {
		c.Complete([]node.Completion{{Tag: st, Resp: &pb.RangeResponse{Header: &pb.ResponseHeader{}}}})
		c.complete(c.takeCompletions())
	}
	charge := func(c *conn, st *stream) {
		c.mu.Lock()
		c.chargeLocked(&st.reqHeld, st.reqSize+callCost)
		c.mu.Unlock()
	}
	for name, run := range map[string]func(c *conn, st *stream){
		"reply, then sent": func(c *conn, st *stream) {
			charge(c, st)
			reply(c, st)
			if c.held == 0 {
				t.Error("a queued reply holds nothing")
			}
			drain(c)
		},
		"deadline, sent, then the node's reply": func(c *conn, st *stream) {
			charge(c, st)
			st.expire()
			drain(c)
			if c.held != st.reqSize+callCost {
				t.Errorf("the node still has the request, but the call holds %d", c.held)
			}
			reply(c, st)
		},
		"reply, then deadline": func(c *conn, st *stream) {
			charge(c, st)
			reply(c, st)
			st.expire()
			drain(c)
		},
		"reset, then the node's reply": func(c *conn, st *stream) {
			charge(c, st)
			c.resetStream(st.id, 0)
			reply(c, st)
		},
		"deadline before the batch goes out": func(c *conn, st *stream) {
			st.expire()
			c.batch = opBatch{ops: []proto.Message{&pb.RangeRequest{}}, tags: []any{st}}
			if err := c.flush(); err != nil {
				t.Fatal(err)
			}
			drain(c)
		},
	} {
		c := testConn()
		st := testCall(c, 1)
		run(c, st)
		if c.held != 0 || st.reqHeld != 0 || st.replyHeld != 0 {
			t.Errorf("%s: the connection holds %d bytes, the call %d and %d", name, c.held, st.reqHeld, st.replyHeld)
		}
	}
}
