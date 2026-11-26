//go:build linux && !(sim || simio)

package wl

import (
	"bytes"
	"crypto/tls"
	"os"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A slow peer can queue many messages. Pre-sizing every addition copies
// the backlog on each send rather than keeping amortized doubling.
func TestQueuedPeerBuffersGrowGeometrically(t *testing.T) {
	for _, kind := range []string{"batch", "proposal", "append"} {
		t.Run(kind, func(t *testing.T) {
			wr, err := newDir(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32))
			if err != nil {
				t.Fatal(err)
			}
			p := &peerOut{sock: *newSock(&handoff{fd: -1, wr: wr}, &counters{})}
			n := node{id: 1, lead: 3, peersOut: map[uint64]*peerOut{2: p}, sizes: sizes{replication: 1 << 30}}
			data := bytes.Repeat([]byte("x"), 128<<10)
			typ := pb.MsgProp
			if kind == "append" {
				typ = pb.MsgApp
			}
			m := &pb.Message{Type: &typ, To: new(uint64(2)), Entries: []*pb.Entry{{Data: data}}}
			growths, copied := 0, 0
			for range 32 {
				before := p.out[:cap(p.out)]
				queued := len(p.out)
				if kind == "batch" {
					n.sendBatch(data)
				} else {
					n.sendPeer(m)
				}
				if &before[0] != &p.out[:cap(p.out)][0] {
					growths++
					copied += queued
				}
			}
			if len(p.out) < 32*len(data) {
				t.Fatal("the peer did not queue every message")
			}
			if growths > 3 || copied > 2*cap(p.out) {
				t.Fatalf("queued sends grew %d times and copied %d bytes for a %d-byte buffer", growths, copied, cap(p.out))
			}
		})
	}
}

// A near-limit put travels as a follower's batch and proposal, or as a
// leader's append. Growing its TLS buffer a record at a time doubles
// outBuf to just over twice its size; flush then discards it. The next
// put allocates and copies the same megabytes again on both peer links.
func TestLargePeerMessagesKeepTheirSendBuffer(t *testing.T) {
	for _, kind := range []string{"batch", "proposal", "append"} {
		t.Run(kind, func(t *testing.T) {
			f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			wr, err := newDir(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32))
			if err != nil {
				t.Fatal(err)
			}
			s := newSock(&handoff{fd: int(f.Fd()), wr: wr}, &counters{})
			p := &peerOut{sock: *s}
			n := node{id: 1, lead: 3, peersOut: map[uint64]*peerOut{2: p}}
			data := bytes.Repeat([]byte("x"), (1536<<10)-256)
			typ := pb.MsgProp
			if kind == "append" {
				typ = pb.MsgApp
			}
			m := &pb.Message{Type: &typ, To: new(uint64(2)), Entries: []*pb.Entry{{Data: data}}}
			send := func() {
				if kind == "batch" {
					n.sendBatch(data)
				} else {
					n.sendPeer(m)
				}
			}
			send()
			// It must be pre-grown to fit, not doubled just past the
			// flush's retention limit. Take the address before the flush
			// so keeping only the capacity cannot conceal a replacement.
			if cap(p.out) < len(data) || cap(p.out) > 2*outBuf {
				t.Fatalf("send buffer capacity %d, want %d..%d", cap(p.out), len(data), 2*outBuf)
			}
			buffer := p.out[:cap(p.out)]
			flush := func() {
				if ok, err := p.flush(); err != nil || !ok {
					t.Fatalf("flush: %t, %v", ok, err)
				}
				if cap(p.out) != len(buffer) || &p.out[:cap(p.out)][0] != &buffer[0] {
					t.Fatal("flush replaced the near-limit put's send buffer")
				}
			}
			flush()
			if allocs := testing.AllocsPerRun(1, func() { send(); flush() }); allocs != 0 {
				t.Fatalf("next near-limit put allocated %g times", allocs)
			}
		})
	}
}
