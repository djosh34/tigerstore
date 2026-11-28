package wl

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestSelfRemovalDrainsLargeRange(t *testing.T) {
	for _, reopen := range []bool{true, false} {
		name := "window-reopens"
		if !reopen {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			c, s, st, log := replyRoomClient(t)
			n := c.node
			c.plain = true // inspect the actual HTTP/2 DATA frames after each flush
			const keys = 3
			value := strings.Repeat("v", 1<<20)
			for i := range keys {
				putValue(t, st, log, fmt.Sprintf("/f/%04d", i), value)
			}
			n.now = time.Now()
			n.selfRemoval.drainBy = n.now.Add(10 * tickEvery)
			c.flow.conn, s.win = 1<<25, 1024
			msg := appendBytesField(appendBytesField(nil, 1, []byte("/f/")), 2, []byte("/f0"))
			n.startBig(c, s, msg)
			t.Cleanup(func() {
				for _, b := range n.bigs {
					n.endBig(b)
				}
			})
			if len(n.bigs) != 1 || s.win != 0 || n.bigs[0].pos >= len(n.bigs[0].buf) || len(c.flow.held) != 0 {
				t.Fatal("range did not retain unsent bytes outside the socket and flow.held")
			}
			// Model successful socket writes while preserving every DATA
			// byte for the complete protobuf reply check below.
			var data []byte
			flush := func() {
				t.Helper()
				c.seal()
				for f := c.out; len(f) > 0; {
					if len(f) < 9 {
						t.Fatal("incomplete HTTP/2 frame header")
					}
					size := int(f[0])<<16 | int(f[1])<<8 | int(f[2])
					if len(f) < 9+size {
						t.Fatal("incomplete HTTP/2 frame body")
					}
					if f[3] == frData && binary.BigEndian.Uint32(f[5:9]) == s.id {
						data = append(data, f[9:9+size]...)
					}
					f = f[9+size:]
				}
				c.out, c.ow, c.wantOut, n.dirty = c.out[:0], 0, false, nil
			}
			flush()
			for range 9 {
				n.pumpBig()
				flush()
				if n.selfRemovalDrained() {
					t.Fatal("closed stream window truncated the large Range before the drain deadline")
				}
				n.now = n.now.Add(tickEvery)
			}
			if !reopen {
				n.now = n.selfRemoval.drainBy
				if len(n.bigs) != 1 || !n.selfRemovalDrained() {
					t.Fatal("large Range extended the ten-tick drain bound")
				}
				return
			}
			c.windowUpdate(s.id, 1<<25)
			for turns := 0; len(n.bigs) > 0; turns++ {
				if turns > 32 {
					t.Fatal("large Range did not finish after the stream window reopened")
				}
				n.pumpBig()
				if n.selfRemovalDrained() {
					t.Fatal("large Range counted as drained before its final socket write")
				}
				flush()
			}
			if !n.selfRemovalDrained() {
				t.Fatal("fully written large Range did not finish the drain")
			}
			if len(data) < 5 || int(binary.BigEndian.Uint32(data[1:5])) != len(data)-5 {
				t.Fatal("large Range reply was truncated")
			}
			var reply pb.RangeResponse
			if err := proto.Unmarshal(data[5:], &reply); err != nil {
				t.Fatal(err)
			}
			if len(reply.Kvs) != keys {
				t.Fatalf("Range returned %d keys, want %d", len(reply.Kvs), keys)
			}
			for i, kv := range reply.Kvs {
				if string(kv.Key) != fmt.Sprintf("/f/%04d", i) || string(kv.Value) != value {
					t.Fatalf("Range record %d was truncated or changed", i)
				}
			}
		})
	}
}

func TestSelfRemovalDrainsActiveProducers(t *testing.T) {
	for _, kind := range []string{"range", "watch", "snapshot"} {
		t.Run(kind, func(t *testing.T) {
			n := &node{now: time.Now()}
			n.selfRemoval.drainBy = n.now.Add(10 * tickEvery)
			switch kind {
			case "range":
				n.bigs = []*bigRange{{}}
			case "watch":
				n.bigWatches = []*bigWatch{{}}
			case "snapshot":
				n.clientSnaps = []*clientSnap{{}}
			}
			if n.selfRemovalDrained() {
				t.Fatal("active response producer was omitted from the drain")
			}
			n.now = n.selfRemoval.drainBy
			if !n.selfRemovalDrained() {
				t.Fatal("active response producer extended the drain deadline")
			}
			n.now = n.now.Add(-tickEvery)
			n.bigs, n.bigWatches, n.clientSnaps = nil, nil, nil
			if !n.selfRemovalDrained() {
				t.Fatal("completed response producer kept the drain open")
			}
		})
	}
}
