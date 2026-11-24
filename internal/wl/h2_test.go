package wl

import (
	"encoding/binary"
	"testing"
)

// A message that spans DATA frames waits in its stream's buffer. Each of
// a client's 8,192 stream slots kept its buffer at the largest message it
// ever had, and the k8s bench's nodes held hundreds of megabytes of them
// (#602): ended streams give their buffers back, and the node keeps only
// a few.
func TestEndedStreamsGiveTheirBuffersBack(t *testing.T) {
	c, _ := flowClient(t)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	c.node.st = st
	// The bench's sizes: the node keeps 64 buffers (sizes.kept).
	c.node.sizes = sizeFor(0)
	const size = 100 << 10
	msg := make([]byte, 5+size)
	binary.BigEndian.PutUint32(msg[1:], size)
	const streams = 1000
	for i := range streams {
		// Each stream takes its own slot, as the ids of a busy client do.
		s := c.newStream(uint32(2*i+3), kAlarm)
		if err := c.data(c.node, s, msg[:60<<10]); err != nil {
			t.Fatal(err)
		}
		if err := c.data(c.node, s, msg[60<<10:]); err != nil {
			t.Fatal(err)
		}
		if s.open {
			t.Fatalf("stream %d is open after its answer", s.id)
		}
	}
	kept := 0
	for i := range c.slots {
		kept += cap(c.slots[i].buf)
	}
	for _, b := range c.node.bufs {
		kept += cap(b)
	}
	if kept > c.node.sizes.kept*keptBufMax {
		t.Fatalf("%d streams of %d KiB messages left %d MiB of buffers, more than the node keeps, %d MiB",
			streams, size>>10, kept>>20, c.node.sizes.kept*keptBufMax>>20)
	}
}

// The node keeps as many buffers that ended streams gave back as its
// budget allows (sizes.kept): 4 at 128 MiB, 64 at the bench's 3 GiB, and
// none past keptBufMax.
func TestTheNodeKeepsBuffersByItsBudget(t *testing.T) {
	for _, c := range []struct {
		budget int64
		want   int
	}{{128 << 20, 4}, {3 << 30, 64}} {
		n := &node{sizes: sizeFor(c.budget)}
		for range 100 {
			n.putBuf(make([]byte, 0, 64<<10))
		}
		n.putBuf(make([]byte, 0, keptBufMax+1))
		if len(n.bufs) != c.want {
			t.Errorf("at %d MiB the node keeps %d buffers, want %d", c.budget>>20, len(n.bufs), c.want)
		}
		for _, b := range n.bufs {
			if cap(b) > keptBufMax {
				t.Errorf("at %d MiB the node keeps a buffer of %d bytes, past %d", c.budget>>20, cap(b), keptBufMax)
			}
		}
	}
}

// A client's trailers that change its HPACK table empty the cache of
// header blocks, as a request's do: the bytes of a cached block that
// names a table entry mean another header once the table moved, and the
// node served them as the call the cache said (#689's area-4 review).
func TestTrailersThatChangeTheTableEmptyTheHeaderCache(t *testing.T) {
	c, _ := flowClient(t)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	c.node.st = st
	c.node.maxStreams = 100
	path := "/etcdserverpb.KV/Range"
	ct := append([]byte{0x0f, 0x10, 16}, "application/grpc"...) // content-type, not indexed
	block := func(sid uint32, end bool, b ...byte) {
		t.Helper()
		c.blockSid, c.blockEnd = sid, end
		if err := c.headers(c.node, b, false); err != nil {
			t.Fatalf("stream %d: %v", sid, err)
		}
	}
	// POST, https, and :path taken into the table, at index 62.
	block(3, false, append(append([]byte{0x83, 0x87, 0x44, byte(len(path))}, path...), ct...)...)
	// The same request naming the table's entry: cached.
	block(5, false, append([]byte{0x83, 0x87, 0x80 | 62}, ct...)...)
	if len(c.hdrCache) != 1 {
		t.Fatalf("the cache holds %d blocks, want the one that names the table", len(c.hdrCache))
	}
	// Stream 3's trailers take x: y into the table, at 62.
	block(3, true, 0x40, 1, 'x', 1, 'y')
	if len(c.hdrCache) != 0 {
		t.Fatalf("after trailers that changed the table, the cache holds %d blocks", len(c.hdrCache))
	}
}
