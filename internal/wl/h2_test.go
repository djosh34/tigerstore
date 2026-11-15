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
	if kept > keptBufs*keptBufMax {
		t.Fatalf("%d streams of %d KiB messages left %d MiB of buffers, more than the node keeps, %d MiB",
			streams, size>>10, kept>>20, keptBufs*keptBufMax>>20)
	}
}
