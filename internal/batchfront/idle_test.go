package batchfront

import (
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/djosh34/tigerstore/internal/node"
)

func TestIdleWriterRefreshesDeadlineAfterWake(t *testing.T) {
	c, peer := testConn(t)
	c.writeTimeout = 50 * time.Millisecond
	c.start(1)
	done := make(chan error, 1)
	go func() { err := c.writeLoop(); c.close(); done <- err }()
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
	// Longer than the writer's timeout, but with no pending network write.
	time.Sleep(2 * c.writeTimeout)
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.PutResponse{}}})
	for {
		frame, err := f.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if h, ok := frame.(*http2.MetaHeadersFrame); ok && h.StreamEnded() {
			break
		}
	}
	c.close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
