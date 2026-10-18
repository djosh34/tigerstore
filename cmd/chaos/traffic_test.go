package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
)

// TestAwaitTraffic checks the wait before a link fault (#224): with no
// client on the link yet, it waits until one connects and bytes go both
// ways, so a fault held from then on has traffic to act on.
func TestAwaitTraffic(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, _ = io.Copy(c, c)
				_ = c.Close()
			}()
		}
	}()
	p, err := proxy.Listen("127.0.0.1:0", ln.Addr().String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	l := &link{Name: "client-node1", From: -1, Proxy: p}
	const late = 300 * time.Millisecond
	go func() {
		time.Sleep(late)
		c, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		b := make([]byte, 1)
		for range 50 {
			if _, err := c.Write(b); err != nil {
				return
			}
			if _, err := io.ReadFull(c, b); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	start := time.Now()
	if err := awaitTraffic(context.Background(), l, proxy.Both); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited < late {
		t.Fatalf("returned after %v, before the client connected", waited)
	}
	if st := p.Stats(); st.Open == 0 || st.ForwardBytes == 0 || st.BackwardBytes == 0 {
		t.Fatalf("stats after the wait: %+v", st)
	}
}
