package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestSmallTLSRecords(t *testing.T) {
	for _, mode := range []string{"none", "short", "long", "switching", "fast", "slow"} {
		t.Run(mode, func(t *testing.T) {
			ep, e := newEndpoint()
			if e != nil {
				t.Fatal(e)
			}
			defer ep.close()
			p, e := newProxy(ep.listener.Addr().String(), mode, 49, false)
			if e != nil {
				t.Fatal(e)
			}
			defer p.close()
			c, _, e := dial(ep, p.listener.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			p.policy.armed.Store(true)
			_ = c.SetDeadline(time.Now().Add(2 * time.Second))
			// Thirty-two 1-byte plaintext records are all smaller than a 4KiB block.
			// A relay that insists on ReadFull(4KiB) cannot complete the first echo.
			for i := 0; i < 32; i++ {
				want := []byte{byte(i)}
				got := make([]byte, 1)
				if e = exchange(c, want, got); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestStallBackpressure(t *testing.T) {
	ep, e := newEndpoint()
	if e != nil {
		t.Fatal(e)
	}
	defer ep.close()
	p, e := newProxy(ep.listener.Addr().String(), "stall", 49, false)
	if e != nil {
		t.Fatal(e)
	}
	defer p.close()
	c, _, e := dial(ep, p.listener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	p.policy.armed.Store(true)
	_ = c.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	payload := bytes.Repeat([]byte{0x59}, 32*1024)
	written := 0
	for written < 64*1024*1024 {
		n, err := c.Write(payload)
		written += n
		if err != nil {
			e = err
			break
		}
	}
	var ne net.Error
	if !errors.As(e, &ne) || !ne.Timeout() {
		t.Fatalf("expected blocked write timeout, bytes=%d error=%v", written, e)
	}
	t.Logf("stall blocked client TLS writes after %d plaintext bytes; error=%v", written, e)
}

// Supplemental raw-stream test. Fault exercise itself uses real TLS endpoints.
// A seeded mutation must select the same input offset despite fragmentation.
func TestMutationOffsetsIgnoreReadFragmentation(t *testing.T) {
	for _, mode := range []string{"drop", "duplicate", "corrupt"} {
		for _, fragment := range []int{1, 7, 113, 4096, 8192} {
			t.Run(mode+"/"+strconv.Itoa(fragment)+"bytes", func(t *testing.T) {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					t.Fatal(e)
				}
				defer l.Close()
				received := make(chan []byte, 1)
				serverErr := make(chan error, 1)
				go func() {
					c, e := l.Accept()
					if e != nil {
						serverErr <- e
						return
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(3 * time.Second))
					b, e := io.ReadAll(c)
					if e != nil {
						serverErr <- e
						return
					}
					received <- b
				}()
				p, e := newProxy(l.Addr().String(), mode, 49, true)
				if e != nil {
					t.Fatal(e)
				}
				defer p.close()
				v, e := net.Dial("tcp", p.listener.Addr().String())
				if e != nil {
					t.Fatal(e)
				}
				c := v.(*net.TCPConn)
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
				src := bytes.Repeat([]byte{0x59}, 8192)
				for off := 0; off < len(src); {
					end := off + fragment
					if end > len(src) {
						end = len(src)
					}
					if e = writeAll(c, src[off:end]); e != nil {
						t.Fatal(e)
					}
					off = end
					if fragment < 128 && off < 256 {
						time.Sleep(5 * time.Microsecond)
					} // encourage separate socket reads
				}
				if e = c.CloseWrite(); e != nil {
					t.Fatal(e)
				}
				var got []byte
				select {
				case got = <-received:
				case e = <-serverErr:
					t.Fatal(e)
				case <-time.After(4 * time.Second):
					t.Fatal("server did not reach EOF")
				}
				target := int(32 + choice(49, 1, 0, 0)%96)
				want := append([]byte(nil), src...)
				switch mode {
				case "drop":
					want = append(want[:target], want[target+1:]...)
				case "duplicate":
					want = append(want[:target+1], append([]byte{src[target]}, src[target+1:]...)...)
				case "corrupt":
					want[target] ^= 1
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s changed the wrong bytes for fragment=%d", mode, fragment)
				}
				p.mu.Lock()
				events := append([]event(nil), p.events...)
				p.mu.Unlock()
				if len(events) != 1 || events[0].Offset != uint64(target) {
					t.Fatalf("events=%v", events)
				}
			})
		}
	}
}
