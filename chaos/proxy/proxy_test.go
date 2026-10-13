package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

// echo starts a TCP server that writes back what it reads.
func echo(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// sink starts a TCP server that sends a byte every 10 ms and counts the
// bytes it reads into got.
func sink(t *testing.T, got chan<- int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			for {
				if _, err := c.Write([]byte{'p'}); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
		buf := make([]byte, 1024)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				got <- n
			}
			if err != nil {
				return
			}
		}
	}()
	return ln.Addr().String()
}

func start(t *testing.T, upstream string, seed uint64) (*Proxy, net.Conn) {
	t.Helper()
	p, err := Listen("127.0.0.1:0", upstream, seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return p, c
}

func set(t *testing.T, p *Proxy, f Fault) {
	t.Helper()
	s, err := p.Set(f)
	if err != nil {
		t.Fatal(err)
	}
	if s.Fault != f {
		t.Fatalf("installed %+v, want %+v", s.Fault, f)
	}
}

// roundTrip sends msg and reads the same number of bytes back.
func roundTrip(t *testing.T, c net.Conn, msg []byte) ([]byte, time.Duration) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := c.Write(msg); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	return got, time.Since(start)
}

func TestPassThrough(t *testing.T) {
	p, c := start(t, echo(t), 1)
	msg := bytes.Repeat([]byte("tiger"), 50000)
	if got, _ := roundTrip(t, c, msg); !bytes.Equal(got, msg) {
		t.Fatal("bytes changed with no fault")
	}
	if s := p.Stats(); s.Hits != 0 || s.ForwardBytes != uint64(len(msg)) || s.BackwardBytes != uint64(len(msg)) {
		t.Fatalf("stats %+v", s)
	}
}

func TestDelay(t *testing.T) {
	p, c := start(t, echo(t), 1)
	set(t, p, Fault{Kind: Delay, Dir: Forward, Delay: 50 * time.Millisecond})
	if _, d := roundTrip(t, c, []byte("x")); d < 50*time.Millisecond {
		t.Fatalf("round trip %v with a 50ms delay", d)
	}
	set(t, p, Fault{Kind: Delay, Dir: Both, Delay: 30 * time.Millisecond})
	if _, d := roundTrip(t, c, []byte("x")); d < 60*time.Millisecond {
		t.Fatalf("round trip %v with 30ms each way", d)
	}
	if p.Stats().Hits != 2 {
		t.Fatalf("hits %d, want 2", p.Stats().Hits)
	}
	set(t, p, Fault{Kind: None})
	if _, d := roundTrip(t, c, []byte("x")); d > 30*time.Millisecond {
		t.Fatalf("round trip %v after heal", d)
	}
}

func TestSwitchingDelay(t *testing.T) {
	p, c := start(t, echo(t), 1)
	set(t, p, Fault{Kind: Switching, Dir: Forward, Delay: time.Millisecond, Delay2: 40 * time.Millisecond, Every: 60 * time.Millisecond})
	var short, long int
	for range 20 {
		_, d := roundTrip(t, c, []byte("x"))
		if d < 20*time.Millisecond {
			short++
		} else {
			long++
		}
		time.Sleep(7 * time.Millisecond)
	}
	if short == 0 || long == 0 {
		t.Fatalf("%d short and %d long round trips; want both", short, long)
	}
}

func TestStallHoldsBytes(t *testing.T) {
	p, c := start(t, echo(t), 1)
	set(t, p, Fault{Kind: Stall, Dir: Forward})
	if _, err := c.Write([]byte("held")); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	var ne net.Error
	if _, err := c.Read(make([]byte, 4)); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("read during stall: %v, want a timeout", err)
	}
	if p.Stats().Hits == 0 {
		t.Fatal("stall has no hit")
	}
	set(t, p, Fault{Kind: None})
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "held" {
		t.Fatalf("after stall got %q, %v", got, err)
	}
}

func TestStallBackpressure(t *testing.T) {
	p, c := start(t, echo(t), 1)
	set(t, p, Fault{Kind: Stall, Dir: Forward})
	_ = c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	payload := make([]byte, 32<<10)
	written := 0
	var err error
	for written < 256<<20 && err == nil {
		var n int
		n, err = c.Write(payload)
		written += n
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("wrote %d bytes into a stalled link: %v", written, err)
	}
}

func TestOneDirectionCut(t *testing.T) {
	got := make(chan int, 1000)
	p, c := start(t, sink(t, got), 1)
	set(t, p, Fault{Kind: Cut, Dir: Forward})
	if _, err := c.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	// Replies still arrive while requests are cut.
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
		t.Fatalf("backward direction during a forward cut: %v", err)
	}
	select {
	case n := <-got:
		t.Fatalf("upstream read %d bytes through a cut", n)
	case <-time.After(100 * time.Millisecond):
	}
	if p.Stats().Hits == 0 {
		t.Fatal("cut has no hit")
	}
	// The healed proxy resets the damaged connection.
	set(t, p, Fault{Kind: None})
	if !closed(c) {
		t.Fatal("a damaged connection stayed open after heal")
	}
}

// closed reports whether reads on c end with an error within 2 s.
func closed(c net.Conn) bool {
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	for {
		if _, err := c.Read(buf); err != nil {
			var ne net.Error
			return !errors.As(err, &ne) || !ne.Timeout()
		}
	}
}

func TestByteFaults(t *testing.T) {
	for _, k := range []Kind{Drop, Corrupt, Duplicate} {
		t.Run(string(k), func(t *testing.T) {
			p, c := start(t, echo(t), 7)
			set(t, p, Fault{Kind: k, Dir: Forward})
			msg := bytes.Repeat([]byte{0x55}, 1000)
			_ = c.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Write(msg); err != nil {
				t.Fatal(err)
			}
			want := map[Kind]int{Drop: 999, Corrupt: 1000, Duplicate: 1001}[k]
			got := make([]byte, want)
			if _, err := io.ReadFull(c, got); err != nil {
				t.Fatal(err)
			}
			if k == Corrupt && bytes.Equal(got, msg) {
				t.Fatal("no bit flipped")
			}
			if p.Stats().Hits != 1 {
				t.Fatalf("hits %d, want 1", p.Stats().Hits)
			}
		})
	}
}

func TestSeedFixesByteFaults(t *testing.T) {
	run := func(seed uint64) []byte {
		p, c := start(t, echo(t), seed)
		set(t, p, Fault{Kind: Corrupt, Dir: Forward})
		got, _ := roundTrip(t, c, make([]byte, 4096))
		return got
	}
	a, b, other := run(3), run(3), run(4)
	if !bytes.Equal(a, b) {
		t.Fatal("one seed corrupted different bits")
	}
	if bytes.Equal(a, other) {
		t.Fatal("two seeds corrupted the same bit")
	}
}

func TestReset(t *testing.T) {
	p, c := start(t, echo(t), 1)
	roundTrip(t, c, []byte("x"))
	set(t, p, Fault{Kind: Reset})
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("read after reset: %v, want ECONNRESET", err)
	}
	// New connections are reset while the fault is installed.
	c2, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if !closed(c2) {
		t.Fatal("a new connection stayed open during reset")
	}
	if s := p.Stats(); s.Hits != 2 {
		t.Fatalf("hits %d, want 2", s.Hits)
	}
}

func TestRateMakesOneLinkSlower(t *testing.T) {
	addr := echo(t)
	fast, cf := start(t, addr, 1)
	slow, cs := start(t, addr, 1)
	set(t, slow, Fault{Kind: Rate, Dir: Forward, Rate: 256 << 10})
	msg := make([]byte, 128<<10)
	_, df := roundTrip(t, cf, msg)
	_, ds := roundTrip(t, cs, msg)
	if ds < 400*time.Millisecond || ds < 10*df {
		t.Fatalf("128 KiB took %v on the slow link and %v on the fast one", ds, df)
	}
	if slow.Stats().Hits == 0 || fast.Stats().Hits != 0 {
		t.Fatalf("hits slow %d fast %d", slow.Stats().Hits, fast.Stats().Hits)
	}
}

func TestValidate(t *testing.T) {
	for _, f := range []Fault{
		{Kind: "melt", Dir: Both},
		{Kind: Delay, Dir: Both},
		{Kind: Cut},
		{Kind: Rate, Dir: Both},
		{Kind: Switching, Dir: Both, Delay: time.Millisecond, Delay2: time.Millisecond, Every: time.Second},
	} {
		if f.Validate() == nil {
			t.Errorf("%+v is valid", f)
		}
	}
}
