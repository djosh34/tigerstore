// Linux-only research prototype; fixed-size framing, bounded state, no TLS.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var mode = flag.String("mode", "net", "net, batch, uring, uringzc, client")
var addr = flag.String("addr", "127.0.0.1:9910", "address")
var reply = flag.Int("reply", 256, "reply bytes")
var connections = flag.Int("connections", 6, "client connections")
var depth = flag.Int("depth", 32, "pipeline depth")
var duration = flag.Duration("duration", 3*time.Second, "measurement duration")
var warmup = flag.Duration("warmup", 500*time.Millisecond, "warmup duration")
var count atomic.Uint64
var measuring atomic.Bool
var startMem runtime.MemStats
var startCPU int64
var started time.Time
var done = make(chan struct{})
var stopOnce sync.Once
var zcNotifications, zcCopied uint64
var netReply []byte

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func cpu() int64 {
	var r unix.Rusage
	check(unix.Getrusage(unix.RUSAGE_SELF, &r))
	return r.Utime.Nano() + r.Stime.Nano()
}
func begin() {
	runtime.ReadMemStats(&startMem)
	startCPU = cpu()
	started = time.Now()
	count.Store(0)
	zcNotifications, zcCopied = 0, 0
	measuring.Store(true)
}
func finish() {
	measuring.Store(false)
	endCPU := cpu()
	elapsed := time.Since(started)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	n := count.Load()
	json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": *mode, "reply": *reply, "requests": n, "seconds": elapsed.Seconds(), "rps": float64(n) / elapsed.Seconds(), "cpu_us_per_req": float64(endCPU-startCPU) / 1000 / float64(n), "mallocs": m.Mallocs - startMem.Mallocs, "allocs_per_req": float64(m.Mallocs-startMem.Mallocs) / float64(n), "bytes_allocated": m.TotalAlloc - startMem.TotalAlloc, "gomaxprocs": runtime.GOMAXPROCS(0), "zc_notifications": zcNotifications, "zc_copied": zcCopied})
}
func special(b byte) bool {
	if b == 254 {
		begin()
		return true
	}
	if b == 253 {
		stopOnce.Do(func() { close(done) })
		return true
	}
	return false
}
func netServer() {
	netReply = make([]byte, 262144)
	for i := range netReply {
		netReply[i] = 7
	}
	l, e := net.Listen("tcp", *addr)
	check(e)
	fmt.Println("READY")
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go netConn(c)
		}
	}()
	<-done
	finish()
}
func netConn(c net.Conn) {
	defer c.Close()
	buf := make([]byte, 65536)
	out := netReply // shared immutable bytes, matching the ring server
	pending := 0
	for {
		var n int
		var e error
		if *mode == "net" {
			_, e = io.ReadFull(c, buf[:100])
			n = 100
		} else {
			n, e = c.Read(buf[pending:])
			n += pending
		}
		if e != nil {
			return
		}
		requests := n / 100
		for i := 0; i < requests; i++ {
			b := buf[i*100]
			if special(b) {
				if b == 253 {
					return
				}
				requests--
				copy(buf[i*100:], buf[(i+1)*100:n])
				n -= 100
				i--
				continue
			}
		}
		if measuring.Load() {
			count.Add(uint64(requests))
		}
		if *mode == "net" {
			if requests > 0 {
				_, e = c.Write(out[:*reply])
			}
		} else {
			left := requests * (*reply)
			for left > 0 {
				k := min(left, len(out))
				_, e = c.Write(out[:k])
				if e != nil {
					return
				}
				left -= k
			}
		}
		if e != nil {
			return
		}
		pending = n % 100
		copy(buf, buf[n-pending:n])
		if requests == 0 {
			_, e = c.Write(out[:*reply])
			if e != nil {
				return
			}
		}
	}
}
func client() {
	dial := func() net.Conn { c, e := net.Dial("tcp", *addr); check(e); return c }
	control := dial()
	conns := make([]net.Conn, *connections)
	for i := range conns {
		conns[i] = dial()
	}
	req := make([]byte, 100*(*depth))
	for i := 0; i < len(req); i += 100 {
		req[i] = 1
	}
	run := func(d time.Duration) uint64 {
		var wg sync.WaitGroup
		var total atomic.Uint64
		end := time.Now().Add(d)
		for _, c := range conns {
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				buf := make([]byte, *reply*(*depth))
				var n uint64
				for time.Now().Before(end) {
					_, e := c.Write(req)
					check(e)
					_, e = io.ReadFull(c, buf)
					check(e)
					if buf[0] != 7 || buf[len(buf)-1] != 7 {
						panic("bad reply bytes")
					}
					n += uint64(*depth)
				}
				total.Add(n)
			}(c)
		}
		wg.Wait()
		return total.Load()
	}
	run(*warmup)
	mark := make([]byte, 100)
	mark[0] = 254
	_, e := control.Write(mark)
	check(e)
	_, e = io.ReadFull(control, make([]byte, *reply))
	check(e)
	t := time.Now()
	n := run(*duration)
	elapsed := time.Since(t)
	mark[0] = 253
	_, e = control.Write(mark)
	check(e)
	fmt.Printf("CLIENT requests=%d seconds=%.6f rps=%.0f\n", n, elapsed.Seconds(), float64(n)/elapsed.Seconds())
	for _, c := range conns {
		c.Close()
	}
	control.Close()
}
func main() {
	flag.Parse()
	if *mode == "client" {
		client()
		return
	}
	if *reply < 1 || *reply > 262144 {
		panic("reply size")
	}
	if *mode == "net" || *mode == "batch" {
		netServer()
	} else {
		uringServer()
	}
}
