// tcbench: raw TCP microbench run through `tailcat forward` (#630). Throwaway.
//
//	tcbench serve :port                 sink + echo server (one protocol, below)
//	tcbench tput host:port bytes streams   send bytes split over streams, print Mbit/s
//	tcbench rtt host:port interval dur  64-byte echo every interval on one held
//	                                    connection; reconnects on error
//
// Protocol: first byte 'S' = sink: 8-byte length, then data; server answers
// 8 bytes (count read) once it has it all. 'E' = echo: 64-byte frames echoed.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func main() {
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2])
	case "tput":
		n, _ := strconv.ParseInt(os.Args[3], 10, 64)
		s, _ := strconv.Atoi(os.Args[4])
		tput(os.Args[2], n, s)
	case "rtt":
		iv, _ := time.ParseDuration(os.Args[3])
		d, _ := time.ParseDuration(os.Args[4])
		rtt(os.Args[2], iv, d)
	}
}

func serve(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			k, err := r.ReadByte()
			if err != nil {
				return
			}
			switch k {
			case 'S':
				var h [8]byte
				if _, err := io.ReadFull(r, h[:]); err != nil {
					return
				}
				want := int64(binary.BigEndian.Uint64(h[:]))
				got, _ := io.CopyN(io.Discard, r, want)
				binary.BigEndian.PutUint64(h[:], uint64(got))
				c.Write(h[:])
			case 'E':
				buf := make([]byte, 64)
				for {
					if _, err := io.ReadFull(r, buf); err != nil {
						return
					}
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}
		}(c)
	}
}

func tput(addr string, total int64, streams int) {
	per := total / int64(streams)
	var wg sync.WaitGroup
	errs := make([]error, streams)
	durs := make([]time.Duration, streams)
	t0 := time.Now()
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s0 := time.Now()
			c, err := net.DialTimeout("tcp", addr, 10*time.Second)
			if err != nil {
				errs[i] = err
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(10 * time.Minute))
			var h [9]byte
			h[0] = 'S'
			binary.BigEndian.PutUint64(h[1:], uint64(per))
			c.Write(h[:])
			buf := make([]byte, 64<<10)
			for left := per; left > 0; {
				n := int64(len(buf))
				if left < n {
					n = left
				}
				if _, err := c.Write(buf[:n]); err != nil {
					errs[i] = err
					return
				}
				left -= n
			}
			if _, err := io.ReadFull(c, h[:8]); err != nil {
				errs[i] = fmt.Errorf("ack: %w", err)
				return
			}
			if got := int64(binary.BigEndian.Uint64(h[:8])); got != per {
				errs[i] = fmt.Errorf("server got %d of %d", got, per)
			}
			durs[i] = time.Since(s0)
		}(i)
	}
	wg.Wait()
	el := time.Since(t0)
	out := map[string]any{"streams": streams, "bytes": per * int64(streams), "seconds": el.Seconds(),
		"mbit_s": float64(per*int64(streams)) * 8 / el.Seconds() / 1e6}
	var per_ []float64
	var es []string
	for i := range durs {
		if errs[i] != nil {
			es = append(es, errs[i].Error())
			continue
		}
		per_ = append(per_, float64(per)*8/durs[i].Seconds()/1e6)
	}
	out["per_stream_mbit_s"] = per_
	out["errors"] = es
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}

func rtt(addr string, iv, dur time.Duration) {
	end := time.Now().Add(dur)
	var all []time.Duration
	errs, reconnects := 0, 0
	var c net.Conn
	frame := make([]byte, 64)
	back := make([]byte, 64)
	win := time.Now()
	var wl []time.Duration
	werr := 0
	flush := func() {
		if len(wl) == 0 && werr == 0 {
			return
		}
		s := append([]time.Duration(nil), wl...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		p := func(q float64) float64 {
			if len(s) == 0 {
				return -1
			}
			return float64(s[min(len(s)-1, int(q*float64(len(s))))].Microseconds()) / 1000
		}
		fmt.Printf("window %s n=%d err=%d p50=%.1fms p99=%.1fms max=%.1fms\n", win.UTC().Format("15:04:05"), len(s), werr, p(.5), p(.99), p(1))
		wl, werr, win = wl[:0], 0, time.Now()
	}
	for time.Now().Before(end) {
		t := time.Now()
		if c == nil {
			var err error
			c, err = net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				errs++
				werr++
				fmt.Printf("%s dial error: %v\n", t.UTC().Format(time.RFC3339), err)
				time.Sleep(time.Second)
				continue
			}
			c.Write([]byte{'E'})
			if len(all) > 0 || errs > 0 {
				reconnects++
			}
		}
		binary.BigEndian.PutUint64(frame, uint64(t.UnixNano()))
		c.SetDeadline(t.Add(5 * time.Second))
		_, err := c.Write(frame)
		if err == nil {
			_, err = io.ReadFull(c, back)
		}
		if err != nil {
			errs++
			werr++
			fmt.Printf("%s echo error: %v\n", t.UTC().Format(time.RFC3339), err)
			c.Close()
			c = nil
			continue
		}
		d := time.Since(t)
		all = append(all, d)
		wl = append(wl, d)
		if time.Since(win) >= 30*time.Second {
			flush()
		}
		time.Sleep(max(0, iv-time.Since(t)))
	}
	flush()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	q := func(f float64) float64 {
		if len(all) == 0 {
			return -1
		}
		return float64(all[min(len(all)-1, int(f*float64(len(all))))].Microseconds()) / 1000
	}
	spikes := 0
	for _, d := range all {
		if float64(d.Microseconds())/1000 > 3*q(.5) {
			spikes++
		}
	}
	b, _ := json.Marshal(map[string]any{"n": len(all), "errors": errs, "reconnects": reconnects,
		"p50_ms": q(.5), "p90_ms": q(.9), "p99_ms": q(.99), "max_ms": q(1), "min_ms": q(0), "spikes_over_3x_p50": spikes})
	fmt.Println(string(b))
}
