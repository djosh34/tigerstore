// Separate processes are taskset-pinned; connections use TCP_NODELAY.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	server := flag.Bool("server", false, "")
	secure := flag.Bool("tls", false, "")
	addr := flag.String("addr", "127.0.0.1:20011", "")
	mode := flag.String("mode", "ping", "")
	duration := flag.Duration("duration", 3*time.Second, "")
	streams := flag.Int("streams", 1, "")
	certs := flag.String("certs", "certs", "")
	alpn := flag.String("alpn", "", "optional TLS ALPN for a handshake probe")
	flag.Parse()
	var cfg *tls.Config
	if *secure {
		name := "client"
		if *server {
			name = "node1"
		}
		cert, err := tls.LoadX509KeyPair(*certs+"/"+name+".pem", *certs+"/"+name+"-key.pem")
		must(err)
		pem, err := os.ReadFile(*certs + "/ca.pem")
		must(err)
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			panic("bad CA")
		}
		cfg = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, ServerName: "localhost"}
		if *alpn != "" {
			cfg.NextProtos = []string{*alpn}
		}
	}
	if *server {
		l, err := net.Listen("tcp", *addr)
		must(err)
		if *secure {
			l = tls.NewListener(l, cfg)
		}
		fmt.Println("ready")
		for {
			c, err := l.Accept()
			must(err)
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				if *mode == "stream" {
					io.Copy(io.Discard, c)
					return
				}
				for {
					if _, err := io.ReadFull(c, buf); err != nil {
						return
					}
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}(c)
		}
	}
	dial := func() net.Conn {
		c, err := net.Dial("tcp", *addr)
		must(err)
		if *secure {
			t := tls.Client(c, cfg)
			must(t.Handshake())
			fmt.Fprintln(os.Stderr, "cipher", tls.CipherSuiteName(t.ConnectionState().CipherSuite))
			c = t
		}
		return c
	}
	if *mode == "handshake" {
		c := dial()
		defer c.Close()
		state := c.(*tls.Conn).ConnectionState()
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"version": state.Version, "cipher": tls.CipherSuiteName(state.CipherSuite), "alpn": state.NegotiatedProtocol}))
		return
	}
	if *mode == "ping" {
		c := dial()
		defer c.Close()
		buf := make([]byte, 1024)
		for range 20 {
			_, err := c.Write(buf)
			must(err)
			_, err = io.ReadFull(c, buf)
			must(err)
		}
		var ns []int64
		start := time.Now()
		for time.Since(start) < *duration {
			t := time.Now()
			_, err := c.Write(buf)
			must(err)
			_, err = io.ReadFull(c, buf)
			must(err)
			ns = append(ns, time.Since(t).Nanoseconds())
		}
		sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": *mode, "tls": *secure, "addr": *addr, "n": len(ns), "p50_us": float64(ns[len(ns)/2]) / 1000, "p99_us": float64(ns[(len(ns)-1)*99/100]) / 1000, "seconds": time.Since(start).Seconds()}))
		return
	}
	conns := make([]net.Conn, *streams)
	for i := range conns {
		conns[i] = dial()
	}
	var wg sync.WaitGroup
	counts := make([]int64, *streams)
	start := time.Now()
	deadline := start.Add(*duration)
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			defer c.Close()
			must(c.SetWriteDeadline(deadline))
			buf := make([]byte, 16384)
			for {
				n, err := c.Write(buf)
				counts[i] += int64(n)
				if err != nil {
					return
				}
			}
		}(i, c)
	}
	wg.Wait()
	var total int64
	for _, n := range counts {
		total += n
	}
	secs := time.Since(start).Seconds()
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": *mode, "tls": *secure, "addr": *addr, "streams": *streams, "bytes": total, "seconds": secs, "mib_per_s": float64(total) / (1 << 20) / secs}))
}
