// Question: which faults can a seeded TCP relay apply to encrypted TLS streams,
// and what does a userspace relay cost on this shared host?
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"
)

type endpoint struct {
	listener     net.Listener
	clientConfig *tls.Config
	ready        chan *tls.Conn
	mu           sync.Mutex
	conns        map[*tls.Conn]bool
	closing      bool
	wg           sync.WaitGroup
}

func newEndpoint() (*endpoint, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return nil, e
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "local-sketch"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if e != nil {
		return nil, e
	}
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		return nil, e
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	config := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, DynamicRecordSizingDisabled: true}
	l, e := tls.Listen("tcp", "127.0.0.1:0", config)
	if e != nil {
		return nil, e
	}
	ep := &endpoint{listener: l, ready: make(chan *tls.Conn, 32), conns: make(map[*tls.Conn]bool), clientConfig: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, DynamicRecordSizingDisabled: true}}
	ep.wg.Add(1)
	go ep.accept()
	return ep, nil
}
func (ep *endpoint) accept() {
	defer ep.wg.Done()
	for {
		v, e := ep.listener.Accept()
		if e != nil {
			return
		}
		c := v.(*tls.Conn)
		ep.mu.Lock()
		if ep.closing {
			ep.mu.Unlock()
			_ = c.Close()
			return
		}
		ep.conns[c] = true
		ep.mu.Unlock()
		ep.wg.Add(1)
		go func() {
			defer ep.wg.Done()
			defer func() { _ = c.Close(); ep.mu.Lock(); delete(ep.conns, c); ep.mu.Unlock() }()
			_ = c.SetDeadline(time.Now().Add(30 * time.Second))
			if c.Handshake() != nil {
				return
			}
			ep.ready <- c
			b := make([]byte, 64*1024)
			for {
				n, e := c.Read(b)
				if n > 0 {
					if writeAll(c, b[:n]) != nil {
						return
					}
				}
				if e != nil {
					return
				}
			}
		}()
	}
}
func (ep *endpoint) close() {
	_ = ep.listener.Close()
	ep.mu.Lock()
	ep.closing = true
	for c := range ep.conns {
		_ = c.Close()
	}
	ep.mu.Unlock()
	ep.wg.Wait()
}
func dial(ep *endpoint, addr string) (*tls.Conn, *tls.Conn, error) {
	d := &net.Dialer{Timeout: 3 * time.Second}
	c, e := tls.DialWithDialer(d, "tcp", addr, ep.clientConfig)
	if e != nil {
		return nil, nil, e
	}
	select {
	case s := <-ep.ready:
		return c, s, nil
	case <-time.After(3 * time.Second):
		_ = c.Close()
		return nil, nil, errors.New("server handshake readiness timeout")
	}
}
func exchange(c *tls.Conn, payload, reply []byte) error {
	if e := writeAll(c, payload); e != nil {
		return e
	}
	if _, e := io.ReadFull(c, reply); e != nil {
		return e
	}
	if !bytes.Equal(payload, reply) {
		return errors.New("plaintext mismatch")
	}
	return nil
}
func cpuNS() int64 {
	var r syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	return r.Utime.Nano() + r.Stime.Nano()
}
func emit(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }
func traceHash(events []event) string {
	canonical := append([]event(nil), events...)
	for i := range canonical {
		canonical[i].AtNS = 0
	}
	sort.Slice(canonical, func(i, j int) bool {
		a, b := canonical[i], canonical[j]
		if a.Direction != b.Direction {
			return a.Direction < b.Direction
		}
		return a.Offset < b.Offset
	})
	b, _ := json.Marshal(canonical)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func exercise(seed uint64, reps int) error {
	for rep := 1; rep <= reps; rep++ {
		for _, mode := range modes {
			ep, e := newEndpoint()
			if e != nil {
				return e
			}
			p, e := newProxy(ep.listener.Addr().String(), mode, seed, false)
			if e != nil {
				ep.close()
				return e
			}
			c, s, e := dial(ep, p.listener.Addr().String())
			if e != nil {
				p.close()
				ep.close()
				return fmt.Errorf("%s handshake: %w", mode, e)
			}
			_ = c.SetDeadline(time.Now().Add(3 * time.Second))
			probe := bytes.Repeat([]byte{0xa7}, 64)
			got := make([]byte, len(probe))
			if e = exchange(c, probe, got); e != nil {
				_ = c.Close()
				p.close()
				ep.close()
				return e
			}
			p.policy.armed.Store(true)
			reverseOK := false
			if mode == "one-way-cut" || mode == "stall" {
				// Receive an independent server record while the request record is withheld.
				_ = c.SetDeadline(time.Now().Add(250 * time.Millisecond))
			} else if mode == "both-cut" || mode == "drop" || mode == "corrupt" || mode == "duplicate" || mode == "reset" {
				_ = c.SetDeadline(time.Now().Add(250 * time.Millisecond))
			}
			payload := bytes.Repeat([]byte{0x59}, 32*1024)
			reply := make([]byte, len(payload))
			start := time.Now()
			cpu := cpuNS()
			if mode == "one-way-cut" || mode == "stall" || mode == "both-cut" {
				e = writeAll(c, payload)
				// Wait until proxy confirms the requested action, rather than guessing with sleep.
				deadline := time.Now().Add(100 * time.Millisecond)
				for time.Now().Before(deadline) {
					p.mu.Lock()
					installed := len(p.events) > 0
					p.mu.Unlock()
					if installed {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if mode == "both-cut" {
					if e == nil {
						e = writeAll(s, payload)
					}
					if e == nil {
						_, e = io.ReadFull(c, reply)
					}
				} else {
					if e == nil {
						e = writeAll(s, probe)
					}
					if e == nil {
						_, e = io.ReadFull(c, got)
						reverseOK = e == nil && bytes.Equal(probe, got)
					}
					if e == nil {
						_, e = io.ReadFull(c, reply)
					}
				}
			} else {
				e = exchange(c, payload, reply)
			}
			elapsed := time.Since(start)
			used := cpuNS() - cpu
			// Close endpoints before collecting traces, so no late alert changes the record.
			_ = c.Close()
			p.close()
			ep.close()
			expectedSuccess := mode == "none" || mode == "short" || mode == "long" || mode == "switching" || mode == "fast" || mode == "slow"
			passed := (e == nil) == expectedSuccess
			if (mode == "stall" || mode == "one-way-cut") && !reverseOK {
				passed = false
			}
			if mode == "both-cut" {
				seen := [2]bool{}
				for _, ev := range p.events {
					seen[ev.Direction] = true
				}
				if !seen[0] || !seen[1] {
					passed = false
				}
			}
			if mode == "stall" || mode == "one-way-cut" || mode == "both-cut" {
				var ne net.Error
				if !errors.As(e, &ne) || !ne.Timeout() {
					passed = false
				}
			}
			if mode == "short" && elapsed < 9*time.Millisecond {
				passed = false
			}
			if mode == "long" && elapsed < 9*25*time.Millisecond {
				passed = false
			}
			if mode == "switching" {
				delays := map[int64]bool{}
				for _, ev := range p.events {
					delays[ev.DelayNS] = true
				}
				if !delays[int64(time.Millisecond)] || !delays[int64(25*time.Millisecond)] {
					passed = false
				}
			}
			if mode != "none" && len(p.events) == 0 {
				passed = false
			}
			if mode == "reset" && !errors.Is(e, syscall.ECONNRESET) {
				passed = false
			}
			if mode == "corrupt" || mode == "duplicate" || mode == "drop" {
				if e == nil || !bytes.Contains([]byte(e.Error()), []byte("bad record MAC")) {
					passed = false
				}
			}
			errText := ""
			if e != nil {
				errText = e.Error()
			}
			result := map[string]any{"kind": "exercise", "rep": rep, "mode": mode, "seed": seed, "pass": passed, "tls": "1.3", "cipher_suite": tls.CipherSuiteName(c.ConnectionState().CipherSuite), "payload_bytes": len(payload), "wall_ns": elapsed.Nanoseconds(), "process_cpu_ns": used, "reverse_independent_record_ok": reverseOK, "error": errText, "events": p.events, "policy_trace_sha256": traceHash(p.events)}
			if e := emit(result); e != nil {
				return e
			}
			if !passed {
				return fmt.Errorf("exercise failed for %s", mode)
			}
		}
	}
	return nil
}
func benchOne(kind string, rep, n, mb int, seed uint64) error {
	ep, e := newEndpoint()
	if e != nil {
		return e
	}
	defer ep.close()
	addr := ep.listener.Addr().String()
	var p *relayProcess
	if kind != "direct" {
		p, e = startRelay(addr, seed)
		if e != nil {
			return e
		}
		defer func() {
			if e := p.stop(); e != nil {
				fmt.Fprintln(os.Stderr, "relay stop:", e)
			}
		}()
		addr = p.addr
	}
	c, _, e := dial(ep, addr)
	if e != nil {
		return e
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	small := bytes.Repeat([]byte{0x62}, 256)
	smallReply := make([]byte, 256)
	for i := 0; i < 100; i++ {
		if e = exchange(c, small, smallReply); e != nil {
			return e
		}
	}
	latency := make([]int64, n)
	latencyProxyStart := tickSample(p)
	cpu := cpuNS()
	start := time.Now()
	for i := range latency {
		t := time.Now()
		if e = exchange(c, small, smallReply); e != nil {
			return e
		}
		latency[i] = time.Since(t).Nanoseconds()
	}
	latencyWall := time.Since(start)
	latencyCPU := cpuNS() - cpu
	latencyProxyTicks := tickSample(p) - latencyProxyStart
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	large := bytes.Repeat([]byte{0x63}, 32*1024)
	largeReply := make([]byte, len(large))
	for i := 0; i < 16; i++ {
		if e = exchange(c, large, largeReply); e != nil {
			return e
		}
	}
	total := mb * 1_000_000
	iterations := (total + len(large) - 1) / len(large)
	actual := iterations * len(large)
	bulkProxyStart := tickSample(p)
	cpu = cpuNS()
	start = time.Now()
	for i := 0; i < iterations; i++ {
		if e = exchange(c, large, largeReply); e != nil {
			return e
		}
	}
	wall := time.Since(start)
	used := cpuNS() - cpu
	bulkProxyTicks := tickSample(p) - bulkProxyStart
	hz := int64(0)
	proxyPID := 0
	proxyAffinity := "not applicable"
	var latencyProxyNS, bulkProxyNS int64
	if p != nil {
		hz = p.hz
		proxyPID = p.cmd.Process.Pid
		proxyAffinity = affinity(proxyPID)
		latencyProxyNS = latencyProxyTicks * 1e9 / hz
		bulkProxyNS = bulkProxyTicks * 1e9 / hz
	}
	bidirMB := float64(2*actual) / 1e6
	return emit(map[string]any{"kind": "benchmark", "path": kind, "rep": rep, "seed": seed, "gomaxprocs": runtime.GOMAXPROCS(0), "proxy_pid": proxyPID, "proxy_clock_ticks_per_second": hz, "proxy_cpus_allowed_list": proxyAffinity, "endpoint_cpus_allowed_list": affinity(os.Getpid()), "cipher_suite": tls.CipherSuiteName(c.ConnectionState().CipherSuite),
		"latency_ops": n, "latency_payload_bytes": 256, "latency_p50_ns": latency[(n-1)/2], "latency_p95_ns": latency[(n-1)*95/100], "latency_p99_ns": latency[(n-1)*99/100], "latency_wall_ns": latencyWall.Nanoseconds(), "latency_endpoint_process_cpu_ns": latencyCPU, "latency_proxy_cpu_ticks": latencyProxyTicks, "latency_proxy_process_cpu_ns": latencyProxyNS,
		"bulk_iterations": iterations, "bulk_payload_bytes": actual, "bulk_bidirectional_payload_bytes": 2 * actual, "bulk_wall_ns": wall.Nanoseconds(), "bulk_endpoint_process_cpu_ns": used, "bulk_proxy_cpu_ticks": bulkProxyTicks, "bulk_proxy_process_cpu_ns": bulkProxyNS, "bulk_total_process_cpu_ns": used + bulkProxyNS,
		"bulk_endpoint_cpu_ns_per_decimal_MB_bidir": float64(used) / bidirMB, "bulk_proxy_cpu_ns_per_decimal_MB_bidir": float64(bulkProxyNS) / bidirMB, "bulk_total_cpu_ns_per_decimal_MB_bidir": float64(used+bulkProxyNS) / bidirMB, "bulk_roundtrip_MB_per_s": float64(actual) / wall.Seconds() / 1e6})
}
func bench(seed uint64, reps, n, mb int) error {
	for rep := 1; rep <= reps; rep++ {
		order := []string{"direct", "proxy"}
		if rep%2 == 0 {
			order = []string{"proxy", "direct"}
		}
		for _, kind := range order {
			if e := benchOne(kind, rep, n, mb, seed); e != nil {
				return e
			}
		}
	}
	return nil
}
func run() error {
	command := flag.String("command", "exercise", "exercise, bench or serve")
	mode := flag.String("mode", "none", "fault mode for serve")
	seed := flag.Uint64("seed", 49, "policy seed")
	listen := flag.String("listen", "127.0.0.1:0", "serve listener")
	upstream := flag.String("upstream", "", "serve upstream TCP address")
	reps := flag.Int("reps", 3, "exercise or benchmark repetitions")
	ops := flag.Int("ops", 2000, "latency operations per repetition")
	mb := flag.Int("mb", 16, "bulk request bytes in decimal MB per repetition")
	flag.Parse()
	if *reps < 1 || *ops < 1 || *mb < 1 {
		return errors.New("reps, ops and mb must be positive")
	}
	switch *command {
	case "exercise":
		return exercise(*seed, *reps)
	case "bench":
		return bench(*seed, *reps, *ops, *mb)
	case "serve":
		if *upstream == "" {
			return errors.New("serve needs -upstream")
		}
		p, e := listenProxy(*listen, *upstream, *mode, *seed, true)
		if e != nil {
			return e
		}
		if e = emit(map[string]any{"kind": "listening", "address": p.listener.Addr().String(), "upstream": *upstream, "mode": *mode, "seed": *seed}); e != nil {
			p.close()
			return e
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		<-signals
		signal.Stop(signals)
		p.close()
		return emit(map[string]any{"kind": "trace", "events": p.events})
	default:
		return fmt.Errorf("unknown command %q", *command)
	}
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
