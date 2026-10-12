// Question: which encrypted transport is cheapest for two loopback append/ack links?
// Throwaway microbenchmark, not a Raft implementation or production UDP protocol.
package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	mr "math/rand/v2"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	quic "github.com/quic-go/quic-go"
)

var deadline = time.Date(2026, 10, 13, 22, 0, 0, 0, time.UTC)

func checkTime() {
	if !time.Now().Before(deadline) {
		panic("experiment deadline reached")
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func put(b []byte, n uint64) { binary.LittleEndian.PutUint64(b, n) }
func get(b []byte) uint64    { return binary.LittleEndian.Uint64(b) }
func writeAll(w io.Writer, b []byte) {
	for len(b) > 0 {
		n, e := w.Write(b)
		must(e)
		if n == 0 {
			panic("zero write")
		}
		b = b[n:]
	}
}

type link interface {
	roundtrip(uint64)
	close()
}
type streamLink struct {
	c       io.ReadWriteCloser
	payload []byte
	ack     [32]byte
	cleanup func()
	done    chan struct{}
}

func (l *streamLink) roundtrip(seq uint64) {
	put(l.payload[8:], seq)
	writeAll(l.c, l.payload)
	_, e := io.ReadFull(l.c, l.ack[:])
	must(e)
	if get(l.ack[:]) != seq {
		panic("bad stream ack")
	}
}
func (l *streamLink) close() { l.cleanup(); <-l.done }
func serveStream(c io.ReadWriteCloser, size int, done chan struct{}) {
	defer close(done)
	b := make([]byte, size+16)
	ack := make([]byte, 32)
	for {
		if _, e := io.ReadFull(c, b); e != nil {
			return
		}
		if get(b) != uint64(size) {
			panic("bad frame length")
		}
		put(ack, get(b[8:]))
		writeAll(c, ack)
	}
}
func tlsConfigs() (*tls.Config, *tls.Config) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(e)
	t := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "loopback"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IsCA: true, BasicConstraintsValid: true}
	der, e := x509.CreateCertificate(rand.Reader, t, t, &key.PublicKey, key)
	must(e)
	cert, e := x509.ParseCertificate(der)
	must(e)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	server := &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"append-bench"}}
	client := &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: roots, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"append-bench"}}
	return server, client
}
func newStream(kind string, size int, server, client *tls.Config) link {
	l := &streamLink{payload: make([]byte, size+16), done: make(chan struct{})}
	put(l.payload, uint64(size))
	_, e := rand.Read(l.payload[16:])
	must(e)
	if kind == "tls" {
		listener, e := net.Listen("tcp4", "127.0.0.1:0")
		must(e)
		accepted := make(chan *tls.Conn, 1)
		go func() {
			c, e := listener.Accept()
			must(e)
			tc := tls.Server(c, server)
			must(tc.Handshake())
			accepted <- tc
		}()
		raw, e := net.Dial("tcp4", listener.Addr().String())
		must(e)
		c := tls.Client(raw, client)
		must(c.Handshake())
		peer := <-accepted
		must(listener.Close())
		if c.ConnectionState().Version != tls.VersionTLS13 {
			panic("not TLS1.3")
		}
		l.c = c
		l.cleanup = func() { _ = c.Close(); _ = peer.Close() }
		go serveStream(peer, size, l.done)
	} else {
		// Defaults, including congestion control, MTU discovery, GSO, and receive windows.
		listener, e := quic.ListenAddr("127.0.0.1:0", server, &quic.Config{})
		must(e)
		accepted := make(chan *quic.Conn, 1)
		go func() { c, e := listener.Accept(context.Background()); must(e); accepted <- c }()
		c, e := quic.DialAddr(context.Background(), listener.Addr().String(), client, &quic.Config{})
		must(e)
		peer := <-accepted
		stream, e := c.OpenStreamSync(context.Background())
		must(e)
		l.c = stream
		go func() { s, e := peer.AcceptStream(context.Background()); must(e); serveStream(s, size, l.done) }()
		l.cleanup = func() { _ = c.CloseWithError(0, ""); _ = peer.CloseWithError(0, ""); _ = listener.Close() }
	}
	return l
}

// UDP uses AES-128-GCM, fresh independent directional keys per link, an explicit
// monotonic 64-bit packet nonce, and authenticated 24-byte headers. 1200-byte
// datagrams, reassembly, a 32-packet window and encrypted 32-byte cumulative acks.
// There is NO loss recovery, key exchange, replay window or congestion control.
// A missing packet fails the run rather than counting incomplete messages.
const packetSize = 1200
const headerSize = 24
const fragmentSize = packetSize - headerSize - 16
const window = 32

func newAEAD() cipher.AEAD {
	k := make([]byte, 16)
	_, e := rand.Read(k)
	must(e)
	b, e := aes.NewCipher(k)
	must(e)
	a, e := cipher.NewGCM(b)
	must(e)
	return a
}
func seal(a cipher.AEAD, dst []byte, packet, msg, offset uint64, payload []byte) []byte {
	dst = dst[:headerSize]
	put(dst, packet)
	put(dst[8:], msg)
	put(dst[16:], offset)
	var nonce [12]byte
	put(nonce[4:], packet)
	return a.Seal(dst, nonce[:], payload, dst)
}
func open(a cipher.AEAD, b []byte) []byte {
	if len(b) < headerSize+16 {
		panic("short UDP packet")
	}
	var nonce [12]byte
	put(nonce[4:], get(b))
	p, e := a.Open(b[headerSize:headerSize], nonce[:], b[headerSize:], b[:headerSize])
	must(e)
	return p
}

type udpLink struct {
	c, s    *net.UDPConn
	tx, rx  cipher.AEAD
	payload []byte
	packets uint64
	done    chan struct{}
	out, in []byte
	ackSeq  uint64
}

func newUDP(size int) link {
	s, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	must(e)
	c, e := net.DialUDP("udp4", nil, s.LocalAddr().(*net.UDPAddr))
	must(e)
	l := &udpLink{c: c, s: s, tx: newAEAD(), rx: newAEAD(), payload: make([]byte, size), done: make(chan struct{}), out: make([]byte, 0, packetSize), in: make([]byte, packetSize)}
	_, e = rand.Read(l.payload)
	must(e)
	// Absolute run safety deadline only, not a per-message timer in the hot path.
	must(c.SetDeadline(time.Now().Add(30 * time.Second)))
	must(s.SetDeadline(time.Now().Add(30 * time.Second)))
	go l.serve()
	return l
}
func (l *udpLink) serve() {
	defer close(l.done)
	b := make([]byte, packetSize)
	out := make([]byte, 0, 128)
	ack := make([]byte, 32)
	assembled := make([]byte, len(l.payload))
	seen := make([]bool, (len(assembled)+fragmentSize-1)/fragmentSize)
	var msg, ackSeq uint64
	received, inWindow := 0, 0
	for {
		n, addr, e := l.s.ReadFromUDPAddrPort(b)
		if e != nil {
			return
		}
		m, offset, pseq := get(b[8:]), int(get(b[16:])), get(b)
		p := open(l.tx, b[:n])
		if m != msg {
			if m != msg+1 || received != len(assembled) && msg != 0 {
				panic("unexpected UDP message")
			}
			msg = m
			received = 0
			inWindow = 0
			clear(seen)
		}
		if offset < 0 || offset+len(p) > len(assembled) || offset%fragmentSize != 0 || seen[offset/fragmentSize] {
			panic("bad UDP fragment")
		}
		copy(assembled[offset:], p)
		seen[offset/fragmentSize] = true
		received += len(p)
		inWindow++
		if inWindow == window || received == len(assembled) {
			ackSeq++
			put(ack, msg)
			put(ack[8:], pseq)
			put(ack[16:], uint64(received))
			out = seal(l.rx, out, ackSeq, msg, 0, ack)
			_, e = l.s.WriteToUDPAddrPort(out, addr)
			must(e)
			inWindow = 0
		}
	}
}
func (l *udpLink) roundtrip(seq uint64) {
	sent := 0
	for sent < len(l.payload) {
		for i := 0; i < window && sent < len(l.payload); i++ {
			end := min(sent+fragmentSize, len(l.payload))
			l.packets++
			l.out = seal(l.tx, l.out, l.packets, seq, uint64(sent), l.payload[sent:end])
			_, e := l.c.Write(l.out)
			must(e)
			sent = end
		}
		n, e := l.c.Read(l.in)
		must(e)
		ackSeq := get(l.in)
		p := open(l.rx, l.in[:n])
		if ackSeq <= l.ackSeq || len(p) != 32 || get(p) != seq || get(p[8:]) != l.packets || get(p[16:]) != uint64(sent) {
			panic("bad UDP ack")
		}
		l.ackSeq = ackSeq
	}
}
func (l *udpLink) close() { _ = l.c.Close(); _ = l.s.Close(); <-l.done }

func cpu() float64 {
	var r syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &r))
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

type result struct {
	Transport                                                                          string
	EntryBytes, Batch, Rep, Procs                                                      int
	Messages                                                                           int
	Seconds, MessagesPerSec, PayloadBytesPerSec, CPUUSPerMessage, P50US, P99US, MeanUS float64
}

func run(kind string, entry, batch, rep int, duration time.Duration, server, client *tls.Config) result {
	checkTime()
	links := make([]link, 2)
	for i := range links {
		if kind == "udp" {
			links[i] = newUDP(entry * batch)
		} else {
			links[i] = newStream(kind, entry*batch, server, client)
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	samples := make([][]int64, 2)
	for i, l := range links {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq uint64
			warm := time.Now().Add(250 * time.Millisecond)
			for time.Now().Before(warm) {
				seq++
				l.roundtrip(seq)
			}
			samples[i] = make([]int64, 0, 300000)
			ready <- struct{}{}
			<-start
			end := time.Now().Add(duration)
			for time.Now().Before(end) {
				seq++
				t := time.Now()
				l.roundtrip(seq)
				samples[i] = append(samples[i], time.Since(t).Nanoseconds())
			}
		}()
	}
	<-ready
	<-ready
	c0 := cpu()
	t := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t).Seconds()
	used := cpu() - c0
	for _, l := range links {
		l.close()
	}
	all := append(samples[0], samples[1]...)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var sum int64
	for _, x := range all {
		sum += x
	}
	n := len(all)
	return result{kind, entry, batch, rep, runtime.GOMAXPROCS(0), n, elapsed, float64(n) / elapsed, float64(n) * float64(entry*batch) / elapsed, used * 1e6 / float64(n), float64(all[n/2]) / 1e3, float64(all[n*99/100]) / 1e3, float64(sum) / float64(n) / 1e3}
}
func main() {
	reps := flag.Int("reps", 5, "repetitions")
	duration := flag.Duration("duration", 2*time.Second, "measurement time per case")
	procs := flag.Int("procs", 4, "GOMAXPROCS")
	smoke := flag.Bool("smoke", false, "one repetition, 100ms per case")
	flag.Parse()
	runtime.GOMAXPROCS(*procs)
	checkTime()
	server, client := tlsConfigs()
	if *smoke {
		*reps = 1
		*duration = 100 * time.Millisecond
	}
	type spec struct {
		kind         string
		entry, batch int
	}
	var cases []spec
	for _, kind := range []string{"tls", "quic", "udp"} {
		for _, entry := range []int{1024, 10240} {
			for _, batch := range []int{1, 16, 256} {
				cases = append(cases, spec{kind, entry, batch})
			}
		}
	}
	rng := mr.New(mr.NewPCG(53, 47))
	enc := json.NewEncoder(os.Stdout)
	fmt.Fprintf(os.Stderr, "%s %s/%s GOMAXPROCS=%d, started %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, *procs, time.Now().UTC().Format(time.RFC3339))
	for rep := 1; rep <= *reps; rep++ {
		rng.Shuffle(len(cases), func(i, j int) { cases[i], cases[j] = cases[j], cases[i] })
		for _, s := range cases {
			r := run(s.kind, s.entry, s.batch, rep, *duration, server, client)
			must(enc.Encode(r))
		}
	}
}
