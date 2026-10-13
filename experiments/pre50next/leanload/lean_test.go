package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func responseWire(t *testing.T, rev int64) []byte {
	t.Helper()
	body, err := proto.Marshal(&pb.PutResponse{Header: &pb.ResponseHeader{Revision: rev, RaftTerm: 7}})
	if err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, len(body)+5)
	binary.BigEndian.PutUint32(wire[1:5], uint32(len(body)))
	copy(wire[5:], body)
	return wire
}
func initialHeaders() []hpack.HeaderField {
	return []hpack.HeaderField{{Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}}
}
func statusHeaders(code string) []hpack.HeaderField {
	return []hpack.HeaderField{{Name: "grpc-status", Value: code}}
}

func TestUnaryFraming(t *testing.T) {
	wire := responseWire(t, 2)
	// Every possible split, including a split inside the five-byte envelope.
	for split := 0; split <= len(wire); split++ {
		var r unaryResponse
		if err := r.headers(initialHeaders(), false); err != nil {
			t.Fatal(err)
		}
		if err := r.data(wire[:split]); err != nil {
			t.Fatal(err)
		}
		if err := r.data(wire[split:]); err != nil {
			t.Fatal(err)
		}
		if err := r.headers(statusHeaders("0"), true); err != nil {
			t.Fatal(err)
		}
		h, err := r.finish()
		if err != nil || h.Revision != 2 || h.RaftTerm != 7 {
			t.Fatalf("split=%d: %v, %v", split, h, err)
		}
	}
}

func TestBadUnaryResponses(t *testing.T) {
	wire := responseWire(t, 2)
	for _, name := range []string{"status", "missing-status", "duplicate-status", "duplicate-message", "duplicate-trailers", "missing-message", "truncated", "compressed", "oversized", "malformed-protobuf", "missing-header", "http", "missing-content-type", "early-data", "nonterminal-status", "data-end"} {
		t.Run(name, func(t *testing.T) {
			var r unaryResponse
			headers := initialHeaders()
			if name == "http" {
				headers[0].Value = "500"
			}
			if name == "missing-content-type" {
				headers = headers[:1]
			}
			var err error
			if name == "early-data" {
				err = r.data(wire)
			} else {
				err = r.headers(headers, false)
			}
			data := bytes.Clone(wire)
			switch name {
			case "missing-message":
				data = nil
			case "duplicate-message":
				data = append(data, wire...)
			case "truncated":
				data = data[:len(data)-1]
			case "compressed":
				data[0] = 1
			case "oversized":
				binary.BigEndian.PutUint32(data[1:5], maxResponseBytes)
			case "malformed-protobuf":
				data = []byte{0, 0, 0, 0, 1, 0xff}
			case "missing-header":
				data = []byte{0, 0, 0, 0, 0}
			}
			if err == nil {
				err = r.data(data)
			}
			trailers := statusHeaders("0")
			if name == "status" {
				trailers[0].Value = "13"
			}
			if name == "missing-status" {
				trailers = nil
			}
			if name == "duplicate-status" {
				trailers = append(trailers, trailers[0])
			}
			if err == nil && name != "data-end" {
				err = r.headers(trailers, name != "nonterminal-status")
			}
			if err == nil && name == "duplicate-trailers" {
				err = r.headers(trailers, true)
			}
			if err == nil {
				_, err = r.finish()
			}
			if err == nil {
				t.Fatal("bad response accepted")
			}
		})
	}
}

func TestPreencodedRequestsAndHPACK(t *testing.T) {
	keys, values := workload()
	wire, err := encodeRequests(keys, values)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, keyspace)
	for i, p := range wire {
		if len(p) >= 2<<10 || p[0] != 0 || int(binary.BigEndian.Uint32(p[1:5])) != len(p)-5 {
			t.Fatalf("request %d framing", i)
		}
		var r pb.PutRequest
		if err := proto.Unmarshal(p[5:], &r); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(r.Key, keys[i]) || !bytes.Equal(r.Value, values[i]) || len(r.Value) != 1024 || seen[string(r.Value)] {
			t.Fatalf("request %d workload", i)
		}
		seen[string(r.Value)] = true
	}
	block, size, err := putHeaders("127.0.0.1:2379")
	if err != nil || size == 0 {
		t.Fatalf("HPACK: %v", err)
	}
	decoder := hpack.NewDecoder(4096, nil)
	for _, limit := range []uint32{4096, 0, 64, 4096} {
		decoder.SetAllowedMaxDynamicTableSize(limit)
		fields, err := decoder.DecodeFull(block)
		if err != nil {
			t.Fatal(err)
		}
		var path string
		for _, h := range fields {
			if h.Name == ":path" {
				path = h.Value
			}
			if h.Name == "grpc-timeout" {
				t.Fatal("unexpected timeout metadata")
			}
		}
		if path != "/etcdserverpb.KV/Put" {
			t.Fatalf("bad path %q", path)
		}
	}
}

func TestSettingsAndFlowCredit(t *testing.T) {
	var out bytes.Buffer
	l := &leanClient{f: http2.NewFramer(&out, nil), window: 2, streamWindow: 100, maxFrame: 16384, pending: map[uint32]int{1: 0}, slots: []leanPending{{id: 1, window: 20}}, next: 3}
	var result connectionResult
	if err := l.apply(leanEvent{kind: eventSettings, settings: []http2.Setting{{ID: http2.SettingInitialWindowSize, Val: 0}, {ID: http2.SettingMaxConcurrentStreams, Val: 1}, {ID: http2.SettingMaxFrameSize, Val: 32768}}}, &result); err != nil {
		t.Fatal(err)
	}
	if l.slots[0].window != -80 || l.streamWindow != 0 || l.maxStreams != 1 || l.maxFrame != 32768 {
		t.Fatal("SETTINGS did not adjust active/new stream credit")
	}
	if err := l.apply(leanEvent{kind: eventWindow, id: 1, delta: 82}, &result); err != nil {
		t.Fatal(err)
	}
	if l.slots[0].window != 2 {
		t.Fatal("stream WINDOW_UPDATE lost")
	}
	if err := l.apply(leanEvent{kind: eventWindow, delta: maxWindow}, &result); err == nil {
		t.Fatal("connection credit overflow accepted")
	}
	if err := l.apply(leanEvent{kind: eventWindow, id: 1, delta: 0}, &result); err == nil {
		t.Fatal("zero credit accepted")
	}
	delete(l.pending, 1)
	if err := l.apply(leanEvent{kind: eventWindow, id: 1, delta: 10}, &result); err != nil {
		t.Fatal("late closed stream credit should be ignored", err)
	}
	if err := l.apply(leanEvent{kind: eventWindow, id: 3, delta: 1}, &result); err == nil {
		t.Fatal("idle stream credit accepted")
	}
	if err := l.apply(leanEvent{kind: eventHeaders, id: 1}, &result); err == nil {
		t.Fatal("duplicate response accepted")
	}
}

func TestSendDataRespectsBothWindows(t *testing.T) {
	var out bytes.Buffer
	l := &leanClient{f: http2.NewFramer(&out, nil), window: 3, maxFrame: 16384, slots: []leanPending{{id: 1, request: []byte("abcdef"), window: 2}}}
	if err := l.sendData(); err != nil {
		t.Fatal(err)
	}
	if l.slots[0].offset != 2 || l.slots[0].window != 0 || l.window != 1 {
		t.Fatal("per-stream credit ignored")
	}
	if err := addCredit(&l.slots[0].window, 4); err != nil {
		t.Fatal(err)
	}
	if err := l.sendData(); err != nil {
		t.Fatal(err)
	}
	if l.slots[0].offset != 3 || l.window != 0 {
		t.Fatal("connection credit ignored")
	}
	if err := addCredit(&l.window, 3); err != nil {
		t.Fatal(err)
	}
	if err := l.sendData(); err != nil {
		t.Fatal(err)
	}
	f := http2.NewFramer(nil, &out)
	for i, want := range []string{"ab", "c", "def"} {
		frame, err := f.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		v, ok := frame.(*http2.DataFrame)
		if !ok || string(v.Data()) != want || v.StreamEnded() != (i == 2) {
			t.Fatalf("frame %d: %v", i, frame)
		}
	}
}

func TestReceiveFlowIncludesPadding(t *testing.T) {
	var out bytes.Buffer
	l := &leanClient{f: http2.NewFramer(&out, nil), receiveWindow: 5, pending: map[uint32]int{1: 0}, slots: []leanPending{{id: 1, receiveWindow: 5, response: unaryResponse{initial: true}}}}
	var result connectionResult
	if err := l.apply(leanEvent{kind: eventData, id: 1, data: []byte{0}, flow: 5}, &result); err != nil {
		t.Fatal(err)
	}
	if l.receiveWindow != 0 || l.slots[0].receiveWindow != 0 || l.received != 5 {
		t.Fatal("padding was not charged")
	}
	if err := l.apply(leanEvent{kind: eventData, id: 1, flow: 1}, &result); err == nil {
		t.Fatal("receive-window overrun accepted")
	}
}

func TestBudgetFiniteAndDuration(t *testing.T) {
	for _, total := range []uint64{1, 63, 64, 65, 257, 4096} {
		b := &budget{total: total}
		seen := make([]atomic.Uint32, total)
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				for {
					n, ok := b.claim()
					if !ok {
						return
					}
					seen[n].Add(1)
				}
			})
		}
		wg.Wait()
		if b.next.Load() != total {
			t.Fatal("wrong finite total")
		}
		for i := range seen {
			if seen[i].Load() != 1 {
				t.Fatalf("ticket %d duplicated/missing", i)
			}
		}
	}
	b := &budget{deadline: time.Now().Add(-time.Millisecond)}
	if _, ok := b.claim(); ok || b.next.Load() != 0 {
		t.Fatal("arrival after deadline")
	}
	b = &budget{total: 100, deadline: time.Now().Add(-time.Millisecond)}
	if _, ok := b.claim(); ok {
		t.Fatal("duration did not constrain finite total")
	}
}

func TestCohortValidation(t *testing.T) {
	before := &pb.ResponseHeader{Revision: 10, RaftTerm: 7}
	after := &pb.ResponseHeader{Revision: 13, RaftTerm: 7}
	good := []measurement{{revision: 13, term: 7}, {revision: 11, term: 7}, {revision: 12, term: 7}}
	if err := validateCohort(good, 3, 3, before, after); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]measurement{
		{{revision: 11, term: 7}, {revision: 11, term: 7}, {revision: 13, term: 7}},
		{{revision: 11, term: 7}, {revision: 12, term: 8}, {revision: 13, term: 7}},
		{{revision: 11, term: 7}, {revision: 12, term: 7}},
	} {
		if err := validateCohort(bad, 3, 3, before, after); err == nil {
			t.Fatal("invalid cohort accepted")
		}
	}
}

// A real grpc-go TLS server, with a client-certificate requirement. The fixture
// is not a benchmark and does not make any storage or durability claim.
type fakeKV struct {
	pb.UnimplementedKVServer
	revision    atomic.Int64
	calls       atomic.Uint64
	badMetadata atomic.Bool
}

func (s *fakeKV) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if len(r.Value) != valueBytes || !bytes.HasPrefix(r.Key, []byte("/registry/pods/")) {
		return nil, errors.New("bad workload")
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("grpc-timeout")) != 0 {
		s.badMetadata.Store(true)
	}
	if _, ok := ctx.Deadline(); ok {
		s.badMetadata.Store(true)
	}
	s.calls.Add(1)
	return &pb.PutResponse{Header: &pb.ResponseHeader{Revision: s.revision.Add(1), RaftTerm: 7}}, nil
}

func tlsFixture(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "leanload-test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	client := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	server := &tls.Config{ClientCAs: pool, Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	return client, server
}

func grpcFixture(t *testing.T) (string, *tls.Config, *fakeKV) {
	t.Helper()
	client, server := tlsFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	kv := &fakeKV{}
	kv.revision.Store(1)
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(server)), grpc.MaxConcurrentStreams(2), grpc.InitialWindowSize(65535), grpc.InitialConnWindowSize(65535))
	pb.RegisterKVServer(s, kv)
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		s.Stop()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return listener.Addr().String(), client, kv
}

func runFixture(t *testing.T, endpoint string, cfg *tls.Config, b *budget, workers, conns int, duration time.Duration) []measurement {
	t.Helper()
	keys, values := workload()
	requests, err := encodeRequests(keys, values)
	if err != nil {
		t.Fatal(err)
	}
	clients := make([]*leanClient, conns)
	for i := range clients {
		slots := workers / conns
		if i < workers%conns {
			slots++
		}
		clients[i], err = newLeanClient(endpoint, cfg, requests, slots)
		if err != nil {
			t.Fatal(err)
		}
		c := clients[i]
		t.Cleanup(func() { c.close(); <-c.readDone })
	}
	results := make([]connectionResult, conns)
	if duration > 0 {
		b.deadline = time.Now().Add(duration)
	}
	abort := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			results[i] = c.run(b, abort)
			if results[i].err != nil {
				once.Do(func() { close(abort) })
			}
		})
	}
	wg.Wait()
	var all []measurement
	for _, r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		all = append(all, r.samples...)
	}
	return all
}

func TestRealTLSFiniteTotals(t *testing.T) {
	endpoint, cfg, kv := grpcFixture(t)
	for _, tc := range []struct {
		total          uint64
		workers, conns int
	}{
		{1, 8, 2}, {7, 8, 2}, {8, 8, 2}, {9, 8, 2}, {4096, 64, 16}, {257, 256, 16},
	} {
		t.Run(strconv.FormatUint(tc.total, 10), func(t *testing.T) {
			before := kv.revision.Load()
			b := &budget{total: tc.total}
			all := runFixture(t, endpoint, cfg, b, tc.workers, tc.conns, 0)
			if err := validateCohort(all, b.next.Load(), tc.total, &pb.ResponseHeader{Revision: before, RaftTerm: 7}, &pb.ResponseHeader{Revision: kv.revision.Load(), RaftTerm: 7}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if kv.badMetadata.Load() {
		t.Fatal("measured Put used a context deadline/grpc-timeout")
	}
}

func TestRealTLSDurationDrains(t *testing.T) {
	endpoint, cfg, kv := grpcFixture(t)
	b := &budget{}
	all := runFixture(t, endpoint, cfg, b, 8, 2, 100*time.Millisecond)
	if len(all) == 0 || time.Now().Before(b.deadline) || kv.calls.Load() != b.next.Load() {
		t.Fatal("duration count/drain failed")
	}
	if err := validateCohort(all, b.next.Load(), 0, &pb.ResponseHeader{Revision: 1, RaftTerm: 7}, &pb.ResponseHeader{Revision: kv.revision.Load(), RaftTerm: 7}); err != nil {
		t.Fatal(err)
	}
}

func TestParseCPUAndFlags(t *testing.T) {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[11], fields[12], fields[19] = "S", "123", "45", "999"
	c, err := parseCPU(42, []byte("42 (worker (cpu) name) "+strings.Join(fields, " ")))
	if err != nil || c.PID != 42 || c.Ticks != 168 || c.Start != 999 {
		t.Fatalf("%v, %v", c, err)
	}
	if _, err := parseCPU(42, []byte("42 (short) S")); err == nil {
		t.Fatal("short stat accepted")
	}
	for _, args := range [][]string{{"-op", "range"}, {"-hz", "NaN"}, {"-hz", "Inf"}, {"-duration", "-1s"}, {"-total", "0"}, {"-conns", "0"}, {"-workers", "1", "-conns", "2"}} {
		if _, err := parseOptions(append(args, "-out", "test.json")); err == nil {
			t.Fatalf("bad flags accepted: %v", args)
		}
	}
	if _, err := parseOptions([]string{"-total", "60000", "-workers", "64", "-conns", "16", "-out", "test.json"}); err != nil {
		t.Fatal(err)
	}
}
