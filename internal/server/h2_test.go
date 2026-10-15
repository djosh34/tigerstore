package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestHTTP1HealthAndVersion reads /health, /version and /readyz over
// HTTP/1.1, as a probe that does not offer h2 does.
func TestHTTP1HealthAndVersion(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 90)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost"},
		// A non-nil empty map turns HTTP/2 off.
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}}
	get := func(path string) (int, string, int) {
		resp, err := hc.Get("https://" + c.running[1].addr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // closing a test response
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b), resp.ProtoMajor
	}
	for range 2 { // the second round reuses the connection
		if code, body, proto := get("/version"); code != 200 || proto != 1 || body != `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}` {
			t.Fatalf("/version = %d HTTP/%d %q", code, proto, body)
		}
		if code, body, _ := get("/health?serializable=true"); code != 200 || body != `{"health":"true","reason":""}` {
			t.Fatalf("/health = %d %q", code, body)
		}
		if code, _, _ := get("/nothing"); code != 404 {
			t.Fatalf("/nothing = %d", code)
		}
		if code, body, _ := get("/readyz?verbose"); code != 200 || body != "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok\n" {
			t.Fatalf("/readyz = %d %q", code, body)
		}
	}
	resp, err := hc.Post("https://"+c.running[1].addr+"/livez", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.ProtoMajor != 1 || resp.Header.Get("Allow") != "GET" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("POST /livez = %d HTTP/%d %v", resp.StatusCode, resp.ProtoMajor, resp.Header)
	}
}

// TestLargeValues puts and reads values near etcd's request limit, so the
// request spans many frames and the response waits on the client's flow
// control; a request above the limit fails like grpc-go's.
func TestLargeValues(t *testing.T) {
	// With 10 ms ticks the leader's commit watchdog (three election
	// timeouts, 300 ms) stopped the leader in CI during these 1.5 MiB
	// writes. Why an entry waited that long is not known; slow fsyncs on
	// a shared runner are a guess, and the stall is open as #88. 50 ms
	// ticks give it 1.5 s, still faster than the 100 ms of a real node.
	c := newClusterWith(t, func(c *cluster) { c.tick = 50 * time.Millisecond })
	cert, _, _, err := c.ca.Issue("client", 91)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(2, cert)
	value := bytes.Repeat([]byte("0123456789abcdef"), (core.MaxRequestBytes-1024)/16)
	for i := range 4 {
		key := []byte{'k', byte('0' + i)}
		retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}, grpc.MaxCallRecvMsgSize(16<<20))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 4 {
		t.Fatalf("got %d keys", len(r.Kvs))
	}
	for _, kv := range r.Kvs {
		if !bytes.Equal(kv.Value, value) {
			t.Fatalf("%s: value of %d bytes differs", kv.Key, len(kv.Value))
		}
	}
	_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("big"), Value: make([]byte, 3<<20)})
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "larger than max") {
		t.Fatalf("put of 3 MiB: %v", err)
	}
	// The connection still works.
	if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("small"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
}

// TestDeadlinesLeaderAndUnknownMethods checks the call metadata: the
// grpc-timeout header, etcd's require-leader metadata on unary calls, and
// Unimplemented for methods that are not served.
func TestDeadlinesLeaderAndUnknownMethods(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 92)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv := pb.NewKVClient(conn)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	err = conn.Invoke(context.Background(), "/nothing.Here/Call", &pb.RangeRequest{}, &pb.RangeResponse{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown service: %v", err)
	}
	ms, err := pb.NewClusterClient(conn).MemberList(context.Background(), &pb.MemberListRequest{})
	if err != nil || ms.Header.MemberId != 1 {
		t.Fatalf("MemberList: %v %v", ms, err)
	}

	lctx := metadata.AppendToOutgoingContext(context.Background(), rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader)
	ka, err := pb.NewLeaseClient(conn).LeaseKeepAlive(lctx)
	if err != nil {
		t.Fatal(err)
	}

	// Without a quorum, a write waits until its deadline, and a call that
	// requires a leader fails at once once the leader is gone.
	c.running[2].stop()
	c.running[3].stop()
	delete(c.running, 2)
	delete(c.running, 3)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")})
	cancel()
	if status.Code(err) != codes.DeadlineExceeded || time.Since(start) > 3*time.Second {
		t.Fatalf("put without a quorum: %v after %v", err, time.Since(start))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(lctx, time.Second)
		_, err = kv.Range(ctx, &pb.RangeRequest{Key: []byte("a")})
		cancel()
		if errors.Is(err, rpctypes.ErrGRPCNoLeader) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("range that requires a leader without one: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The keepalive stream that requires a leader ends too, as etcd's
	// stream interceptor ends it.
	if _, err := ka.Recv(); !errors.Is(err, rpctypes.ErrGRPCNoLeader) {
		t.Fatalf("keepalive stream without a leader: %v", err)
	}
}

// TestKeepAliveStream renews a lease over the bidirectional stream.
func TestKeepAliveStream(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 93)
	if err != nil {
		t.Fatal(err)
	}
	lc := pb.NewLeaseClient(c.dial(3, cert))
	g := retry(t, func(ctx context.Context) (*pb.LeaseGrantResponse, error) {
		return lc.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 30})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ka, err := lc.LeaseKeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := ka.Send(&pb.LeaseKeepAliveRequest{ID: g.ID}); err != nil {
			t.Fatal(err)
		}
		r, err := ka.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != g.ID || r.TTL != 30 {
			t.Fatalf("keepalive = %v", r)
		}
	}
	if err := ka.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := ka.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("after CloseSend: %v", err)
	}
}

// TestCanceledCalls cancels many calls in flight, some before and some
// after they reach the node. Their late replies are dropped and the
// connection keeps working.
func TestCanceledCalls(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%20)*100*time.Microsecond)
			defer cancel()
			_, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")})
			if err != nil && status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Canceled {
				t.Errorf("put: %v", err)
			}
		})
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("3")}); err != nil {
		t.Fatal(err)
	}
}

// TestRawFrames speaks HTTP/2 to the server directly: settings, ping, a
// reset stream whose reply comes late, a call after it, and GOAWAY when
// the server stops.
func TestRawFrames(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	tc, err := tls.Dial("tcp", c.running[1].addr, &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost", NextProtos: []string{"h2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close() //nolint:errcheck // closing a test connection
	if err := tc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tc, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(tc, tc)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(fr.WriteSettings())
	must(fr.WritePing(false, [8]byte{1, 2, 3}))
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	headers := func(id uint32, path string, extra ...hpack.HeaderField) {
		t.Helper()
		hb.Reset()
		for _, f := range append([]hpack.HeaderField{
			{Name: ":method", Value: "POST"},
			{Name: ":scheme", Value: "https"},
			{Name: ":path", Value: path},
			{Name: ":authority", Value: "localhost"},
			{Name: "content-type", Value: "application/grpc"},
			{Name: "te", Value: "trailers"},
		}, extra...) {
			must(enc.WriteField(f))
		}
		must(fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: hb.Bytes(), EndHeaders: true}))
	}
	call := func(id uint32, path string, req proto.Message) {
		t.Helper()
		headers(id, path)
		b, err := proto.Marshal(req)
		must(err)
		must(fr.WriteData(id, true, append([]byte{0, 0, 0, 0, byte(len(b))}, b...)))
	}
	call(1, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("raw"), Value: []byte("1")})
	must(fr.WriteRSTStream(1, http2.ErrCodeCancel))
	call(3, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("raw"), Value: []byte("2")})
	call(5, "/etcdserverpb.Maintenance/Nothing", &pb.DefragmentRequest{})
	// A second end of the same stream is a stream error, not a second put.
	call(7, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("once"), Value: []byte("1")})
	must(fr.WriteData(7, true, nil))
	// A call whose request never completes still ends at its deadline.
	headers(9, "/etcdserverpb.KV/Put", hpack.HeaderField{Name: "grpc-timeout", Value: "100m"})

	var settings, pinged bool
	statuses := map[uint32]string{}
	reset7 := false
	for statuses[3] == "" || statuses[5] == "" || statuses[9] == "" || (statuses[7] == "" && !reset7) || !settings || !pinged {
		f, err := fr.ReadFrame()
		must(err)
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				settings = true
				must(fr.WriteSettingsAck())
			}
		case *http2.PingFrame:
			pinged = f.IsAck() && f.Data == [8]byte{1, 2, 3}
		case *http2.RSTStreamFrame:
			reset7 = reset7 || f.StreamID == 7
		case *http2.MetaHeadersFrame:
			if v := f.PseudoValue("status"); v != "" && v != "200" {
				t.Fatalf("stream %d: HTTP status %s", f.StreamID, v)
			}
			for _, h := range f.RegularFields() {
				if h.Name == "grpc-status" {
					statuses[f.StreamID] = h.Value
				}
			}
		}
	}
	if statuses[3] != "0" || statuses[5] != "12" || statuses[9] != "4" {
		t.Fatalf("grpc-status by stream: %v", statuses)
	}
	// The reset may have come before the put reached the node, so the
	// put may or may not have happened, but never twice.
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("once")})
	if err != nil || len(r.Kvs) > 1 || (len(r.Kvs) == 1 && r.Kvs[0].Version != 1) {
		t.Fatalf("key written by one put: %v %v", r, err)
	}

	// A finished put goes to the node even when the next frame is a bad
	// header block or one that continues in a later frame.
	waitStatus := func(id uint32) string {
		t.Helper()
		for {
			f, err := fr.ReadFrame()
			must(err)
			if h, ok := f.(*http2.MetaHeadersFrame); ok && h.StreamID == id {
				for _, hf := range h.RegularFields() {
					if hf.Name == "grpc-status" {
						return hf.Value
					}
				}
			}
		}
	}
	call(11, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("next"), Value: []byte("1")})
	headers(13, "/etcdserverpb.KV/Put", hpack.HeaderField{Name: "X-Upper", Value: "bad"})
	if got := waitStatus(11); got != "0" {
		t.Fatalf("put before a bad header block: grpc-status %s", got)
	}
	call(15, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("next"), Value: []byte("2")})
	hb.Reset()
	must(enc.WriteField(hpack.HeaderField{Name: ":method", Value: "POST"}))
	must(fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 17, BlockFragment: hb.Bytes()}))
	if got := waitStatus(15); got != "0" {
		t.Fatalf("put before a continued header block: grpc-status %s", got)
	}
	hb.Reset()
	for _, f := range []hpack.HeaderField{
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"},
		{Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"},
	} {
		must(enc.WriteField(f))
	}
	must(fr.WriteContinuation(17, true, hb.Bytes()))
	b, err := proto.Marshal(&pb.PutRequest{Key: []byte("next"), Value: []byte("3")})
	must(err)
	must(fr.WriteData(17, true, append([]byte{0, 0, 0, 0, byte(len(b))}, b...)))
	if got := waitStatus(17); got != "0" {
		t.Fatalf("put with a continued header block: grpc-status %s", got)
	}

	// Stopping the server sends GOAWAY before the connection closes.
	go c.running[1].stop()
	delete(c.running, 1)
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("connection closed without GOAWAY: %v", err)
		}
		if ga, ok := f.(*http2.GoAwayFrame); ok {
			if ga.ErrCode != http2.ErrCodeNo || ga.LastStreamID < 17 {
				t.Fatalf("GOAWAY %v", ga)
			}
			break
		}
	}
}

// TestNodeStopsUnderLoad stops a node while a connection has calls
// waiting on it and keeps sending new ones as it stops: every call gets
// an answer, none hangs.
func TestNodeStopsUnderLoad(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 89)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	// Without the other two, writes wait for a quorum that never comes.
	c.running[2].stop()
	c.running[3].stop()
	delete(c.running, 2)
	delete(c.running, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			// Waiting calls first, then a stream of reads and writes
			// that runs into the stop.
			for i := 0; ; i++ {
				var err error
				if i%2 == 0 {
					_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")})
				} else {
					_, err = kv.Range(ctx, &pb.RangeRequest{Key: []byte("a"), Serializable: true})
				}
				if err == nil {
					continue
				}
				if !errors.Is(err, rpctypes.ErrGRPCStopped) && !errors.Is(err, rpctypes.ErrGRPCNoLeader) {
					t.Errorf("call while the node stopped: %v", err)
				}
				if errors.Is(err, rpctypes.ErrGRPCStopped) || ctx.Err() != nil {
					return
				}
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	c.running[1].stopNode()
	wg.Wait()
}

// TestRequestSizeLimitWithCertificate checks the largest put over TLS
// with a client certificate: etcd v3.7.2 puts the certificate's common
// name and the auth revision into the request's header and limits that
// header too (#99), so a longer name leaves less room for the value. A
// request with grpcgateway-accept metadata has no identity in etcd.
func TestRequestSizeLimitWithCertificate(t *testing.T) {
	c := newCluster(t)
	key := make([]byte, 39)
	for _, tc := range []struct {
		name    string
		gateway bool
		largest int
	}{{"client", false, 1572791}, {"a-much-longer-client-name", false, 1572772}, {"client", true, 1572801}} {
		cert, _, _, err := c.ca.Issue(tc.name, 96)
		if err != nil {
			t.Fatal(err)
		}
		kv := c.client(1, cert)
		retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte("ready"), Value: []byte("1")})
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if tc.gateway {
			ctx = metadata.AppendToOutgoingContext(ctx, "grpcgateway-accept", "application/json")
		}
		if _, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: make([]byte, tc.largest)}); err != nil {
			t.Errorf("%s: put of %d bytes: %v", tc.name, tc.largest, err)
		}
		if _, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: make([]byte, tc.largest+1)}); !errors.Is(rpctypes.Error(err), rpctypes.ErrRequestTooLarge) {
			t.Errorf("%s: put of %d bytes: %v, want too large", tc.name, tc.largest+1, err)
		}
		cancel()
	}
}
