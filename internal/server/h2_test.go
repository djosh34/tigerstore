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

// TestHTTP1HealthAndVersion reads /health and /version over HTTP/1.1, as a
// probe that does not offer h2 does.
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
	}
}

// TestLargeValues puts and reads values near etcd's request limit, so the
// request spans many frames and the response waits on the client's flow
// control; a request above the limit fails like grpc-go's.
func TestLargeValues(t *testing.T) {
	c := newCluster(t)
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
	_, err = pb.NewAuthClient(conn).Authenticate(context.Background(), &pb.AuthenticateRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Authenticate: %v", err)
	}
	err = conn.Invoke(context.Background(), "/nothing.Here/Call", &pb.RangeRequest{}, &pb.RangeResponse{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown service: %v", err)
	}
	ms, err := pb.NewClusterClient(conn).MemberList(context.Background(), &pb.MemberListRequest{})
	if err != nil || ms.Header.MemberId != 1 {
		t.Fatalf("MemberList: %v %v", ms, err)
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
	lctx := metadata.AppendToOutgoingContext(context.Background(), rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader)
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
	call := func(id uint32, path string, req proto.Message) {
		t.Helper()
		hb.Reset()
		for _, f := range []hpack.HeaderField{
			{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: path},
			{Name: ":authority", Value: "localhost"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
		} {
			must(enc.WriteField(f))
		}
		must(fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: hb.Bytes(), EndHeaders: true}))
		b, err := proto.Marshal(req)
		must(err)
		must(fr.WriteData(id, true, append([]byte{0, 0, 0, 0, byte(len(b))}, b...)))
	}
	call(1, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("raw"), Value: []byte("1")})
	must(fr.WriteRSTStream(1, http2.ErrCodeCancel))
	call(3, "/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("raw"), Value: []byte("2")})
	call(5, "/etcdserverpb.KV/RangeStream", &pb.RangeRequest{Key: []byte("raw")})

	var settings, pinged bool
	statuses := map[uint32]string{}
	for len(statuses) < 2 || !settings || !pinged {
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
	if statuses[3] != "0" || statuses[5] != "12" {
		t.Fatalf("grpc-status by stream: %v", statuses)
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
			if ga.ErrCode != http2.ErrCodeNo || ga.LastStreamID < 5 {
				t.Fatalf("GOAWAY %v", ga)
			}
			break
		}
	}
}
