package wl

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"golang.org/x/net/http2/hpack"
)

// A node whose client URLs are http serves its clients without TLS, as
// etcd's do (#718): gRPC over HTTP/2 with prior knowledge (h2c), puts, a
// Txn, a watch, a lease and a Lock, which the node runs through its own
// client, itself over h2c then; and HTTP/1.1, /version, /health and the
// gateway. A client whose preface comes in pieces is taken whole. The
// node tells each client --max-concurrent-streams and refuses a stream
// past it.
func TestANodeServesPlainHTTPClients(t *testing.T) {
	const maxStreams = 8
	var plain []string
	newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			plain = append(plain, ln.Addr().String())
			for _, l := range c.ClientListeners {
				_ = l.Close()
			}
			c.ClientListeners, c.ClientTLS = nil, nil
			c.PlainClientListeners = []net.Listener{ln}
			c.ClientURLs = []string{"http://" + ln.Addr().String()}
			c.MaxStreams = maxStreams
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := clientv3.New(clientv3.Config{Endpoints: plain, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	wch := c.Watch(ctx, "/p/", clientv3.WithPrefix())
	for {
		if _, err := c.Put(ctx, "/p/k", "v"); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("no put was taken over h2c")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r, err := c.Get(ctx, "/p/k"); err != nil || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "v" {
		t.Fatalf("a get over h2c: %v %v", r, err)
	}
	if r, err := c.Txn(ctx).If(clientv3.Compare(clientv3.Value("/p/k"), "=", "v")).Then(clientv3.OpPut("/p/t", "t")).Commit(); err != nil || !r.Succeeded {
		t.Fatalf("a Txn over h2c: %v %v", r, err)
	}
	if ev := <-wch; ev.Err() != nil || len(ev.Events) == 0 || string(ev.Events[0].Kv.Key) != "/p/k" {
		t.Fatalf("a watch over h2c: %v %v", ev.Events, ev.Err())
	}
	lease, err := c.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.KeepAliveOnce(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	locks := v3lockpb.NewLockClient(c.ActiveConnection())
	lk, err := locks.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("/p/lock"), Lease: int64(lease.ID)})
	if err != nil || !bytes.HasPrefix(lk.Key, []byte("/p/lock/")) {
		t.Fatalf("a Lock, through the node's own client over h2c: %v %v", lk, err)
	}
	if _, err := locks.Unlock(ctx, &v3lockpb.UnlockRequest{Key: lk.Key}); err != nil {
		t.Fatal(err)
	}

	get := func(method, path, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, "http://"+plain[0]+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	if code, body := get(http.MethodGet, "/version", ""); code != http.StatusOK || !strings.Contains(body, "etcdserver") {
		t.Errorf("/version over HTTP/1.1: %d %q", code, body)
	}
	if code, body := get(http.MethodGet, "/health", ""); code != http.StatusOK || !strings.Contains(body, `"health":"true"`) {
		t.Errorf("/health over HTTP/1.1: %d %q", code, body)
	}
	key := base64.StdEncoding.EncodeToString([]byte("/p/k"))
	if code, body := get(http.MethodPost, "/v3/kv/range", `{"key":"`+key+`"}`); code != http.StatusOK || !strings.Contains(body, base64.StdEncoding.EncodeToString([]byte("v"))) {
		t.Errorf("the gateway's range over HTTP/1.1: %d %q", code, body)
	}

	// The preface in two pieces, then an empty SETTINGS frame: the node
	// answers with its own SETTINGS, which hold the limit on streams.
	nc, err := net.DialTimeout("tcp", plain[0], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	if _, err := nc.Write([]byte(h2Preface[:10])); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := nc.Write(append([]byte(h2Preface[10:]), 0, 0, 0, frSettings, 0, 0, 0, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := nc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame := func() (typ byte, sid uint32, p []byte) {
		t.Helper()
		hdr := make([]byte, 9)
		if _, err := io.ReadFull(nc, hdr); err != nil {
			t.Fatal(err)
		}
		p = make([]byte, int(hdr[0])<<16|int(hdr[1])<<8|int(hdr[2]))
		if _, err := io.ReadFull(nc, p); err != nil {
			t.Fatal(err)
		}
		return hdr[3], binary.BigEndian.Uint32(hdr[5:]) & (1<<31 - 1), p
	}
	typ, _, p := frame()
	if typ != frSettings {
		t.Fatalf("the node's first frame after a preface in two pieces is of type %d", typ)
	}
	told := uint32(0)
	for ; len(p) >= 6; p = p[6:] {
		if binary.BigEndian.Uint16(p) == 0x3 {
			told = binary.BigEndian.Uint32(p[2:])
		}
	}
	if told != maxStreams {
		t.Fatalf("the node told its client a limit of %d streams, want %d", told, maxStreams)
	}

	// Watch streams stay open: the one past the limit is refused, and the
	// others are not.
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	var out []byte
	for i := range maxStreams + 1 {
		// Later blocks name the same fields from the table.
		block.Reset()
		for _, f := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":path", "/etcdserverpb.Watch/Watch"}, {":authority", plain[0]}, {"content-type", "application/grpc"}, {"te", "trailers"}} {
			if err := enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}); err != nil {
				t.Fatal(err)
			}
		}
		h := make([]byte, 9)
		putFrameHeader(h, block.Len(), frHeaders, flEndHeaders, uint32(2*i+1))
		out = append(append(out, h...), block.Bytes()...)
	}
	if _, err := nc.Write(out); err != nil {
		t.Fatal(err)
	}
	for {
		typ, sid, p := frame()
		if typ != frRST {
			continue
		}
		if sid != 2*maxStreams+1 || len(p) != 4 || binary.BigEndian.Uint32(p) != codeRefusedStream {
			t.Fatalf("RST_STREAM %x on stream %d; want only REFUSED_STREAM on stream %d", p, sid, 2*maxStreams+1)
		}
		break
	}
	checkProtocolErrors(t, plain[0])
}

// checkProtocolErrors sends a client connection at addr, over h2c, each
// way of breaking HTTP/2 that grpc-go's server refuses: the node answers
// with a GOAWAY of the right code and closes the connection.
func checkProtocolErrors(t *testing.T, addr string) {
	t.Helper()
	frame := func(typ, flags byte, sid uint32, p []byte) []byte {
		h := make([]byte, 9, 9+len(p))
		putFrameHeader(h, len(p), typ, flags, sid)
		return append(h, p...)
	}
	u32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	// block is a request's header block of fields, by a fresh encoder: each
	// case has its own connection.
	block := func(fields ...string) []byte {
		var b bytes.Buffer
		enc := hpack.NewEncoder(&b)
		for i := 0; i < len(fields); i += 2 {
			if err := enc.WriteField(hpack.HeaderField{Name: fields[i], Value: fields[i+1]}); err != nil {
				t.Fatal(err)
			}
		}
		return b.Bytes()
	}
	request := []string{":method", "POST", ":scheme", "http", ":path", "/etcdserverpb.Watch/Watch", ":authority", addr, "content-type", "application/grpc", "te", "trailers"}
	with := func(extra ...string) []byte { return block(append(slices.Clone(request), extra...)...) }
	watch := frame(frHeaders, flEndHeaders, 1, with())
	for _, tc := range []struct {
		name string
		in   []byte
		code h2Error
	}{
		{"DATA on stream 0", frame(frData, 0, 0, []byte{0}), errProtocol},
		{"a frame past the largest frame size", frame(frData, 0, 1, make([]byte, maxFrame+1)), errFrameSize},
		{"HEADERS on stream 0", frame(frHeaders, flEndHeaders, 0, with()), errProtocol},
		{"a stream of the server's", frame(frHeaders, flEndHeaders, 2, with()), errProtocol},
		{"a stream below the last", append(frame(frHeaders, flEndHeaders, 3, with()), frame(frHeaders, flEndHeaders, 1, block(request...))...), errProtocol},
		{"DATA on an idle stream", frame(frData, 0, 5, []byte{0}), errProtocol},
		{"DATA after the client ended its side", append(frame(frHeaders, flEndHeaders|flEndStream, 1, with()), frame(frData, 0, 1, []byte{0})...), errProtocol},
		{"RST_STREAM on an idle stream", frame(frRST, 0, 5, u32(0)), errProtocol},
		{"RST_STREAM of 3 bytes", append(watch, frame(frRST, 0, 1, []byte{0, 0, 0})...), errProtocol},
		{"PRIORITY of 4 bytes", frame(frPriority, 0, 1, u32(0)), errProtocol},
		{"PRIORITY on itself", frame(frPriority, 0, 1, append(u32(1), 16)), errProtocol},
		{"HEADERS that depend on themselves", frame(frHeaders, flEndHeaders|flPriority, 1, append(append(u32(1), 16), with()...)), errProtocol},
		{"SETTINGS on a stream", frame(frSettings, 0, 1, nil), errProtocol},
		{"a SETTINGS ACK with a payload", frame(frSettings, flAck, 0, make([]byte, 6)), errProtocol},
		{"SETTINGS_ENABLE_PUSH 2", frame(frSettings, 0, 0, append([]byte{0, 2}, u32(2)...)), errProtocol},
		{"SETTINGS_MAX_FRAME_SIZE below its initial value", frame(frSettings, 0, 0, append([]byte{0, 5}, u32(maxFrame-1)...)), errProtocol},
		{"PING of 7 bytes", frame(frPing, 0, 0, make([]byte, 7)), errProtocol},
		{"PING on a stream", frame(frPing, 0, 1, make([]byte, 8)), errProtocol},
		{"PUSH_PROMISE", frame(frPushPromise, flEndHeaders, 1, u32(2)), errProtocol},
		{"WINDOW_UPDATE of 0", frame(frWindowUpdate, 0, 0, u32(0)), errProtocol},
		{"WINDOW_UPDATE on an idle stream", frame(frWindowUpdate, 0, 5, u32(1)), errProtocol},
		{"the connection's window past 2^31-1", frame(frWindowUpdate, 0, 0, u32(1<<31-1)), errFlowControl},
		{"a stream's window past 2^31-1", append(watch, frame(frWindowUpdate, 0, 1, u32(1<<31-1))...), errFlowControl},
		{"a request without :path", frame(frHeaders, flEndHeaders, 1, block(":method", "POST", ":scheme", "http", ":authority", addr)), errProtocol},
		{"an empty :path", frame(frHeaders, flEndHeaders, 1, block(":method", "POST", ":scheme", "http", ":path", "")), errProtocol},
		{"a :path twice", frame(frHeaders, flEndHeaders, 1, with(":path", "/x")), errProtocol},
		{"an unknown pseudo-header", frame(frHeaders, flEndHeaders, 1, block(append([]string{":who", "x"}, request...)...)), errProtocol},
		{"a response's pseudo-header", frame(frHeaders, flEndHeaders, 1, block(append([]string{":status", "200"}, request...)...)), errProtocol},
		{"a pseudo-header after a regular one", frame(frHeaders, flEndHeaders, 1, with(":authority", addr)), errProtocol},
		{"a name in upper case", frame(frHeaders, flEndHeaders, 1, with("X-Up", "1")), errProtocol},
		{"a field of the connection's", frame(frHeaders, flEndHeaders, 1, with("connection", "keep-alive")), errProtocol},
		{"te but trailers", frame(frHeaders, flEndHeaders, 1, block(":method", "POST", ":scheme", "http", ":path", "/etcdserverpb.Watch/Watch", "te", "gzip")), errProtocol},
		{"trailers with a pseudo-header", append(watch, frame(frHeaders, flEndHeaders|flEndStream, 1, block(":path", "/x"))...), errProtocol},
		{"trailers that do not end the stream", append(watch, frame(frHeaders, flEndHeaders, 1, block("x", "y"))...), errProtocol},
	} {
		nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		in := append([]byte(h2Preface), frame(frSettings, 0, 0, nil)...)
		if _, err := nc.Write(append(in, tc.in...)); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got, err := io.ReadAll(nc)
		_ = nc.Close()
		if err != nil {
			t.Errorf("%s: the node did not close the connection: %v", tc.name, err)
			continue
		}
		code := h2Error(0)
		for len(got) >= 9 {
			l := int(got[0])<<16 | int(got[1])<<8 | int(got[2])
			if len(got) < 9+l {
				break
			}
			if got[3] == frGoAway && l >= 8 {
				code = h2Error(binary.BigEndian.Uint32(got[9+4:]))
			}
			got = got[9+l:]
		}
		if code != tc.code {
			t.Errorf("%s: GOAWAY with code %d, want %d", tc.name, code, tc.code)
		}
	}
}
