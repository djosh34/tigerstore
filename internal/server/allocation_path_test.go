package server_test

// Throwaway allocation census. This is a correctness-sized test, not a
// throughput benchmark. The process counters include the client and all three
// nodes. The Actions experiment measures the leader process separately.
import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Allocation profiling and retained-history checks use production's 100ms
// clock. The general fixture's 10ms clock makes its disk fence only 200ms,
// which can mistake profiler or shared-runner scheduling delays for disk loss.
// Keep the real default election/watchdog policy and disk barriers unchanged.
func newAllocationCluster(t *testing.T) *cluster {
	return newClusterWith(t, func(c *cluster) { c.tick = 100 * time.Millisecond })
}

func TestAllocationPath(t *testing.T) {
	// Never run concurrently: MemStats and the allocation profiler are global.
	oldRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() { runtime.MemProfileRate = oldRate }()
	c := newAllocationCluster(t)
	cert, _, _, err := c.ca.Issue("allocation-client", 191)
	if err != nil {
		t.Fatal(err)
	}
	leader := retry(t, func(ctx context.Context) (uint64, error) {
		st, err := pb.NewMaintenanceClient(c.dial(1, cert)).Status(ctx, &pb.StatusRequest{})
		if err == nil && st.Leader == 0 {
			err = errors.New("no leader")
		}
		return st.GetLeader(), err
	})
	kv := c.client(leader, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("allocation-probe"), Value: []byte("ready")})
	})
	conn, err := tls.Dial("tcp", c.running[leader].addr, &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool,
		ServerName: "localhost", NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS13,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	cl, err := allocationClient(conn)
	if err != nil {
		t.Fatal(err)
	}
	for range 32 {
		if err := cl.put(); err != nil {
			t.Fatal(err)
		}
	}
	out := os.Getenv("ALLOC50_TEST_OUT")
	profile := func(name string) {
		if out == "" {
			return
		}
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(out, name+".pb.gz"))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Two GCs flush delayed memory-profile accounting. Profiles are cumulative;
	// subtract before from after with pprof -base, not by reading after alone.
	runtime.GC()
	runtime.GC()
	profile("before")
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const puts = 256
	for range puts {
		if err := cl.put(); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	runtime.GC()
	runtime.GC()
	profile("after")
	var retained runtime.MemStats
	runtime.ReadMemStats(&retained)
	result := map[string]any{
		"scope":    "in-process client plus leader and two followers, not leader-only",
		"strategy": os.Getenv("TIGER_ALLOC_STRATEGY"), "successful_puts": puts,
		"leader_id": leader, "value_bytes": 1024, "mem_profile_rate": 1,
		"mallocs_per_put":            float64(after.Mallocs-before.Mallocs) / puts,
		"allocated_bytes_per_put":    float64(after.TotalAlloc-before.TotalAlloc) / puts,
		"heap_alloc_after_forced_gc": retained.HeapAlloc,
	}
	// The index must retain immutable entry bytes after transient bodies and
	// replies have been recycled. Check every replica outside the counters.
	for id := uint64(1); id <= 3; id++ {
		got := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
			r, err := c.client(id, cert).Range(ctx, &pb.RangeRequest{Key: []byte("allocation-probe"), Serializable: true})
			if err == nil && uint64(r.Header.Revision) != cl.lastRev {
				err = fmt.Errorf("replica %d has revision %d, want %d", id, r.Header.Revision, cl.lastRev)
			}
			return r, err
		})
		if len(got.Kvs) != 1 || !bytes.Equal(got.Kvs[0].Value, bytes.Repeat([]byte{0xab}, 1024)) {
			t.Fatalf("replica %d lost the wire-backed value", id)
		}
	}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
	if out != "" {
		if err := os.WriteFile(filepath.Join(out, "memstats.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAllocationValueLifetimes catches reuse of a request body or stream slot
// while any historical value still references it. Each overwrite has distinct
// bytes; constant-value throughput workloads cannot catch that corruption.
func TestAllocationValueLifetimes(t *testing.T) {
	for _, mode := range []string{"base", "ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TIGER_ALLOC_STRATEGY", mode)
			c := newAllocationCluster(t)
			cert, _, _, err := c.ca.Issue("lifetime-client", 192)
			if err != nil {
				t.Fatal(err)
			}
			kv := c.client(1, cert)
			type version struct {
				key, value []byte
				rev        int64
			}
			var versions []version
			for i := range 96 {
				key := fmt.Appendf(nil, "/alloc50/lifetime/%d", i%8)
				value := bytes.Repeat([]byte{byte(i + 1)}, 1024+i%17)
				r := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
					return kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
				})
				versions = append(versions, version{key: key, value: value, rev: r.Header.Revision})
			}
			verify := func() {
				for id := uint64(1); id <= 3; id++ {
					client := c.client(id, cert)
					for _, v := range versions {
						r := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
							return client.Range(ctx, &pb.RangeRequest{Key: v.key, Revision: v.rev})
						})
						if len(r.Kvs) != 1 || r.Kvs[0].ModRevision != v.rev || !bytes.Equal(r.Kvs[0].Value, v.value) {
							t.Fatalf("node %d, key %q, revision %d: recycled historical bytes", id, v.key, v.rev)
						}
					}
				}
			}
			verify()
			// Reopen every WAL after slots have been reused. Live MVCC reads
			// alone cannot establish that the durable bytes stayed immutable.
			for _, r := range c.running {
				r.stop()
			}
			for id := uint64(1); id <= 3; id++ {
				c.start(id)
			}
			verify()
		})
	}
}

type allocClient struct {
	conn        *tls.Conn
	first, next []byte
	second      int
	id          uint32
	wantTrailer []byte
	head        [9]byte
	payload     [65536]byte
	reply       [256]byte
	lastRev     uint64
}

func allocationClient(conn *tls.Conn) (*allocClient, error) {
	var pre bytes.Buffer
	pre.WriteString(http2.ClientPreface)
	fr := http2.NewFramer(&pre, nil)
	if err := fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 4 << 20}); err != nil {
		return nil, err
	}
	if err := fr.WriteWindowUpdate(0, (16<<20)-65535); err != nil {
		return nil, err
	}
	if _, err := conn.Write(pre.Bytes()); err != nil {
		return nil, err
	}
	req, err := proto.Marshal(&pb.PutRequest{Key: []byte("allocation-probe"), Value: bytes.Repeat([]byte{0xab}, 1024)})
	if err != nil {
		return nil, err
	}
	msg := make([]byte, 5, 5+len(req))
	binary.BigEndian.PutUint32(msg[1:], uint32(len(req)))
	msg = append(msg, req...)
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: "/etcdserverpb.KV/Put"}, {Name: ":authority", Value: "localhost"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
	makeRequest := func() ([]byte, int, error) {
		hb.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				return nil, 0, err
			}
		}
		var wire bytes.Buffer
		fr := http2.NewFramer(&wire, nil)
		if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true}); err != nil {
			return nil, 0, err
		}
		second := wire.Len()
		if err := fr.WriteData(1, true, msg); err != nil {
			return nil, 0, err
		}
		return wire.Bytes(), second, nil
	}
	first, _, err := makeRequest()
	if err != nil {
		return nil, err
	}
	next, second, err := makeRequest()
	return &allocClient{conn: conn, first: first, next: next, second: second}, err
}

func (cl *allocClient) put() error {
	cl.id += 2
	id := cl.id - 1
	wire, second := cl.next, cl.second
	if id == 1 {
		wire = cl.first
		second = 9 + int(wire[0])<<16 + int(wire[1])<<8 + int(wire[2])
	}
	binary.BigEndian.PutUint32(wire[5:9], id)
	binary.BigEndian.PutUint32(wire[second+5:second+9], id)
	if _, err := cl.conn.Write(wire); err != nil {
		return err
	}
	replyN := 0
	for {
		if _, err := io.ReadFull(cl.conn, cl.head[:]); err != nil {
			return err
		}
		n := int(cl.head[0])<<16 | int(cl.head[1])<<8 | int(cl.head[2])
		if n > len(cl.payload) {
			return fmt.Errorf("frame too large: %d", n)
		}
		b := cl.payload[:n]
		if _, err := io.ReadFull(cl.conn, b); err != nil {
			return err
		}
		typ, flags := http2.FrameType(cl.head[3]), http2.Flags(cl.head[4])
		sid := binary.BigEndian.Uint32(cl.head[5:]) & 0x7fffffff
		if typ == http2.FrameSettings && flags&http2.FlagSettingsAck == 0 {
			ack := [9]byte{3: byte(http2.FrameSettings), 4: byte(http2.FlagSettingsAck)}
			if _, err := cl.conn.Write(ack[:]); err != nil {
				return err
			}
		}
		if sid != id {
			continue
		}
		if typ == http2.FrameRSTStream {
			return fmt.Errorf("reset on %d: %x", sid, b)
		}
		if typ == http2.FrameData {
			if replyN+n > len(cl.reply) {
				return errors.New("reply too large")
			}
			replyN += copy(cl.reply[replyN:], b)
		}
		if typ == http2.FrameHeaders && flags&http2.FlagHeadersEndStream != 0 {
			if cl.wantTrailer == nil {
				fields, err := hpack.NewDecoder(4096, nil).DecodeFull(b)
				if err != nil {
					return err
				}
				ok := false
				for _, f := range fields {
					if f.Name == "grpc-status" && f.Value == "0" {
						ok = true
					}
				}
				if !ok {
					return fmt.Errorf("non-OK trailer: %v", fields)
				}
				cl.wantTrailer = append([]byte(nil), b...)
			} else if !bytes.Equal(b, cl.wantTrailer) {
				return fmt.Errorf("changed trailer: %x", b)
			}
			return cl.validate(cl.reply[:replyN])
		}
	}
}

func (cl *allocClient) validate(msg []byte) error {
	if len(msg) < 5 || int(binary.BigEndian.Uint32(msg[1:5])) != len(msg)-5 {
		return errors.New("invalid gRPC response")
	}
	b := msg[5:]
	num, typ, n := protowire.ConsumeTag(b)
	if n < 0 || num != 1 || typ != protowire.BytesType {
		return errors.New("missing response header")
	}
	h, n := protowire.ConsumeBytes(b[n:])
	if n < 0 {
		return errors.New("bad response header")
	}
	var rev uint64
	for len(h) > 0 {
		num, typ, n = protowire.ConsumeTag(h)
		if n < 0 || typ != protowire.VarintType {
			return errors.New("bad header field")
		}
		h = h[n:]
		v, k := protowire.ConsumeVarint(h)
		if k < 0 {
			return errors.New("bad header value")
		}
		if num == 3 {
			rev = v
		}
		h = h[k:]
	}
	if rev == 0 || (cl.lastRev > 0 && rev != cl.lastRev+1) {
		return fmt.Errorf("revision %d after %d", rev, cl.lastRev)
	}
	cl.lastRev = rev
	return nil
}
