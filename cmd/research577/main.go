// Command research577 measures independent primitives, never a tigerstore handler.
// CPU is getrusage(RUSAGE_SELF), not wall time. TCP peers are separate processes
// pinned to CPU 3, excluded from CPU accounting. Run main pinned to CPU 0.
package main

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/internal/k8ssize"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2/hpack"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var duration = flag.Duration("duration", 300*time.Millisecond, "wall time per observation")
var reps = flag.Int("reps", 3, "independent observations")
var helper = flag.String("helper", "", "internal: TCP drainer or source")
var certDir = flag.String("certs", "certs", "development TLS certificates")
var sink uint64
var only = flag.String("only", "", "more: skip the transport sweep; transport: only it")

func emit(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		panic(err)
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func cpu() int64 {
	var r unix.Rusage
	must(unix.Getrusage(unix.RUSAGE_SELF, &r))
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (r.Utime.Usec+r.Stime.Usec)*1e3
}

type result struct {
	Kind, Name          string
	Bytes, N, Rep       int
	CPU_ns, Wall_ns     float64
	Alloc_bytes, Allocs float64
}

func measure(name string, size int, f func()) {
	for rep := 0; rep < *reps; rep++ {
		runtime.GC()
		var a, z runtime.MemStats
		runtime.ReadMemStats(&a)
		begin := time.Now()
		start := cpu()
		n := 0
		for time.Since(begin) < *duration {
			for range 32 {
				f()
				n++
			}
		}
		used := cpu() - start
		wall := time.Since(begin)
		runtime.ReadMemStats(&z)
		emit(result{"primitive", name, size, n, rep, float64(used) / float64(n), float64(wall.Nanoseconds()) / float64(n), float64(z.TotalAlloc-a.TotalAlloc) / float64(n), float64(z.Mallocs-a.Mallocs) / float64(n)})
	}
}
func vfield(b []byte, n protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, n, 0), v)
}
func bfield(b []byte, n protowire.Number, v []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, n, 2), v)
}
func clustered(r string) bool {
	switch r {
	case "apiservices", "certificatesigningrequests", "clusterrolebindings", "clusterroles", "csinodes", "flowschemas", "minions", "namespaces", "priorityclasses", "prioritylevelconfigurations", "storageclasses":
		return true
	}
	return false
}
func key(r string, id int) []byte {
	sep := "/"
	if clustered(r) {
		sep = "-"
	}
	return []byte(fmt.Sprintf("/registry/%s/bench-000-0000%sobject-%012d", r, sep, id))
}
func header() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: 0x123456789abcdef0, MemberId: 0x23456789abcdef01, Revision: 2000000, RaftTerm: 2}
}
func kv(r string, size, id int) *mvccpb.KeyValue {
	x := &mvccpb.KeyValue{Key: key(r, id), Value: make([]byte, size), CreateRevision: 1000000, ModRevision: 2000000, Version: 5}
	if r == "events" {
		x.Lease = 0x3456789abcdef012
	}
	return x
}

type fixture struct {
	Name, Resource string
	Weight         float64
	Request, Reply []byte
	KVs            []*mvccpb.KeyValue
	WatchKVs       []*mvccpb.KeyValue
	Write, Event   int
	WatchNode      int
}
type resource struct {
	Name               string
	Count, ValueBytes  int
	MeanKey, MeanValue float64
	WatchNode          int
}

func marshal(m proto.Message) []byte { b, e := proto.Marshal(m); must(e); return b }
func fixtures() ([]fixture, []resource) {
	rs := k8ssize.Resources()
	resources := make([]resource, len(rs))
	index := map[string]int{}
	for i, r := range rs {
		index[r] = i
		resources[i] = resource{Name: r, WatchNode: i % 3}
	}
	total := 0
	for _, b := range k8ssize.Buckets {
		r := &resources[index[b.Resource]]
		r.Count += b.Count
		r.ValueBytes += b.Count * b.Size
		total += b.Count
	}
	for i := range resources {
		r := &resources[i]
		r.MeanValue = float64(r.ValueBytes) / float64(r.Count)
		r.MeanKey = float64(len(key(r.Name, 0)))
	}
	var fs []fixture
	for _, b := range k8ssize.Buckets {
		if b.Count == 0 {
			continue
		}
		weight := float64(b.Count) / float64(total)
		k := kv(b.Resource, b.Size, 0)
		old := proto.Clone(k).(*mvccpb.KeyValue)
		for op, p := range []float64{.18, .10, 0, .50, .12, .08} {
			if p == 0 {
				continue
			}
			var req, reply proto.Message
			name := ""
			write, event := 0, 0
			kvs := []*mvccpb.KeyValue(nil)
			watchKVs := []*mvccpb.KeyValue(nil)
			switch op {
			case 0:
				name = "get"
				req = &pb.RangeRequest{Key: k.Key, Limit: 1}
				reply = &pb.RangeResponse{Header: header(), Kvs: []*mvccpb.KeyValue{k}, Count: 1}
				kvs = []*mvccpb.KeyValue{k}
			case 1:
				name = "rev"
				req = &pb.RangeRequest{Key: []byte("/registry/" + b.Resource), Limit: 1}
				reply = &pb.RangeResponse{Header: header()}
			default:
				name = map[int]string{3: "update", 4: "create", 5: "delete"}[op]
				rev := int64(2000000)
				if op == 4 {
					rev = 0
				}
				cmp := &pb.Compare{Target: pb.Compare_MOD, Key: k.Key, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}
				tx := &pb.TxnRequest{Compare: []*pb.Compare{cmp}}
				tr := &pb.TxnResponse{Header: header(), Succeeded: true}
				var ev *mvccpb.Event
				if op == 5 {
					tx.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: k.Key}}}}
					tr.Responses = []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Header: header(), Deleted: 1}}}}
					tomb := &mvccpb.KeyValue{Key: k.Key, ModRevision: 2000000}
					ev = &mvccpb.Event{Type: mvccpb.Event_DELETE, Kv: tomb, PrevKv: old}
				} else {
					put := &pb.PutRequest{Key: k.Key, Value: k.Value, Lease: k.Lease}
					tx.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: put}}}
					tr.Responses = []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: header()}}}}
					ev = &mvccpb.Event{Kv: k}
					if op != 4 {
						ev.PrevKv = old
					}
				}
				if op != 4 {
					tx.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: k.Key}}}}
				}
				req, reply = tx, tr
				write = 2*len(k.Key) + 8
				if op != 5 {
					write += b.Size
					if k.Lease != 0 {
						write += 8
					}
				}
				event = len(marshal(&pb.WatchResponse{Header: header(), WatchId: 1, Events: []*mvccpb.Event{ev}}))
				watchKVs = append(watchKVs, ev.Kv)
				if ev.PrevKv != nil {
					watchKVs = append(watchKVs, ev.PrevKv)
				}
			}
			fs = append(fs, fixture{Name: name, Resource: b.Resource, Weight: p * weight, Request: marshal(req), Reply: marshal(reply), KVs: kvs, WatchKVs: watchKVs, Write: write, Event: event, WatchNode: index[b.Resource] % 3})
		}
	}
	// Rev chooses resources uniformly, not by live object count. Fix weights.
	for i := range fs {
		if fs[i].Name == "rev" {
			r := resources[index[fs[i].Resource]]
			bucketWeight := fs[i].Weight / .10
			fs[i].Weight = .10 / float64(len(rs)) * bucketWeight / (float64(r.Count) / float64(total))
		}
	}
	// LIST shape at preload: model occupancy by resource probability, 1020 groups.
	// These fixtures are an explicit model, NOT measured pages or a new workload.
	rng := rand.New(rand.NewPCG(577, 1))
	for _, r := range resources {
		n := min(500, int(float64(1000000*r.Count)/float64(total*1020)+.5))
		var kvs []*mvccpb.KeyValue
		for j := 0; j < n; j++ {
			b := k8ssize.Buckets[k8ssize.Draw(rng, r.Name)]
			kvs = append(kvs, kv(r.Name, b.Size, j))
		}
		prefix := strings.Split(string(key(r.Name, 0)), "object-")[0]
		end := []byte(prefix)
		end[len(end)-1]++
		req := &pb.RangeRequest{Key: []byte(prefix), RangeEnd: end, Limit: 500}
		rr := &pb.RangeResponse{Header: header(), Kvs: kvs, Count: int64(n), More: n == 500}
		fs = append(fs, fixture{Name: "list", Resource: r.Name, Weight: .02 * float64(r.Count) / float64(total), Request: marshal(req), Reply: marshal(&pb.RangeStreamResponse{RangeResponse: rr}), KVs: kvs, WatchNode: r.WatchNode})
	}
	emit(map[string]any{"kind": "resources", "resources": resources, "objects": total})
	return fs, resources
}
func reportCounts(fs []fixture) {
	type count struct{ Weight, Request, Reply, Write, Event, Keys float64 }
	sums := map[string]*count{}
	nodeEvents := [3]float64{}
	nodeEventCounts := [3]float64{}
	for _, f := range fs {
		s := sums[f.Name]
		if s == nil {
			s = &count{}
			sums[f.Name] = s
		}
		s.Weight += f.Weight
		s.Request += f.Weight * float64(len(f.Request))
		s.Reply += f.Weight * float64(len(f.Reply))
		s.Write += f.Weight * float64(f.Write)
		s.Event += f.Weight * float64(f.Event)
		s.Keys += f.Weight * float64(len(f.KVs))
		nodeEvents[f.WatchNode] += f.Weight * float64(f.Event)
		if f.Event != 0 {
			nodeEventCounts[f.WatchNode] += f.Weight
		}
	}
	emit(map[string]any{"kind": "counts", "by_operation": sums, "watch_bytes_by_node": nodeEvents, "watch_events_by_node": nodeEventCounts, "list_assumption": "1M fixed live keys; mean occupancy by resource/1020 capped at 500, random values conditional on resource, NOT measured steady-state pages"})
}

// parse fields by aliasing byte payloads; recursively parse request submessages,
// not key/value bytes. This measures a no-copy parser, not protobuf's Go objects.
func parse(b []byte, depth int) uint64 {
	var sum uint64
	for len(b) > 0 {
		n, t, k := protowire.ConsumeTag(b)
		if k < 0 {
			panic("bad tag")
		}
		b = b[k:]
		switch t {
		case 0:
			v, k := protowire.ConsumeVarint(b)
			if k < 0 {
				panic("varint")
			}
			sum += v
			b = b[k:]
		case 2:
			v, k := protowire.ConsumeBytes(b)
			if k < 0 {
				panic("bytes")
			}
			sum += uint64(len(v))
			if depth == 2 && n == 1 {
				sum += parse(v, 0)
			} else if depth == 2 && (n == 2 || n == 3) {
				sum += parse(v, 1)
			} else if depth == 1 && (n == 1 || n == 2 || n == 3) {
				sum += parse(v, 0)
			}
			b = b[k:]
		default:
			k := protowire.ConsumeFieldValue(n, t, b)
			if k < 0 {
				panic("field")
			}
			b = b[k:]
		}
	}
	return sum
}
func encodeKV(b []byte, k *mvccpb.KeyValue) []byte {
	b = bfield(b, 1, k.Key)
	if k.CreateRevision != 0 {
		b = vfield(b, 2, uint64(k.CreateRevision))
	}
	if k.ModRevision != 0 {
		b = vfield(b, 3, uint64(k.ModRevision))
	}
	if k.Version != 0 {
		b = vfield(b, 4, uint64(k.Version))
	}
	if len(k.Value) > 0 {
		b = bfield(b, 5, k.Value)
	}
	if k.Lease != 0 {
		b = vfield(b, 6, uint64(k.Lease))
	}
	return b
}
func primitives(fs []fixture) {
	measure("empty", 0, func() { sink++ })
	block, _ := aes.NewCipher(make([]byte, 16))
	gcm, e := cipher.NewGCM(block)
	must(e)
	for _, n := range []int{64, 256, 1024, 4096, 16384} {
		plain := make([]byte, n+1)
		plain[n] = 23
		out := make([]byte, 0, n+32)
		nonce := make([]byte, 12)
		ad := []byte{23, 3, 3, byte((n + 17) >> 8), byte(n + 17)}
		seq := uint64(0)
		measure("tls13_record_seal", n, func() {
			seq++
			binary.BigEndian.PutUint64(nonce[4:], seq)
			out = gcm.Seal(out[:0], nonce, plain, ad)
			sink += uint64(out[0])
		})
		sealed := gcm.Seal(nil, nonce, plain, ad)
		dst := make([]byte, 0, len(plain))
		measure("tls13_record_open", n, func() {
			var e error
			dst, e = gcm.Open(dst[:0], nonce, sealed, ad)
			must(e)
			if dst[len(dst)-1] != 23 {
				panic("inner type")
			}
			sink += uint64(dst[0])
		})
		src := make([]byte, n)
		copyDst := make([]byte, n)
		measure("copy", n, func() { copy(copyDst, src); sink += uint64(copyDst[n-1]) })
	}
	// HPACK established dynamic table: real encoder/decoder. Initial literals are
	// outside measurements; never charge an amortizable setup to every call.
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: "/etcdserverpb.KV/Txn"}, {Name: ":authority", Value: "127.0.0.1:2379"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
	dec := hpack.NewDecoder(4096, func(f hpack.HeaderField) { sink += uint64(len(f.Value)) })
	for _, f := range fields {
		must(enc.WriteField(f))
	}
	_, e = dec.Write(hb.Bytes())
	must(e)
	must(dec.Close())
	hb.Reset()
	for _, f := range fields {
		must(enc.WriteField(f))
	}
	indexed := bytes.Clone(hb.Bytes())
	emit(map[string]any{"kind": "hpack", "indexed_request_bytes": len(indexed)})
	measure("hpack_decode_indexed", len(indexed), func() { _, e := dec.Write(indexed); must(e); must(dec.Close()) })
	measure("hpack_encode_indexed", len(indexed), func() {
		hb.Reset()
		for _, f := range fields {
			must(enc.WriteField(f))
		}
		sink += uint64(hb.Len())
	})
	// Independent HTTP2/gRPC header scan and assembly; payload alias, no handler.
	for _, n := range []int{64, 1024, 16384} {
		b := make([]byte, 14+n)
		b[0] = byte((n + 5) >> 16)
		b[1] = byte((n + 5) >> 8)
		b[2] = byte(n + 5)
		b[3] = 0
		b[4] = 1
		binary.BigEndian.PutUint32(b[5:9], 1)
		binary.BigEndian.PutUint32(b[10:14], uint32(n))
		measure("h2_grpc_parse", n, func() {
			size := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
			if b[3] != 0 || b[9] != 0 || size != n+5 || binary.BigEndian.Uint32(b[10:14]) != uint32(n) {
				panic("frame")
			}
			sink += uint64(binary.BigEndian.Uint32(b[5:9]))
		})
		measure("h2_grpc_build", n, func() {
			b[0] = byte((n + 5) >> 16)
			b[1] = byte((n + 5) >> 8)
			b[2] = byte(n + 5)
			b[3] = 0
			b[4] = 1
			binary.BigEndian.PutUint32(b[5:9], uint32(sink)|1)
			b[9] = 0
			binary.BigEndian.PutUint32(b[10:14], uint32(n))
			sink += uint64(b[8])
		})
	}
	// Weighted corpus loops: one loop is one *global* mixed operation, not a
	// leader op. Counts are deterministic stratified draws from the specified mix.
	cdf := make([]float64, len(fs))
	total := 0.0
	for i, f := range fs {
		total += f.Weight
		cdf[i] = total
	}
	corpus := make([]*fixture, 10000)
	for i := range corpus {
		v := (float64(i) + .5) / float64(len(corpus))
		j := sort.SearchFloat64s(cdf, v)
		corpus[i] = &fs[min(j, len(fs)-1)]
	}
	rng := rand.New(rand.NewPCG(577, 2))
	rng.Shuffle(len(corpus), func(i, j int) { corpus[i], corpus[j] = corpus[j], corpus[i] })
	at := 0
	measure("protobuf_request_alias_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		depth := 0
		if f.Write != 0 {
			depth = 2
		}
		sink += parse(f.Request, depth)
	})
	measure("protobuf_request_alias_writes_global", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		if f.Write != 0 {
			sink += parse(f.Request, 2)
		}
	})
	buf := make([]byte, 0, 2<<20)
	measure("replication_payload_copy_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		if f.Write != 0 {
			buf = append(buf[:0], f.Request...)
			sink += uint64(len(buf))
		}
	})
	for node := 0; node < 3; node++ {
		measure(fmt.Sprintf("watch_kv_encode_node%d_global", node), 0, func() {
			f := corpus[at%len(corpus)]
			at++
			if f.WatchNode == node {
				buf = buf[:0]
				for _, k := range f.WatchKVs {
					buf = encodeKV(buf, k)
				}
				sink += uint64(len(buf))
			}
		})
	}
	measure("protobuf_reply_copy_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		if len(f.KVs) == 0 {
			buf = append(buf[:0], f.Reply...)
		} else {
			buf = buf[:0]
			for _, k := range f.KVs {
				buf = encodeKV(buf, k)
			}
		}
		sink += uint64(len(buf))
	})
	// Per-KeyValue cost separated from bytes; no object allocation.
	k := kv("events", 0, 0)
	measure("protobuf_kv_metadata", len(k.Key), func() { buf = encodeKV(buf[:0], k); sink += uint64(len(buf)) })
	// Minimal replication ordered append/quorum accounting, not the Raft stack.
	// Two peer acks, own durable index and 3-way majority. All actual durability,
	// network, membership/election/retry paths remain separate.
	var a, b, committed uint64
	own := uint64(4)
	ack := make([]byte, 24)
	measure("peer_ack_encode_parse", 24, func() {
		binary.LittleEndian.PutUint64(ack, sink)
		binary.LittleEndian.PutUint64(ack[8:], sink+1)
		binary.LittleEndian.PutUint64(ack[16:], 2)
		sink += binary.LittleEndian.Uint64(ack[8:])
	})
	measure("quorum_three_indexes", 0, func() {
		own++
		a = own - (sink & 3)
		b = own - ((sink >> 2) & 3)
		hi := min(own, max(a, b))
		if hi > committed {
			committed = hi
		}
		sink += committed
	})
	// Primitive async queue/sync overhead, not unavoidable if a single loop owns it.
	q := make(chan uint64, 64)
	measure("channel_queue_uncontended", 0, func() { q <- sink; sink = <-q })
	// Preallocated 1M-entry hash map + contiguous ordered payload descriptors.
	// It is deliberately independent of tigerstore's MVCC/index implementation.
	keys := make([]string, 1000000)
	m := make(map[string]uint64, len(keys))
	for i := range keys {
		keys[i] = string(key("events", i))
		m[keys[i]] = uint64(i)
	}
	pos := uint64(577)
	measure("hash_lookup_1M", 0, func() { pos ^= pos << 13; pos ^= pos >> 7; pos ^= pos << 17; sink += m[keys[pos%uint64(len(keys))]] })
	measure("hash_update_1M", 0, func() { pos ^= pos << 13; pos ^= pos >> 7; pos ^= pos << 17; k := keys[pos%uint64(len(keys))]; m[k]++ })
}

func tlsConfig(server bool) *tls.Config {
	if server {
		c, e := tls.LoadX509KeyPair(*certDir+"/server.pem", *certDir+"/server-key.pem")
		if e != nil {
			c, e = tls.LoadX509KeyPair(*certDir+"/client.pem", *certDir+"/client-key.pem")
		}
		must(e)
		return &tls.Config{Certificates: []tls.Certificate{c}, MinVersion: tls.VersionTLS13}
	}
	return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13} // established data only; handshake is excluded
}
func helperMain() {
	parts := strings.Split(*helper, ",")
	mode := parts[0]
	useTLS := parts[1] == "tls"
	size, e := strconv.Atoi(parts[2])
	must(e)
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	fmt.Println(ln.Addr().String())
	c, e := ln.Accept()
	must(e)
	if useTLS {
		t := tls.Server(c, tlsConfig(true))
		must(t.Handshake())
		c = t
	}
	buf := make([]byte, size)
	if mode == "send" {
		for {
			if _, e = c.Write(buf); e != nil {
				return
			}
		}
	} else {
		_, e = io.CopyBuffer(io.Discard, c, buf)
	}
}
func transport() {
	exe, e := os.Executable()
	must(e)
	for _, protocol := range []string{"tcp", "tls"} {
		for _, mode := range []string{"send", "receive"} {
			for _, size := range []int{1024, 4096, 16384, 65536, 262144} {
				other := "drain"
				if mode == "receive" {
					other = "send"
				}
				cmd := exec.Command("taskset", "-c", "3", exe, "-helper", fmt.Sprintf("%s,%s,%d", other, protocol, size), "-certs", *certDir)
				cmd.Stderr = os.Stderr
				stdout, e := cmd.StdoutPipe()
				must(e)
				must(cmd.Start())
				line, e := bufio.NewReader(stdout).ReadString('\n')
				must(e)
				c, e := net.Dial("tcp", strings.TrimSpace(line))
				must(e)
				if protocol == "tls" {
					t := tls.Client(c, tlsConfig(false))
					must(t.Handshake())
					c = t
				}
				data := make([]byte, size)
				measure(protocol+"_"+mode, size, func() {
					if mode == "send" {
						n, e := c.Write(data)
						must(e)
						if n != size {
							panic("short write")
						}
					} else {
						_, e := io.ReadFull(c, data)
						must(e)
					}
				})
				must(c.Close())
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	}
}
func main() {
	flag.Parse()
	if *helper != "" {
		helperMain()
		return
	}
	runtime.GOMAXPROCS(2)
	emit(map[string]any{"kind": "runtime", "go": runtime.Version(), "arch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "duration": duration.String(), "cpu_clock": "RUSAGE_SELF user+system; separate TCP helper excluded"})
	fs, _ := fixtures()
	reportCounts(fs)
	if *only != "transport" {
		primitives(fs)
		more(fs)
	}
	if *only != "more" {
		transport()
	}
	emit(map[string]any{"kind": "sink", "value": sink})
}
