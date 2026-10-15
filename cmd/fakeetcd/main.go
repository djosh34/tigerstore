// Command fakeetcd is a temporary in-memory etcd v3 KV and Watch server for
// issue #188. It answers only what cmd/loadgen's mixed mode sends (Range,
// Txn with one compare, Put, DeleteRange, prefix watches with prev_kv), over
// TLS 1.3 and raw HTTP/2, and does little work per request, so that the load
// generator, not the server, is the limit. Not for merge.
//
//	fakeetcd -listen 127.0.0.1:42000 -cert node1.pem -key node1-key.pem -cacert ca.pem
package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/btree"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protowire"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:42000", "comma-free host:port to serve")
	cert := flag.String("cert", "", "server certificate")
	key := flag.String("key", "", "server key")
	cacert := flag.String("cacert", "", "CA of client certificates")
	profile := flag.String("cpuprofile", "", "write a CPU profile here from -profile-after for -profile-for")
	after := flag.Duration("profile-after", 5*time.Second, "when the profile starts")
	length := flag.Duration("profile-for", 5*time.Second, "how long the profile runs")
	flag.Parse()
	if *profile != "" {
		go func() {
			time.Sleep(*after)
			f, err := os.Create(*profile)
			if err != nil {
				log.Fatal(err)
			}
			_ = pprof.StartCPUProfile(f)
			runtime.SetMutexProfileFraction(10)
			time.Sleep(*length)
			pprof.StopCPUProfile()
			_ = f.Close()
			if m, err := os.Create(*profile + ".mutex"); err == nil {
				_ = pprof.Lookup("mutex").WriteTo(m, 0)
				_ = m.Close()
			}
		}()
	}
	kp, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		log.Fatal(err)
	}
	pemb, err := os.ReadFile(*cacert)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pemb)
	cfg := &tls.Config{Certificates: []tls.Certificate{kp}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	ln, err := tls.Listen("tcp", *listen, cfg)
	if err != nil {
		log.Fatal(err)
	}
	s := &store{}
	for i := range s.shards {
		s.shards[i].kvs = btree.NewG(32, func(a, b *item) bool { return bytes.Compare(a.key, b.key) < 0 })
	}
	for {
		nc, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := serve(s, nc); err != nil && !errors.Is(err, io.EOF) {
				log.Printf("conn: %v", err)
			}
		}()
	}
}

// Field numbers, from etcd v3.7.2 rpc.proto and kv.proto.
const (
	hdrRevision = 3

	rangeKey, rangeEnd, rangeLimit   = 1, 2, 3
	rrHeader, rrKVs, rrMore, rrCount = 1, 2, 3, 4

	txnCompare, txnSuccess, txnFailure   = 1, 2, 3
	opRange, respRange                   = 1, 1
	cmpTarget, cmpKey, cmpCreate, cmpMod = 2, 3, 5, 6
	targetCreate, targetMod              = 1, 2
	opPut, opDelete                      = 2, 3
	putKey, putValue                     = 1, 2
	trHeader, trSucceeded, trResps       = 1, 2, 3
	respPut, respDelete                  = 2, 3
	delDeleted                           = 2

	kvKey, kvCreate, kvMod, kvVersion, kvValue = 1, 2, 3, 4, 5

	wreqCreate                          = 1
	wcKey, wcEnd, wcStart, wcPrevKV     = 1, 2, 3, 6
	wrHeader, wrID, wrCreated, wrEvents = 1, 2, 3, 11
	evType, evKV, evPrevKV              = 1, 2, 3
)

type item struct {
	key                  []byte
	enc                  []byte // the encoded mvccpb.KeyValue
	create, mod, version int64
}

// store is sharded by LIST group, the resource plus the 14-byte
// "bench-OOO-NNNN" namespace or name prefix, so that one lock does not
// serialize the server. Every request of mixed mode stays within one
// group, except the preload's batches, which lock their shards in order.
// seq assigns revisions and queues watch events in revision order; it is
// held only to encode and queue.
type store struct {
	rev      atomic.Int64
	shards   [256]shard
	seq      sync.Mutex
	watchers []*watcher
}

type shard struct {
	mu  sync.RWMutex
	kvs *btree.BTreeG[*item]
	mem arena
}

func (s *store) shard(key []byte) int {
	rest, ok := bytes.CutPrefix(key, []byte("/registry/"))
	if !ok {
		return 0
	}
	n := len(key) - len(rest)
	if i := bytes.IndexByte(rest, '/'); i >= 0 {
		n += i + 1 + 14
	}
	h := fnv.New32a()
	h.Write(key[:min(n, len(key))])
	return int(h.Sum32() % uint32(len(s.shards)))
}

type watcher struct {
	key, end []byte
	mu       sync.Mutex
	cond     *sync.Cond
	events   []wevent
	last     int64
}

// wevent references the encoded KVs of one event; the sender builds the
// event bytes outside seq.
type wevent struct {
	del      bool
	kv, prev []byte
}

func appendEvent(m []byte, e wevent) []byte {
	n := 0
	if e.del {
		n += 2
	}
	n += 1 + protowire.SizeBytes(len(e.kv))
	if e.prev != nil {
		n += 1 + protowire.SizeBytes(len(e.prev))
	}
	m = protowire.AppendTag(m, wrEvents, protowire.BytesType)
	m = protowire.AppendVarint(m, uint64(n))
	if e.del {
		m = varint(m, evType, 1)
	}
	m = bytesF(m, evKV, e.kv)
	if e.prev != nil {
		m = bytesF(m, evPrevKV, e.prev)
	}
	return m
}

// arena holds stored keys and KVs outside the Go heap, so the collector
// does not scan or copy a million values. Nothing is freed: a short run's
// overwrites fit. Callers hold the shard's lock.
type arena struct{ chunk []byte }

func (a *arena) alloc(n int) []byte {
	if n > len(a.chunk) {
		size := max(n, 64<<20)
		b, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			log.Fatal(err)
		}
		a.chunk = b
	}
	b := a.chunk[:0:n]
	a.chunk = a.chunk[n:]
	return b
}

func encKV(mem *arena, key, value []byte, create, mod, version int64) []byte {
	b := mem.alloc(len(key) + len(value) + 40)
	b = protowire.AppendTag(b, kvKey, protowire.BytesType)
	b = protowire.AppendBytes(b, key)
	if create != 0 {
		b = protowire.AppendTag(b, kvCreate, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(create))
	}
	b = protowire.AppendTag(b, kvMod, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(mod))
	if version != 0 {
		b = protowire.AppendTag(b, kvVersion, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(version))
	}
	if len(value) > 0 {
		b = protowire.AppendTag(b, kvValue, protowire.BytesType)
		b = protowire.AppendBytes(b, value)
	}
	return b
}

func header(b []byte, num protowire.Number, rev int64) []byte {
	h := protowire.AppendTag(nil, hdrRevision, protowire.VarintType)
	h = protowire.AppendVarint(h, uint64(rev))
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, h)
}

func varint(b []byte, num protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func bytesF(b []byte, num protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

// each calls f for every top-level field of m.
func each(m []byte, f func(num protowire.Number, v uint64, b []byte)) error {
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return errors.New("bad proto")
		}
		m = m[n:]
		var v uint64
		var b []byte
		switch typ {
		case protowire.VarintType:
			v, n = protowire.ConsumeVarint(m)
		case protowire.BytesType:
			b, n = protowire.ConsumeBytes(m)
		default:
			n = protowire.ConsumeFieldValue(num, typ, m)
		}
		if n < 0 {
			return errors.New("bad proto")
		}
		m = m[n:]
		f(num, v, b)
	}
	return nil
}

func (s *store) rangeReq(req, out []byte, hits []*item) ([]byte, []*item, error) {
	var key, end []byte
	var limit int
	if err := each(req, func(n protowire.Number, v uint64, b []byte) {
		switch n {
		case rangeKey:
			key = b
		case rangeEnd:
			end = b
		case rangeLimit:
			limit = int(v)
		}
	}); err != nil {
		return nil, hits, err
	}
	sh := &s.shards[s.shard(key)]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	// Size the answer first, so building it copies each KV once.
	hits = hits[:0]
	count, size := 0, 32
	if len(end) == 0 {
		if it, ok := sh.kvs.Get(&item{key: key}); ok {
			hits = append(hits, it)
			count, size = 1, size+len(it.enc)+4
		}
	} else {
		sh.kvs.AscendRange(&item{key: key}, &item{key: end}, func(it *item) bool {
			if limit == 0 || count < limit {
				hits = append(hits, it)
				size += len(it.enc) + 4
			}
			count++
			return true
		})
	}
	out = slices.Grow(out[:0], size)
	out = header(out, rrHeader, s.rev.Load())
	for _, it := range hits {
		out = bytesF(out, rrKVs, it.enc)
	}
	if limit > 0 && count > limit {
		out = varint(out, rrMore, 1)
	}
	if count > 0 {
		out = varint(out, rrCount, uint64(count))
	}
	return out, hits, nil
}

type mutation struct {
	del        bool
	key, value []byte
}

func (s *store) txnReq(req []byte) ([]byte, error) {
	var cmps [][]byte
	var muts []mutation
	var gets [][]byte // failure branch: single-key Ranges, as kube-apiserver's Else(OpGet(key))
	var perr error
	err := each(req, func(n protowire.Number, _ uint64, b []byte) {
		switch n {
		case txnCompare:
			cmps = append(cmps, b)
		case txnSuccess:
			err := each(b, func(n protowire.Number, _ uint64, b []byte) {
				var m mutation
				switch n {
				case opPut:
				case opDelete:
					m.del = true
				default:
					perr = fmt.Errorf("unsupported request op %d", n)
					return
				}
				_ = each(b, func(n protowire.Number, _ uint64, v []byte) {
					switch n {
					case putKey:
						m.key = v
					case putValue:
						if !m.del {
							m.value = v
						}
					}
				})
				muts = append(muts, m)
			})
			if err != nil {
				perr = err
			}
		case txnFailure:
			_ = each(b, func(n protowire.Number, _ uint64, b []byte) {
				if n != opRange {
					perr = fmt.Errorf("unsupported failure op %d", n)
					return
				}
				_ = each(b, func(n protowire.Number, _ uint64, v []byte) {
					if n == rangeKey {
						gets = append(gets, v)
					}
				})
			})
		default:
			perr = fmt.Errorf("unsupported txn field %d", n)
		}
	})
	if err == nil {
		err = perr
	}
	if err != nil {
		return nil, err
	}
	var locked []int
	for _, m := range muts {
		locked = append(locked, s.shard(m.key))
	}
	for _, c := range cmps {
		_ = each(c, func(n protowire.Number, _ uint64, b []byte) {
			if n == cmpKey {
				locked = append(locked, s.shard(b))
			}
		})
	}
	for _, g := range gets {
		locked = append(locked, s.shard(g))
	}
	slices.Sort(locked)
	locked = slices.Compact(locked)
	for _, i := range locked {
		s.shards[i].mu.Lock()
	}
	defer func() {
		for _, i := range locked {
			s.shards[i].mu.Unlock()
		}
	}()
	ok := true
	for _, c := range cmps {
		var target, want uint64
		var key []byte
		_ = each(c, func(n protowire.Number, v uint64, b []byte) {
			switch n {
			case cmpTarget:
				target = v
			case cmpKey:
				key = b
			case cmpCreate, cmpMod:
				want = v
			}
		})
		var have int64
		if it, found := s.shards[s.shard(key)].kvs.Get(&item{key: key}); found {
			if target == targetCreate {
				have = it.create
			} else {
				have = it.mod
			}
		}
		if (target != targetCreate && target != targetMod) || uint64(have) != want {
			ok = false
		}
	}
	if !ok {
		rev := s.rev.Load()
		out := header(nil, trHeader, rev)
		for _, g := range gets {
			rr := header(nil, rrHeader, rev)
			if it, found := s.shards[s.shard(g)].kvs.Get(&item{key: g}); found {
				rr = bytesF(rr, rrKVs, it.enc)
				rr = varint(rr, rrCount, 1)
			}
			out = bytesF(out, trResps, bytesF(nil, respRange, rr))
		}
		return out, nil
	}
	rev := s.rev.Load()
	// Look up and update the btrees outside seq: the shard locks already
	// keep every reader of these keys out until the Txn returns. seq only
	// orders the revision and the watch queues.
	type pending struct {
		sh   *shard
		old  *item
		had  bool
		key  []byte
		put  *item
	}
	pend := make([]pending, len(muts))
	for i, m := range muts {
		sh := &s.shards[s.shard(m.key)]
		old, had := sh.kvs.Get(&item{key: m.key})
		key := m.key
		if had {
			key = old.key
		} else {
			key = append(sh.mem.alloc(len(m.key)), m.key...)
		}
		pend[i] = pending{sh: sh, old: old, had: had, key: key}
	}
	var wake []*watcher
	if len(muts) > 0 {
		s.seq.Lock()
		rev = s.rev.Add(1)
	}
	out := header(nil, trHeader, rev)
	out = varint(out, trSucceeded, 1)
	for i, m := range muts {
		p := &pend[i]
		sh, old, had, key := p.sh, p.old, p.had, p.key
		var event wevent
		has := false
		var r []byte
		if m.del {
			deleted := 0
			if had {
				deleted = 1
				event = wevent{del: true, kv: encKV(&sh.mem, key, nil, 0, rev, 0), prev: old.enc}
				has = true
			}
			inner := header(nil, 1, rev)
			if deleted > 0 {
				inner = varint(inner, delDeleted, uint64(deleted))
			}
			r = bytesF(nil, respDelete, inner)
		} else {
			it := &item{key: key, mod: rev, create: rev, version: 1}
			if had {
				it.create, it.version = old.create, old.version+1
			}
			it.enc = encKV(&sh.mem, key, m.value, it.create, it.mod, it.version)
			p.put = it
			event, has = wevent{kv: it.enc}, true
			if had {
				event.prev = old.enc
			}
			r = bytesF(nil, respPut, header(nil, 1, rev))
		}
		out = bytesF(out, trResps, r)
		if has {
			for _, w := range s.watchers {
				if bytes.Compare(key, w.key) >= 0 && bytes.Compare(key, w.end) < 0 {
					w.mu.Lock()
					w.events = append(w.events, event)
					w.last = rev
					w.mu.Unlock()
					wake = append(wake, w)
				}
			}
		}
	}
	if len(muts) > 0 {
		s.seq.Unlock()
	}
	// Wake the senders outside seq: a wakeup can cost a futex call.
	for _, w := range wake {
		w.cond.Signal()
	}
	for _, p := range pend {
		if p.put != nil {
			p.sh.kvs.ReplaceOrInsert(p.put)
		} else if p.had {
			p.sh.kvs.Delete(p.old)
		}
	}
	return out, nil
}

// HTTP/2 side.

type hstream struct {
	path string
	buf  []byte
	sent bool // response headers sent (watch)
}

type hconn struct {
	nc      net.Conn
	fr      *http2.Framer
	wmu     sync.Mutex
	cond    *sync.Cond
	out     bytes.Buffer
	sendWin int64
	strWin  map[uint32]int64
	initWin int64
	dec     *hpack.Decoder
	path    string
	streams map[uint32]*hstream
	recv    int64
	scratch []byte
	hits    []*item
}

var respHeaders, okTrailers []byte

func init() {
	enc := func(fields ...string) []byte {
		var b bytes.Buffer
		e := hpack.NewEncoder(&b)
		for i := 0; i < len(fields); i += 2 {
			_ = e.WriteField(hpack.HeaderField{Name: fields[i], Value: fields[i+1], Sensitive: true})
		}
		return b.Bytes()
	}
	respHeaders = enc(":status", "200", "content-type", "application/grpc")
	okTrailers = enc("grpc-status", "0")
}

func frameHeader(b *bytes.Buffer, length int, typ http2.FrameType, flags http2.Flags, id uint32) {
	b.Write([]byte{byte(length >> 16), byte(length >> 8), byte(length), byte(typ), byte(flags),
		byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)})
}

// appendData appends one gRPC message as DATA frames. The caller holds wmu.
func (c *hconn) appendData(id uint32, msg []byte, wait bool) error {
	n := int64(len(msg) + 5)
	for c.sendWin < n || c.strWin[id] < n {
		if !wait {
			return errors.New("client send window exhausted")
		}
		c.cond.Wait()
	}
	c.sendWin -= n
	c.strWin[id] -= n
	var prefix [5]byte
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(msg)))
	k := min(len(msg), 16384-5)
	frameHeader(&c.out, k+5, http2.FrameData, 0, id)
	c.out.Write(prefix[:])
	c.out.Write(msg[:k])
	for msg = msg[k:]; len(msg) > 0; msg = msg[k:] {
		k = min(len(msg), 16384)
		frameHeader(&c.out, k, http2.FrameData, 0, id)
		c.out.Write(msg[:k])
	}
	return nil
}

func (c *hconn) flushLocked() error {
	if c.out.Len() == 0 {
		return nil
	}
	_, err := c.nc.Write(c.out.Bytes())
	c.out.Reset()
	return err
}

func serve(s *store, nc net.Conn) error {
	defer nc.Close()
	br := bufio.NewReaderSize(nc, 64<<10)
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(br, preface); err != nil {
		return err
	}
	c := &hconn{nc: nc, sendWin: 65535, initWin: 65535, strWin: map[uint32]int64{}, streams: map[uint32]*hstream{}}
	c.cond = sync.NewCond(&c.wmu)
	c.fr = http2.NewFramer(&c.out, br)
	c.fr.SetReuseFrames()
	c.dec = hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		if f.Name == ":path" {
			c.path = f.Value
		}
	})
	c.wmu.Lock()
	_ = c.fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 30},
		http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 10000})
	_ = c.fr.WriteWindowUpdate(0, 1<<30-65535)
	err := c.flushLocked()
	c.wmu.Unlock()
	if err != nil {
		return err
	}
	for {
		f, err := c.fr.ReadFrame()
		if err != nil {
			return err
		}
		c.wmu.Lock()
		err = c.frame(s, f)
		if err == nil && br.Buffered() == 0 {
			err = c.flushLocked()
		}
		c.wmu.Unlock()
		if err != nil {
			return err
		}
	}
}

// frame handles one frame. The caller holds wmu.
func (c *hconn) frame(s *store, f http2.Frame) error {
	switch f := f.(type) {
	case *http2.SettingsFrame:
		if f.IsAck() {
			return nil
		}
		if v, ok := f.Value(http2.SettingInitialWindowSize); ok {
			for id := range c.strWin {
				c.strWin[id] += int64(v) - c.initWin
			}
			c.initWin = int64(v)
			c.cond.Broadcast()
		}
		return c.fr.WriteSettingsAck()
	case *http2.PingFrame:
		if !f.IsAck() {
			return c.fr.WritePing(true, f.Data)
		}
	case *http2.WindowUpdateFrame:
		if f.StreamID == 0 {
			c.sendWin += int64(f.Increment)
		} else if _, ok := c.strWin[f.StreamID]; ok {
			c.strWin[f.StreamID] += int64(f.Increment)
		}
		c.cond.Broadcast()
	case *http2.HeadersFrame:
		c.path = ""
		if _, err := c.dec.Write(f.HeaderBlockFragment()); err != nil {
			return err
		}
		if !f.HeadersEnded() {
			return errors.New("CONTINUATION not supported")
		}
		c.streams[f.StreamID] = &hstream{path: c.path}
		c.strWin[f.StreamID] = c.initWin
	case *http2.DataFrame:
		c.recv += int64(f.Length)
		if c.recv >= 1<<28 {
			if err := c.fr.WriteWindowUpdate(0, uint32(c.recv)); err != nil {
				return err
			}
			c.recv = 0
		}
		st := c.streams[f.StreamID]
		if st == nil {
			return nil
		}
		st.buf = append(st.buf, f.Data()...)
		if len(st.buf) < 5 || len(st.buf) < 5+int(binary.BigEndian.Uint32(st.buf[1:5])) {
			return nil
		}
		msg := st.buf[5:]
		if st.path == "/etcdserverpb.Watch/Watch" {
			st.buf = nil
			return c.watch(s, f.StreamID, msg)
		}
		if !f.StreamEnded() {
			return errors.New("unary request without END_STREAM")
		}
		delete(c.streams, f.StreamID)
		var resp []byte
		var err error
		switch st.path {
		case "/etcdserverpb.KV/Range":
			resp, c.hits, err = s.rangeReq(msg, c.scratch, c.hits)
			c.scratch = resp
		case "/etcdserverpb.KV/Txn":
			resp, err = s.txnReq(msg)
		default:
			err = fmt.Errorf("unsupported path %q", st.path)
		}
		if err != nil {
			return err
		}
		frameHeader(&c.out, len(respHeaders), http2.FrameHeaders, http2.FlagHeadersEndHeaders, f.StreamID)
		c.out.Write(respHeaders)
		if err := c.appendData(f.StreamID, resp, false); err != nil {
			return err
		}
		frameHeader(&c.out, len(okTrailers), http2.FrameHeaders, http2.FlagHeadersEndHeaders|http2.FlagHeadersEndStream, f.StreamID)
		c.out.Write(okTrailers)
		delete(c.strWin, f.StreamID)
	case *http2.RSTStreamFrame:
		delete(c.streams, f.StreamID)
		delete(c.strWin, f.StreamID)
	case *http2.GoAwayFrame:
		return io.EOF
	}
	return nil
}

// watch creates a watch from a WatchCreateRequest and starts its sender.
// The caller holds wmu.
func (c *hconn) watch(s *store, id uint32, msg []byte) error {
	w := &watcher{}
	w.cond = sync.NewCond(&w.mu)
	var start int64
	var prevKV bool
	err := each(msg, func(n protowire.Number, _ uint64, b []byte) {
		if n != wreqCreate {
			return
		}
		_ = each(b, func(n protowire.Number, v uint64, b []byte) {
			switch n {
			case wcKey:
				w.key = bytes.Clone(b)
			case wcEnd:
				w.end = bytes.Clone(b)
			case wcStart:
				start = int64(v)
			case wcPrevKV:
				prevKV = v == 1
			}
		})
	})
	if err != nil {
		return err
	}
	if w.key == nil || len(w.end) == 0 || !prevKV {
		return errors.New("only prefix watches with prev_kv are supported")
	}
	s.seq.Lock()
	rev := s.rev.Load()
	if start != 0 && start != rev+1 {
		s.seq.Unlock()
		return fmt.Errorf("watch from %d needs history; store is at %d", start, rev)
	}
	s.watchers = append(s.watchers, w)
	s.seq.Unlock()
	created := header(nil, wrHeader, rev)
	created = varint(created, wrID, uint64(id))
	created = varint(created, wrCreated, 1)
	frameHeader(&c.out, len(respHeaders), http2.FrameHeaders, http2.FlagHeadersEndHeaders, id)
	c.out.Write(respHeaders)
	if err := c.appendData(id, created, false); err != nil {
		return err
	}
	go c.sendEvents(w, id)
	return nil
}

// sendEvents sends the queued events of w in revision order, up to 16 per
// WatchResponse, as etcd batches the events it has ready.
func (c *hconn) sendEvents(w *watcher, id uint32) {
	for {
		w.mu.Lock()
		for len(w.events) == 0 {
			w.cond.Wait()
		}
		evs := w.events
		w.events = nil
		last := w.last
		w.mu.Unlock()
		c.wmu.Lock()
		for len(evs) > 0 {
			k := min(len(evs), 16)
			m := header(nil, wrHeader, last)
			m = varint(m, wrID, uint64(id))
			for _, e := range evs[:k] {
				m = appendEvent(m, e)
			}
			evs = evs[k:]
			if err := c.appendData(id, m, true); err != nil {
				c.wmu.Unlock()
				return
			}
		}
		err := c.flushLocked()
		c.wmu.Unlock()
		if err != nil {
			return
		}
	}
}
