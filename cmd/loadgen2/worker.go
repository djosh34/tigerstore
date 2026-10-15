package main

import (
	"bytes"
	"net"
	"time"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	opGet = iota
	opRev
	opUpd
	opCre
	opDel
	opPre
	nOps
)

var maxBatch = 64 << 10

var opNames = [nOps]string{"get", "currev", "update", "create", "delete", "preload"}

type kstate struct {
	key                  []byte
	enc                  []byte // expected encoded KeyValue while live
	h                    uint64 // kvHash(enc)
	create, mod, version int64  // mod is the delete revision while absent
	res                  uint8
	live, busy           bool
	idx                  int32 // position in live or absent
}

type slot struct {
	id      uint32
	used    bool
	gotHdr  bool
	op      uint8
	key     int32
	msgs    int32
	floor   int64
	sent    int64
	val     []byte
	buf     []byte
	pre     []int32  // preload batch keys
	preVals [][]byte // preload batch values
	prefix  []byte   // currev key
}

type rec struct {
	rev int64
	fp  uint64
}

type worker struct {
	id, owner int
	c         *h2
	rng       *rand.Rand
	keys      []kstate
	live      []int32
	absent    []int32
	ns        int
	nextName  int
	pool      []byte
	slots     []slot
	mask      uint32
	nextID    uint32
	inflight  int
	S         int
	lastAck   int64
	hdrRange  []byte
	hdrTxn    []byte
	scratch   []byte
	tnow      int64
	t1, t2    int64
	stopping  bool
	owed      int
	t0        int64
	rate      float64 // ops per ns, 0 for closed loop
	issued    int64
	behind    int64 // schedule slots that found all streams busy
	mixCum    [5]int

	// results, read by main after the worker returns
	wlog      []rec
	plog      []int64
	resWrites []int64
	lat       hist
	opsWin    [nOps]int64
	errs      map[string]int
	fatal     error
}

func newWorker(id, owner int, c *h2, seed uint64, nkeys int, S int, mix [5]int) *worker {
	w := &worker{id: id, owner: owner, c: c, rng: rand.New(rand.NewPCG(seed, uint64(id)+1)), S: S, errs: map[string]int{}, nextID: 1}
	w.ns = max(1, 1024/workersTotal)
	n := 1
	for n < 4*S {
		n <<= 1
	}
	w.slots = make([]slot, n)
	w.mask = uint32(n - 1)
	w.pool = make([]byte, 128<<10)
	for i := range w.pool {
		w.pool[i] = byte(w.rng.Uint32())
	}
	w.resWrites = make([]int64, len(resources))
	w.hdrRange = headersFrame(c, "/etcdserverpb.KV/Range")
	w.hdrTxn = headersFrame(c, "/etcdserverpb.KV/Txn")
	sum := 0
	for i, p := range mix {
		sum += p
		w.mixCum[i] = sum
	}
	w.keys = make([]kstate, 0, nkeys*2)
	for range nkeys {
		w.newKey(int(resForNew[w.rng.IntN(len(resForNew))]))
	}
	return w
}

func headersFrame(c *h2, path string) []byte {
	blk := reqHeaders(c.tc.RemoteAddr().String(), path)
	return append(appendFH(nil, len(blk), fHeaders, flEndHdr, 0), blk...)
}

// newKey appends an absent key that is in neither list.
func (w *worker) newKey(res int) int32 {
	r := resources[res]
	ns := w.nextName % w.ns
	b := make([]byte, 0, 64)
	b = append(b, "/registry/"...)
	b = append(b, r...)
	b = append(b, "/bench-"...)
	b = pad(b, w.owner, 3)
	b = append(b, '-')
	b = pad(b, ns, 4)
	if clusterScoped(r) {
		b = append(b, '-')
	} else {
		b = append(b, '/')
	}
	b = append(b, "object-"...)
	b = pad(b, w.nextName, 12)
	w.nextName++
	w.keys = append(w.keys, kstate{key: b, res: uint8(res), idx: -1})
	return int32(len(w.keys) - 1)
}

func pad(b []byte, v, n int) []byte {
	s := strconv.Itoa(v)
	for i := len(s); i < n; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}

func clusterScoped(resource string) bool {
	switch resource {
	case "apiservices", "certificatesigningrequests", "clusterrolebindings", "clusterroles", "csinodes", "flowschemas", "minions", "namespaces", "priorityclasses", "prioritylevelconfigurations", "storageclasses":
		return true
	}
	return false
}

func (w *worker) value(res uint8) []byte {
	t := sizeTable[res]
	n := int(t[w.rng.IntN(len(t))])
	off := w.rng.IntN(len(w.pool) - n + 1)
	return w.pool[off : off+n]
}

func (w *worker) fail(what string) { w.errs[what]++ }

func (w *worker) failf(format string, a ...any) { w.errs[fmt.Sprintf(format, a...)]++ }

// --- request building --------------------------------------------------

func (w *worker) allocSlot() *slot {
	for {
		id := w.nextID
		w.nextID += 2
		sl := &w.slots[(id>>1)&w.mask]
		if !sl.used {
			*sl = slot{id: id, used: true, buf: sl.buf[:0], pre: sl.pre[:0], preVals: sl.preVals[:0], sent: w.tnow, floor: w.lastAck}
			w.inflight++
			return sl
		}
	}
}

// begin appends HEADERS and room for the DATA frame header and gRPC prefix,
// and returns the offset of the DATA frame header.
func (w *worker) begin(sl *slot, hdr []byte) int {
	out := w.c.out
	at := len(out)
	out = append(out, hdr...)
	binary.BigEndian.PutUint32(out[at+5:], sl.id)
	d := len(out)
	out = append(out, make([]byte, 14)...)
	w.c.out = out
	return d
}

// finish frames the message appended after begin.
func (w *worker) finish(sl *slot, d int) {
	out := w.c.out
	n := len(out) - d - 14
	payload := n + 5
	w.c.sendWin -= int64(payload)
	if payload <= maxFrame {
		h := appendFH(out[d:d], payload, fData, flEnd, sl.id)
		_ = h
		out[d+9] = 0
		binary.BigEndian.PutUint32(out[d+10:], uint32(n))
		return
	}
	msg := append(w.scratch[:0], out[d+14:]...)
	w.scratch = msg
	p := make([]byte, 5, 5+n)
	binary.BigEndian.PutUint32(p[1:], uint32(n))
	p = append(p, msg...)
	out = out[:d]
	for pos := 0; pos < len(p); {
		k := min(maxFrame, len(p)-pos)
		var fl byte
		if pos+k == len(p) {
			fl = flEnd
		}
		out = appendFH(out, k, fData, fl, sl.id)
		out = append(out, p[pos:pos+k]...)
		pos += k
	}
	w.c.out = out
}

func appendBytesField(b []byte, tag byte, v []byte) []byte {
	b = append(b, tag)
	b = protowire.AppendVarint(b, uint64(len(v)))
	return append(b, v...)
}

func sizeBytes(v int) int { return 1 + protowire.SizeVarint(uint64(v)) + v }

// txn appends a kube-apiserver shaped TxnRequest:
// update: If(mod == m) Then(Put) Else(Get); create: If(create == 0)
// Then(Put); delete: If(mod == m) Then(Delete) Else(Get).
func appendTxn(b []byte, op uint8, key []byte, mod int64, value []byte) []byte {
	kl := sizeBytes(len(key))
	var cmpLen int
	if op == opCre {
		cmpLen = 2 + kl + 2
	} else {
		cmpLen = 2 + kl + 1 + protowire.SizeVarint(uint64(mod))
	}
	var innerLen int // PutRequest or DeleteRangeRequest
	if op == opDel {
		innerLen = kl
	} else {
		innerLen = kl + sizeBytes(len(value))
	}
	succLen := sizeBytes(innerLen)
	// compare
	b = append(b, 1<<3|2)
	b = protowire.AppendVarint(b, uint64(cmpLen))
	if op == opCre {
		b = append(b, 2<<3, 1) // target CREATE
		b = appendBytesField(b, 3<<3|2, key)
		b = append(b, 5<<3, 0) // create_revision 0
	} else {
		b = append(b, 2<<3, 2) // target MOD
		b = appendBytesField(b, 3<<3|2, key)
		b = append(b, 6<<3)
		b = protowire.AppendVarint(b, uint64(mod))
	}
	// success
	b = append(b, 2<<3|2)
	b = protowire.AppendVarint(b, uint64(succLen))
	if op == opDel {
		b = append(b, 3<<3|2)
	} else {
		b = append(b, 2<<3|2)
	}
	b = protowire.AppendVarint(b, uint64(innerLen))
	b = appendBytesField(b, 1<<3|2, key)
	if op != opDel {
		b = appendBytesField(b, 2<<3|2, value)
	}
	if op != opCre {
		// failure: RequestOp{request_range{key}}
		b = append(b, 3<<3|2)
		b = protowire.AppendVarint(b, uint64(sizeBytes(kl)))
		b = append(b, 1<<3|2)
		b = protowire.AppendVarint(b, uint64(kl))
		b = appendBytesField(b, 1<<3|2, key)
	}
	return b
}

func (w *worker) pickLive() int32 {
	if len(w.live) == 0 {
		return -1
	}
	k := w.live[w.rng.IntN(len(w.live))]
	if w.keys[k].busy {
		return -1
	}
	return k
}

func (w *worker) pickAbsent() int32 {
	if len(w.absent) == 0 {
		return -1
	}
	k := w.absent[w.rng.IntN(len(w.absent))]
	if w.keys[k].busy {
		return -1
	}
	return k
}

func (w *worker) removeFrom(list *[]int32, k int32) {
	l := *list
	i := w.keys[k].idx
	last := l[len(l)-1]
	l[i] = last
	w.keys[last].idx = i
	*list = l[:len(l)-1]
	w.keys[k].idx = -1
}

func (w *worker) addTo(list *[]int32, k int32) {
	w.keys[k].idx = int32(len(*list))
	*list = append(*list, k)
}

// issue starts one request of the mix.
func (w *worker) issue() {
	if w.c.sendWin < 70000 {
		w.owed++
		return
	}
	for range 64 {
		if w.try() {
			return
		}
	}
	w.fail("could not pick a free key")
}

func (w *worker) try() bool {
	n := w.rng.IntN(100)
	op := uint8(opGet)
	for i, c := range w.mixCum {
		if n < c {
			op = uint8(i)
			break
		}
	}
	switch op {
	case opRev:
		res := w.rng.IntN(len(resources))
		sl := w.allocSlot()
		sl.op = opRev
		sl.prefix = resPrefix[res]
		d := w.begin(sl, w.hdrRange)
		w.c.out = appendBytesField(w.c.out, rangeKey<<3|2, sl.prefix)
		w.finish(sl, d)
		return true
	case opGet:
		k := int32(-1)
		if len(w.absent) > 0 && w.rng.IntN(10) == 0 {
			k = w.pickAbsent()
		} else {
			k = w.pickLive()
		}
		if k < 0 {
			return false
		}
		w.keys[k].busy = true
		sl := w.allocSlot()
		sl.op, sl.key = opGet, k
		d := w.begin(sl, w.hdrRange)
		w.c.out = appendBytesField(w.c.out, rangeKey<<3|2, w.keys[k].key)
		w.finish(sl, d)
		return true
	case opCre:
		k := int32(-1)
		if len(w.absent) > 0 && w.rng.IntN(2) == 0 {
			if k = w.pickAbsent(); k >= 0 {
				w.removeFrom(&w.absent, k)
			}
		}
		if k < 0 {
			k = w.newKey(int(resForNew[w.rng.IntN(len(resForNew))]))
		}
		ks := &w.keys[k]
		ks.busy = true
		sl := w.allocSlot()
		sl.op, sl.key = opCre, k
		sl.val = w.value(ks.res)
		d := w.begin(sl, w.hdrTxn)
		w.c.out = appendTxn(w.c.out, opCre, ks.key, 0, sl.val)
		w.finish(sl, d)
		return true
	default: // update, delete
		k := w.pickLive()
		if k < 0 {
			return len(w.live) == 0 && false
		}
		ks := &w.keys[k]
		ks.busy = true
		sl := w.allocSlot()
		sl.op, sl.key = op, k
		if op == opUpd {
			sl.val = w.value(ks.res)
		}
		d := w.begin(sl, w.hdrTxn)
		w.c.out = appendTxn(w.c.out, op, ks.key, ks.mod, sl.val)
		w.finish(sl, d)
		return true
	}
}

// issuePreload starts one batched Txn of puts over keys from next on and
// returns the next key.
func (w *worker) issuePreload(next int) int {
	sl := w.allocSlot()
	sl.op = opPre
	d := w.begin(sl, w.hdrTxn)
	start := len(w.c.out)
	for next < len(w.keys) && len(sl.pre) < 128 {
		ks := &w.keys[next]
		v := w.value(ks.res)
		put := sizeBytes(len(ks.key)) + sizeBytes(len(v))
		reqOp := sizeBytes(put)
		if len(w.c.out)-start+sizeBytes(reqOp) > 60<<10 && len(sl.pre) > 0 {
			break
		}
		b := append(w.c.out, 2<<3|2)
		b = protowire.AppendVarint(b, uint64(reqOp))
		b = append(b, 2<<3|2)
		b = protowire.AppendVarint(b, uint64(put))
		b = appendBytesField(b, 1<<3|2, ks.key)
		b = appendBytesField(b, 2<<3|2, v)
		w.c.out = b
		sl.pre = append(sl.pre, int32(next))
		sl.preVals = append(sl.preVals, v)
		next++
	}
	w.finish(sl, d)
	return next
}

// --- responses ---------------------------------------------------------

func (w *worker) lookup(sid uint32) *slot {
	sl := &w.slots[(sid>>1)&w.mask]
	if !sl.used || sl.id != sid {
		return nil
	}
	return sl
}

// frames handles every complete frame in the read buffer.
func (w *worker) frames() error {
	c := w.c
	for {
		typ, flags, sid, p, ok, err := c.frame()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		handled, err := c.control(typ, flags, sid, p)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		sl := w.lookup(sid)
		if sl == nil {
			return fmt.Errorf("frame %d for unknown stream %d", typ, sid)
		}
		switch typ {
		case fData:
			p, err = c.dataPayload(flags, p)
			if err != nil {
				return err
			}
			w.data(sl, p)
			if flags&flEnd != 0 {
				w.fail("stream ended without trailers")
				w.end(sl)
			}
		case fHeaders:
			blk, err := headerBlock(flags, p)
			if err != nil {
				return err
			}
			if flags&flEnd != 0 {
				if err := c.checkBlock(blk, true); err != nil {
					w.failf("trailers: %v", err)
				} else if !sl.gotHdr {
					w.fail("trailers-only response")
				} else if sl.msgs != 1 {
					w.fail("unary call without exactly one message")
				} else if len(sl.buf) > 0 {
					w.fail("truncated gRPC message")
				}
				w.end(sl)
			} else {
				if err := c.checkBlock(blk, false); err != nil {
					w.failf("headers: %v", err)
				}
				sl.gotHdr = true
			}
		case fRST:
			w.fail("stream reset")
			w.end(sl)
		}
	}
}

func (w *worker) data(sl *slot, p []byte) {
	if len(sl.buf) == 0 && len(p) >= 5 && p[0] == 0 && 5+int(binary.BigEndian.Uint32(p[1:5])) == len(p) {
		w.message(sl, p[5:])
		return
	}
	sl.buf = append(sl.buf, p...)
	b := sl.buf
	for len(b) >= 5 {
		if b[0] != 0 {
			w.fail("compressed gRPC message")
			b = nil
			break
		}
		n := 5 + int(binary.BigEndian.Uint32(b[1:5]))
		if len(b) < n {
			break
		}
		w.message(sl, b[5:n])
		b = b[n:]
	}
	sl.buf = append(sl.buf[:0], b...)
}

func (w *worker) ack(rev int64) {
	if rev > w.lastAck {
		w.lastAck = rev
	}
}

func (w *worker) message(sl *slot, m []byte) {
	sl.msgs++
	if sl.msgs > 1 {
		return // reported at trailers
	}
	switch sl.op {
	case opGet, opRev:
		w.checkRange(sl, m)
	case opPre:
		w.checkPreload(sl, m)
	default:
		w.checkTxn(sl, m)
	}
}

func (w *worker) checkRange(sl *slot, m []byte) {
	var rev int64
	var nkv int
	var kv []byte
	var count, more uint64
	for len(m) > 0 {
		num, _, v, b, n := field(m)
		if n < 0 {
			w.fail("range: malformed")
			return
		}
		switch num {
		case rrHeader:
			r, err := headerRev(b)
			if err != nil {
				w.fail("range: bad header")
				return
			}
			rev = r
		case rrKVs:
			nkv++
			kv = b
		case rrMore:
			more = v
		case rrCount:
			count = v
		}
		m = m[n:]
	}
	if rev == 0 {
		w.fail("range: no header")
		return
	}
	if rev < sl.floor {
		w.fail("range: header revision below the worker's last ack")
	}
	if sl.op == opRev {
		if nkv != 0 || count != 0 || more != 0 {
			w.fail("currev: prefix key returned kvs")
		}
		w.ack(rev)
		return
	}
	k := &w.keys[sl.key]
	if rev < k.mod {
		w.fail("get: header revision below the key's mod revision")
	}
	if k.live {
		if nkv != 1 || count != 1 || more != 0 {
			w.fail("get: live key not returned once")
		} else if !bytes.Equal(kv, k.enc) {
			canon, _, _, s, err := canonKV(kv, w.scratch)
			w.scratch = s
			if err != nil || !bytes.Equal(canon, k.enc) {
				w.fail("get: wrong KeyValue")
			}
		}
	} else if nkv != 0 || count != 0 || more != 0 {
		w.fail("get: absent key returned")
	}
	w.ack(rev)
}

// txnResp parses a TxnResponse: revision, succeeded, number of responses,
// kind of the last response and deleted of a delete response.
func txnResp(m []byte) (rev int64, succ uint64, nresp int, kind protowire.Number, deleted uint64, err error) {
	for len(m) > 0 {
		num, _, v, b, n := field(m)
		if n < 0 {
			return 0, 0, 0, 0, 0, errProto
		}
		switch num {
		case trHeader:
			rev, err = headerRev(b)
			if err != nil {
				return
			}
		case trSucc:
			succ = v
		case trResps:
			nresp++
			for r := b; len(r) > 0; {
				num, _, _, b2, n2 := field(r)
				if n2 < 0 {
					return 0, 0, 0, 0, 0, errProto
				}
				if num == roPut || num == roDelete || num == roRange {
					kind = num
				}
				if num == roDelete {
					deleted = 0
					for d := b2; len(d) > 0; {
						num3, _, v3, _, n3 := field(d)
						if n3 < 0 {
							return 0, 0, 0, 0, 0, errProto
						}
						if num3 == delDeleted {
							deleted = v3
						}
						d = d[n3:]
					}
				}
				r = r[n2:]
			}
		}
		m = m[n:]
	}
	if rev == 0 {
		err = errors.New("no header")
	}
	return
}

func (w *worker) checkTxn(sl *slot, m []byte) {
	rev, succ, nresp, kind, deleted, err := txnResp(m)
	if err != nil {
		w.fail("txn: malformed")
		return
	}
	if succ != 1 {
		w.fail("txn: compare failed")
		return
	}
	want := protowire.Number(roPut)
	if sl.op == opDel {
		want = roDelete
	}
	if nresp != 1 || kind != want {
		w.fail("txn: not exactly one response of the right kind")
		return
	}
	if sl.op == opDel && deleted != 1 {
		w.fail("txn: delete did not remove one key")
		return
	}
	k := &w.keys[sl.key]
	if rev <= k.mod || rev <= sl.floor {
		w.fail("txn: write revision did not advance")
	}
	w.ack(rev)
	var fp uint64
	switch sl.op {
	case opUpd:
		k.enc = encKV(k.enc[:0], k.key, k.create, rev, k.version+1, sl.val)
		h := kvHash(k.enc)
		fp = fingerprint(0, h, k.h, true)
		k.h = h
		k.mod = rev
		k.version++
	case opCre:
		k.create, k.mod, k.version = rev, rev, 1
		k.enc = encKV(k.enc[:0], k.key, rev, rev, 1, sl.val)
		k.h = kvHash(k.enc)
		fp = fingerprint(0, k.h, 0, false)
		k.live = true
		w.addTo(&w.live, sl.key)
	case opDel:
		de := encKV(w.scratch[:0], k.key, 0, rev, 0, nil)
		w.scratch = de
		fp = fingerprint(1, kvHash(de), k.h, true)
		k.enc = k.enc[:0]
		k.h = 0
		k.create, k.mod, k.version = 0, rev, 0
		k.live = false
		w.removeFrom(&w.live, sl.key)
		w.addTo(&w.absent, sl.key)
	}
	if fault == "rev" && len(w.wlog) == 1000 {
		rev++
	}
	w.wlog = append(w.wlog, rec{rev, fp})
	w.resWrites[k.res]++
}

func (w *worker) checkPreload(sl *slot, m []byte) {
	rev, succ, nresp, kind, _, err := txnResp(m)
	if err != nil || succ != 1 || nresp != len(sl.pre) || kind != roPut {
		w.fail("preload: bad txn response")
		return
	}
	if rev <= sl.floor {
		w.fail("preload: revision did not advance")
	}
	w.ack(rev)
	w.plog = append(w.plog, rev)
	for i, k := range sl.pre {
		ks := &w.keys[k]
		v := sl.preVals[i]
		ks.create, ks.mod, ks.version = rev, rev, 1
		ks.enc = encKV(make([]byte, 0, len(ks.key)+len(v)+40), ks.key, rev, rev, 1, v)
		ks.h = kvHash(ks.enc)
		ks.live = true
		w.addTo(&w.live, k)
		if fault == "enc" && k%1000 == 0 {
			ks.enc[len(ks.enc)-1] ^= 1
			ks.h = kvHash(ks.enc)
		}
	}
}

// end completes a stream and, while running, starts the next request.
func (w *worker) end(sl *slot) {
	if sl.op <= opDel {
		if sl.op != opRev {
			w.keys[sl.key].busy = false
		}
		if w.tnow >= w.t1 && w.tnow < w.t2 {
			w.lat.record(w.tnow - sl.sent)
			w.opsWin[sl.op]++
		}
	}
	op := sl.op
	sl.used = false
	sl.val = nil
	w.inflight--
	if op != opPre && !w.stopping && w.rate == 0 {
		w.issue()
	}
}

// round processes what was read.
func (w *worker) round() error {
	if err := w.frames(); err != nil {
		return err
	}
	for w.owed > 0 && w.c.sendWin >= 70000 && !w.stopping {
		w.owed--
		w.issue()
	}
	return nil
}

// issueDue starts the requests the fixed-rate schedule wants by now.
func (w *worker) issueDue() {
	due := int64(float64(w.tnow-w.t0) * w.rate)
	for w.issued < due && w.inflight < w.S {
		w.issue()
		w.issued++
	}
	if w.issued < due {
		w.behind += due - w.issued
		w.issued = due // drop, do not burst later
	}
}

// more reads what is already there without blocking; if nothing is, it
// sends what was produced and waits for data.
func (w *worker) more() error {
	if len(w.c.out) < maxBatch {
		got, err := w.c.tryFill()
		if err != nil {
			return err
		}
		if got {
			w.tnow = now()
			return nil
		}
	}
	if err := w.c.flush(); err != nil {
		return err
	}
	if w.rate > 0 && w.inflight == 0 {
		next := w.t0 + int64(float64(w.issued+1)/w.rate)
		w.c.tc.SetReadDeadline(epoch.Add(time.Duration(next)))
		err := w.c.fill()
		w.c.tc.SetReadDeadline(time.Time{})
		w.tnow = now()
		var ne net.Error
		if err != nil && !(errors.As(err, &ne) && ne.Timeout()) {
			return err
		}
		return nil
	}
	if err := w.c.fill(); err != nil {
		return err
	}
	w.tnow = now()
	return nil
}

func (w *worker) preload() error {
	next := 0
	w.tnow = now()
	for {
		for w.inflight < 8 && next < len(w.keys) {
			next = w.issuePreload(next)
		}
		if err := w.round(); err != nil {
			return err
		}
		if next == len(w.keys) && w.inflight == 0 {
			return w.c.flush()
		}
		if w.inflight < 8 && next < len(w.keys) {
			continue
		}
		if err := w.more(); err != nil {
			return err
		}
	}
}

func (w *worker) run(start <-chan struct{}) error {
	<-start
	w.tnow = now()
	if w.rate == 0 {
		for range w.S {
			w.issue()
		}
	}
	for {
		if !w.stopping && w.tnow >= w.t2 {
			w.stopping = true
		}
		if w.rate > 0 && !w.stopping {
			w.issueDue()
		}
		if err := w.round(); err != nil {
			return err
		}
		if w.stopping && w.inflight == 0 {
			return w.c.flush()
		}
		if err := w.more(); err != nil {
			return err
		}
	}
}
