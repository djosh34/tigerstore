package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

// The calls kube-apiserver 1.37 sends to etcd (k8s.io/apiserver
// pkg/storage/etcd3/store.go and go.etcd.io/etcd/client/v3/kubernetes at
// v3.7.2). Its watch cache serves a GET with a resourceVersion and every
// consistent LIST; etcd sees the rest.
const (
	// mixedGet is a GET without a resourceVersion: a Range of one key
	// with limit 1 (store.Get).
	mixedGet = iota
	// mixedRev is the revision a consistent LIST from the watch cache
	// waits for: a Range of the resource's prefix key with limit 1, which
	// answers no key (store.GetCurrentResourceVersion).
	mixedRev
	// mixedList is a LIST the watch cache does not serve: a RangeStream
	// of one namespace with the page limit of 500 (store.streamChunks).
	mixedList
	// mixedUpdate is GuaranteedUpdate: a Txn that puts if the mod
	// revision is unchanged and reads the key otherwise (OptimisticPut).
	mixedUpdate
	// mixedCreate is Create: a Txn that puts if the key has no mod
	// revision, with the events' lease on an event (OptimisticPut).
	mixedCreate
	// mixedDelete is Delete: a Txn that deletes if the mod revision is
	// unchanged and reads the key otherwise (OptimisticDelete).
	mixedDelete
	mixedOps
)

// defaultMix is the share of each call, get,rev,list,update,create,delete.
// The calls are kube-apiserver's; the shares are an estimate, made in
// #503: writes are most of what etcd sees, updates most of the writes
// (leases and status), and LISTs that miss the watch cache are rare.
const defaultMix = "18,10,2,50,12,8"

// listPage is the limit of a LIST page, the default of client-go's pager.
const listPage = 500

// eventTTL is the lease of events, kube-apiserver's --event-ttl default.
const eventTTL = 3600

func parseMix(s string) ([mixedOps]int, error) {
	var mix [mixedOps]int
	parts := strings.Split(s, ",")
	if len(parts) != len(mix) {
		return mix, errors.New("mix needs get,rev,list,update,create,delete percentages")
	}
	total := 0
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return mix, errors.New("mix percentages must be non-negative integers")
		}
		mix[i] = n
		total += n
	}
	if total != 100 {
		return mix, errors.New("mix percentages must sum to 100")
	}
	return mix, nil
}

type mixedKey struct {
	key       []byte
	bin       int
	mod       int64 // 0 while the key does not exist
	size      int   // the bytes of its value
	busy      bool  // a write of it is in flight
	group     int
	liveIndex int // index in the owner's live-key slice
}

type mixedCall struct {
	op    int
	key   *mixedKey
	value []byte
}

type mixedLoad struct {
	wk         *worker
	owner      int
	namespaces int
	resources  []string
	revKeys    [][]byte // per resource: the key mixedRev reads
	prefixes   [][]byte // per group: the prefix a LIST reads
	ends       [][]byte // per group: the end of its range
	pool       values
	live       []*mixedKey
	absent     []*mixedKey
	calls      map[uint32]mixedCall
	next       int
	preload    int
	pending    *mixedCall
	mix        [mixedOps]int
	ledger     *mixedLedger
	lease      int64 // the events' lease, 0 for none
	// Scratch for the parts of a request.
	buf, cmp, op, fail []byte
}

// Each worker owns its keys and groups, so that no two workers write a key.
func mixedPrefix(resource string, owner, namespace int) []byte {
	if clusterScoped(resource) {
		return fmt.Appendf(nil, "/registry/%s/bench-%03d-%04d-", resource, owner, namespace)
	}
	return fmt.Appendf(nil, "/registry/%s/bench-%03d-%04d/", resource, owner, namespace)
}

func mixedName(resource string, owner, namespace, id int) []byte {
	return fmt.Appendf(mixedPrefix(resource, owner, namespace), "object-%012d", id)
}

func clusterScoped(resource string) bool {
	switch resource {
	case "apiservices", "certificatesigningrequests", "clusterrolebindings", "clusterroles", "csinodes", "flowschemas", "minions", "namespaces", "priorityclasses", "prioritylevelconfigurations", "storageclasses":
		return true
	default:
		return false
	}
}

func newMixedLoad(wk *worker, cfg config, owner int, ledger *mixedLedger) (*mixedLoad, error) {
	mix, err := parseMix(cfg.mix)
	if err != nil {
		return nil, err
	}
	w := &mixedLoad{
		wk: wk, owner: owner, resources: k8ssize.Resources(), ledger: ledger,
		pool: *makeValues(33365, cfg.seed+uint64(owner)), calls: make(map[uint32]mixedCall), mix: mix,
	}
	// About 1,024 namespaces (or cluster-scoped name groups) overall.
	w.namespaces = max(1, 1024/cfg.conns)
	for _, r := range w.resources {
		w.revKeys = append(w.revKeys, []byte("/registry/"+r))
		for ns := range w.namespaces {
			p := mixedPrefix(r, owner, ns)
			w.prefixes = append(w.prefixes, p)
			w.ends = append(w.ends, prefixEnd(p))
		}
	}
	for id := owner; id < cfg.keys; id += cfg.conns {
		k := w.newKey()
		k.liveIndex = len(w.live)
		w.live = append(w.live, k)
	}
	w.preload = len(w.live)
	return w, nil
}

func (w *mixedLoad) newKey() *mixedKey {
	bin := k8ssize.Draw(w.wk.rng, "")
	resource := k8ssize.Buckets[bin].Resource
	r := 0
	for w.resources[r] != resource {
		r++
	}
	ns := w.next % w.namespaces
	k := &mixedKey{key: mixedName(resource, w.owner, ns, w.next), bin: bin, group: r*w.namespaces + ns}
	w.next++
	return k
}

// value draws the next value of k: a size of its resource, from the pool.
func (w *mixedLoad) value(k *mixedKey) []byte {
	k.bin = k8ssize.DrawLike(w.wk.rng, k.bin)
	w.pool.size = k8ssize.Buckets[k.bin].Size
	return w.pool.next()
}

// idle returns the index of a key of keys with no write in flight, from a
// random start, or -1.
func (w *mixedLoad) idle(keys []*mixedKey) int {
	if len(keys) == 0 {
		return -1
	}
	start := w.wk.rng.IntN(len(keys))
	for offset := range len(keys) {
		if i := (start + offset) % len(keys); !keys[i].busy {
			return i
		}
	}
	return -1
}

func (w *mixedLoad) choose() *mixedCall {
	n := w.wk.rng.IntN(100)
	op := mixedGet
	for i, p := range w.mix {
		n -= p
		if n < 0 {
			op = i
			break
		}
	}
	switch {
	case op == mixedRev:
		return &mixedCall{op: op}
	case op == mixedCreate || len(w.live) == 0:
		var k *mixedKey
		if w.wk.rng.IntN(2) == 0 {
			if i := w.idle(w.absent); i >= 0 {
				k = w.absent[i]
				w.absent[i] = w.absent[len(w.absent)-1]
				w.absent = w.absent[:len(w.absent)-1]
			}
		}
		if k == nil {
			k = w.newKey()
		}
		return &mixedCall{op: mixedCreate, key: k, value: w.value(k)}
	case op == mixedGet || op == mixedList:
		return &mixedCall{op: op, key: w.live[w.wk.rng.IntN(len(w.live))]}
	}
	i := w.idle(w.live)
	if i < 0 {
		return nil
	}
	call := &mixedCall{op: op, key: w.live[i]}
	if op == mixedUpdate {
		call.value = w.value(call.key)
	}
	return call
}

// request encodes call into w.buf and returns its path, the bytes it must
// make durable on each node, and the key and value bytes of the watch
// event a write makes, previous key-value included: the read work a
// write causes, whenever its event arrives.
func (w *mixedLoad) request(call *mixedCall) (path string, written, event int64) {
	w.buf = w.buf[:0]
	switch call.op {
	case mixedGet:
		w.buf = protowire.AppendTag(w.buf, rangeKey, protowire.BytesType)
		w.buf = protowire.AppendBytes(w.buf, call.key.key)
		w.buf = appendVarintField(w.buf, rangeLimit, 1)
		return "/etcdserverpb.KV/Range", 0, 0
	case mixedRev:
		w.buf = protowire.AppendTag(w.buf, rangeKey, protowire.BytesType)
		w.buf = protowire.AppendBytes(w.buf, w.revKeys[w.wk.rng.IntN(len(w.revKeys))])
		w.buf = appendVarintField(w.buf, rangeLimit, 1)
		return "/etcdserverpb.KV/Range", 0, 0
	case mixedList:
		g := call.key.group
		w.buf = protowire.AppendTag(w.buf, rangeKey, protowire.BytesType)
		w.buf = protowire.AppendBytes(w.buf, w.prefixes[g])
		w.buf = protowire.AppendTag(w.buf, rangeEnd, protowire.BytesType)
		w.buf = protowire.AppendBytes(w.buf, w.ends[g])
		w.buf = appendVarintField(w.buf, rangeLimit, listPage)
		return "/etcdserverpb.KV/RangeStream", 0, 0
	}
	k := call.key
	// The compare: the key's mod revision, 0 for a create.
	w.cmp = appendVarintField(w.cmp[:0], cmpTarget, cmpTargetMod)
	w.cmp = protowire.AppendTag(w.cmp, cmpKey, protowire.BytesType)
	w.cmp = protowire.AppendBytes(w.cmp, k.key)
	w.cmp = appendVarintField(w.cmp, cmpModRevision, uint64(k.mod))
	written = int64(len(k.key) + revisionBytes + len(k.key))
	// The event's key-value, and its previous one but after a create.
	event = int64(len(k.key) + len(call.value))
	if call.op != mixedCreate {
		event += int64(len(k.key) + k.size)
	}
	// The success branch: one put or delete of the key.
	w.op = protowire.AppendTag(w.op[:0], putKey, protowire.BytesType)
	w.op = protowire.AppendBytes(w.op, k.key)
	field := protowire.Number(opPut)
	if call.op == mixedDelete {
		field = opDeleteRange
	} else {
		w.op = protowire.AppendTag(w.op, putValue, protowire.BytesType)
		w.op = protowire.AppendBytes(w.op, call.value)
		written += int64(len(call.value))
		// kube-apiserver puts an event with its lease, on create and on
		// update alike.
		if w.lease != 0 && w.resources[k.group/w.namespaces] == "events" {
			w.op = appendVarintField(w.op, putLease, uint64(w.lease))
			written += revisionBytes
		}
	}
	w.buf = protowire.AppendTag(w.buf, txnCompare, protowire.BytesType)
	w.buf = protowire.AppendBytes(w.buf, w.cmp)
	w.buf = protowire.AppendTag(w.buf, txnSuccess, protowire.BytesType)
	w.buf = protowire.AppendVarint(w.buf, uint64(protowire.SizeTag(field)+protowire.SizeBytes(len(w.op))))
	w.buf = protowire.AppendTag(w.buf, field, protowire.BytesType)
	w.buf = protowire.AppendBytes(w.buf, w.op)
	if call.op != mixedCreate {
		// The failure branch reads the key, as GetOnFailure asks.
		w.fail = protowire.AppendTag(w.fail[:0], rangeKey, protowire.BytesType)
		w.fail = protowire.AppendBytes(w.fail, k.key)
		w.buf = protowire.AppendTag(w.buf, txnFailure, protowire.BytesType)
		w.buf = protowire.AppendVarint(w.buf, uint64(protowire.SizeTag(opRange)+protowire.SizeBytes(len(w.fail))))
		w.buf = protowire.AppendTag(w.buf, opRange, protowire.BytesType)
		w.buf = protowire.AppendBytes(w.buf, w.fail)
	}
	return "/etcdserverpb.KV/Txn", written, event
}

func appendVarintField(b []byte, num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}

func (w *mixedLoad) start(c *conn, st *stream, limit int) bool {
	if w.pending == nil {
		w.pending = w.choose()
	}
	if w.pending == nil {
		return false
	}
	call := w.pending
	path, written, event := w.request(call)
	if !c.canStart(messageLen(w.buf), limit) {
		return false
	}
	st.written, st.read = written, event
	st.many = call.op == mixedList
	c.startMessage(st, path, w.buf)
	w.calls[st.id] = *call
	if call.op >= mixedUpdate {
		call.key.busy = true
	}
	w.pending = nil
	return true
}

func (w *mixedLoad) exhausted() bool { return false }

// message reads the sizes of a read's answer, and the outcome and revision
// of a write, which the next write of the key compares against.
func (w *mixedLoad) message(st *stream, m []byte) error {
	call := w.calls[st.id]
	switch call.op {
	case mixedGet:
		n, err := rangeBytes(m)
		st.read += n
		return err
	case mixedRev:
		return nil
	case mixedList:
		r, err := bytesField(m, rangeStreamResponse)
		if err != nil {
			return err
		}
		n, err := rangeBytes(r)
		st.read += n
		return err
	}
	rev, ok, err := txnOutcome(m)
	if err != nil {
		return err
	}
	if !ok {
		return errCASFailed
	}
	k := call.key
	k.mod = rev
	k.size = len(call.value)
	w.ledger.wrote(rev, st.intended)
	switch call.op {
	case mixedDelete:
		last := w.live[len(w.live)-1]
		w.live[k.liveIndex] = last
		last.liveIndex = k.liveIndex
		w.live = w.live[:len(w.live)-1]
		k.mod, k.size = 0, 0
		w.absent = append(w.absent, k)
	case mixedCreate:
		k.liveIndex = len(w.live)
		w.live = append(w.live, k)
	}
	return nil
}

func (w *mixedLoad) done(st *stream, at int64, err error) {
	call := w.calls[st.id]
	delete(w.calls, st.id)
	if call.op >= mixedUpdate {
		call.key.busy = false
	}
	w.wk.stats.complete(st, at, err)
}

// mixedPreload puts the first keys with Txns of at most 128 puts and 60 KiB,
// within the peer's initial stream window, so no partly sent request needs
// a writer.
type mixedPreload struct {
	w     *mixedLoad
	next  int
	batch map[uint32][]*mixedKey
}

func (p *mixedPreload) exhausted() bool { return p.next == p.w.preload }

func (p *mixedPreload) start(c *conn, st *stream, limit int) bool {
	if p.exhausted() {
		return false
	}
	m := newMsg()
	var batch []*mixedKey
	for i := p.next; i < p.w.preload && len(batch) < 128; i++ {
		k := p.w.live[i]
		put := newMsg()
		put.bytes(putKey, k.key)
		v := p.w.value(k)
		k.size = len(v)
		put.bytes(putValue, v)
		op := newMsg()
		op.sub(opPut, put)
		if len(m.b)+len(op.b)+16 > min(60<<10, int(c.streamWin)) {
			break
		}
		m.sub(txnSuccess, op)
		batch = append(batch, k)
	}
	if len(batch) == 0 {
		c.err = errors.New("preload value exceeds stream window")
		return false
	}
	if !c.canStart(messageLen(m.b), min(8, limit)) {
		return false
	}
	c.startMessage(st, "/etcdserverpb.KV/Txn", m.b)
	p.batch[st.id] = batch
	p.next += len(batch)
	return true
}

func (p *mixedPreload) message(st *stream, m []byte) error {
	rev, ok, err := txnOutcome(m)
	if err != nil {
		return err
	}
	if !ok {
		return errCASFailed
	}
	for _, k := range p.batch[st.id] {
		k.mod = rev
	}
	return nil
}

func (p *mixedPreload) done(st *stream, at int64, err error) {
	delete(p.batch, st.id)
	p.w.wk.stats.complete(st, at, err)
}

// mixedLedger pairs each write with its watch event by revision, for the
// event's delay from the write's intended send time. Either may come
// first.
type mixedLedger struct {
	mu          sync.Mutex
	writes      map[int64]int64 // revision: intended send time of a write whose event has not come
	events      map[int64]int64 // revision: arrival of an event whose write has not answered
	start, stop int64
	lat         hist
	count       int64 // events that arrived in the window
}

func newMixedLedger() *mixedLedger {
	return &mixedLedger{writes: make(map[int64]int64), events: make(map[int64]int64)}
}

// window starts a new measurement window and forgets what was not paired.
func (l *mixedLedger) window(start, stop int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	clear(l.writes)
	clear(l.events)
	l.start, l.stop = start, stop
	l.lat, l.count = hist{}, 0
}

func (l *mixedLedger) wrote(rev, intended int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at, ok := l.events[rev]; ok {
		delete(l.events, rev)
		l.record(intended, at)
		return
	}
	l.writes[rev] = intended
}

func (l *mixedLedger) saw(rev, at int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at >= l.start && at < l.stop {
		l.count++
	}
	if intended, ok := l.writes[rev]; ok {
		delete(l.writes, rev)
		l.record(intended, at)
		return
	}
	l.events[rev] = at
}

// record counts the delay of an event whose write was meant to go out in
// the window. The caller holds l.mu.
func (l *mixedLedger) record(intended, at int64) {
	if intended >= l.start && intended < l.stop {
		l.lat.record(at - intended)
	}
}

// mixedWatch is one resource's watch, as kube-apiserver's watch cache
// opens it: the resource's prefix with previous key-values and progress
// notifications.
type mixedWatch struct {
	ledger  *mixedLedger
	prefix  []byte
	created bool
	err     error
}

// message keeps the watch's first error now, rather than in stream.err
// for the end of a call, since a watch never ends. Later responses of a
// failed watch are not read.
func (w *mixedWatch) message(_ *stream, m []byte) error {
	if w.err == nil {
		w.err = w.response(m, now())
	}
	return nil
}

func (w *mixedWatch) response(m []byte, at int64) error {
	return fields(m, func(n protowire.Number, v uint64, b []byte) error {
		//exhaustive:ignore // other fields do not change the watch
		switch n {
		case watchCreated:
			w.created = v == 1
		case watchCanceled, watchCompactRev:
			if v != 0 {
				return errors.New("mixed watch canceled or compacted")
			}
		case watchEvents:
			kv, err := bytesField(b, eventKV)
			if err != nil {
				return err
			}
			rev, err := varintField(kv, kvModRevision)
			if err != nil {
				return err
			}
			w.ledger.saw(int64(rev), at)
		}
		return nil
	})
}

func (w *mixedWatch) done(_ *stream, _ int64, err error) {
	if err == nil {
		err = errors.New("mixed watch ended")
	}
	if w.err == nil {
		w.err = err
	}
}

// grantLease grants a lease of ttl seconds on c, as kube-apiserver's lease
// manager does for events, and returns its ID.
func grantLease(c *conn, ttl int64) (int64, error) {
	g := &leaseGrant{ch: make(chan struct{})}
	c.mu.Lock()
	body := appendVarintField(nil, leaseTTL, uint64(ttl))
	if !c.canStart(messageLen(body), 1) {
		c.mu.Unlock()
		return 0, fmt.Errorf("%s: cannot start the lease grant", c.addr)
	}
	c.startMessage(c.newStream(g), "/etcdserverpb.Lease/LeaseGrant", body)
	c.mu.Unlock()
	if err := c.flush(); err != nil {
		return 0, err
	}
	select {
	case <-g.ch:
	case <-time.After(10 * time.Second):
		return 0, errors.New("lease grant: no answer in 10s")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if g.err == nil && g.id == 0 {
		g.err = errors.New("lease grant: no lease ID")
	}
	return g.id, g.err
}

type leaseGrant struct {
	id  int64
	err error
	ch  chan struct{}
}

func (g *leaseGrant) message(_ *stream, m []byte) error {
	id, err := varintField(m, leaseID)
	g.id = int64(id)
	return err
}

func (g *leaseGrant) done(_ *stream, _ int64, err error) {
	g.err = err
	close(g.ch)
}
