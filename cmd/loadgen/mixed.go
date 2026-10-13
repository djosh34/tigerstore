package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// get, list, update, create, delete. Watches run alongside these arrivals.
const defaultMix = "25,10,40,15,10"

func parseMix(s string) ([5]int, error) {
	var mix [5]int
	parts := strings.Split(s, ",")
	if len(parts) != len(mix) {
		return mix, errors.New("mix needs get,list,update,create,delete percentages")
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
	key                  []byte
	bin                  int
	value                []byte
	create, mod, version int64
	busy                 bool
	group                int
	liveIndex            int // index in the owner's live-key slice
}

type mixedGroup struct {
	prefix  []byte
	keys    []*mixedKey // insertion order, including tombstones
	active  int
	listing bool
}

type mixedCall struct {
	op    int
	key   *mixedKey
	group *mixedGroup
	value []byte
	batch []*mixedKey
	floor int64 // latest acknowledged cluster revision when this call started
}

type mixedLoad struct {
	wk           *worker
	ownerID      int
	resources    []string
	cfg          config
	writes       *revisionSet
	ledger       *mixedLedger
	pool         values
	groups       []mixedGroup
	live, absent []*mixedKey
	calls        map[uint32]mixedCall
	next         int
	preload      int
	pending      *mixedCall
	mix          [5]int
}

// Namespace ownership also makes LIST validation exact: a LIST waits for
// its namespace's outstanding calls, then blocks mutations there until its
// response completes. Other namespaces continue sending in parallel.
func mixedName(resource string, owner, namespace, id int) []byte {
	return fmt.Appendf(nil, "/registry/%s/bench-%03d-%04d/object-%012d", resource, owner, namespace, id)
}

func newMixedLoad(wk *worker, cfg config, owner int, writes *revisionSet, ledger *mixedLedger) (*mixedLoad, error) {
	if cfg.mix == "" {
		cfg.mix = defaultMix
	}
	mix, err := parseMix(cfg.mix)
	if err != nil {
		return nil, err
	}
	w := &mixedLoad{wk: wk, ownerID: owner, resources: k8sResources(), cfg: cfg, writes: writes, ledger: ledger, pool: *makeValues(33365, cfg.seed+uint64(owner)), calls: make(map[uint32]mixedCall), mix: mix}
	resources := k8sResources()
	// About 1,024 namespaces across the connections. A common resource has
	// more than a 500-key page at 1M, so LIST tests count and more too.
	namespaces := max(1, 1024/cfg.conns)
	for _, r := range resources {
		for ns := range namespaces {
			w.groups = append(w.groups, mixedGroup{prefix: fmt.Appendf(nil, "/registry/%s/bench-%03d-%04d/", r, owner, ns)})
		}
	}
	for id := owner; id < cfg.keys; id += cfg.conns {
		k := w.newKey(owner, namespaces)
		k.liveIndex = len(w.live)
		w.live = append(w.live, k)
	}
	w.preload = len(w.live)
	return w, nil
}

func (w *mixedLoad) newKey(owner, namespaces int) *mixedKey {
	bin := drawBin(w.wk.rng, "")
	resource := k8sSizes[bin].resource
	resources := w.resources
	ns := w.next % namespaces
	group := slices.Index(resources, resource)*namespaces + ns
	k := &mixedKey{key: mixedName(resource, owner, ns, w.next), bin: bin, group: group}
	w.next++
	w.groups[group].keys = append(w.groups[group].keys, k)
	return k
}

func (w *mixedLoad) value(k *mixedKey) []byte {
	k.bin = drawBin(w.wk.rng, k8sSizes[k.bin].resource)
	w.pool.size = k8sSizes[k.bin].draw(w.wk.rng)
	return w.pool.next()
}

func mixedTxn(k *mixedKey, op int, value []byte) *msg {
	cmp := newMsg()
	if op == 3 {
		cmp.varint(cmpTarget, 1)
		cmp.bytes(cmpKey, k.key)
		cmp.varint(5, 0)
	} else {
		cmp.varint(cmpTarget, cmpTargetMod)
		cmp.bytes(cmpKey, k.key)
		cmp.varint(cmpModRevision, uint64(k.mod))
	}
	branch := newMsg()
	mutation := newMsg()
	mutation.bytes(putKey, k.key)
	if op == 4 {
		branch.sub(3, mutation)
	} else {
		mutation.bytes(putValue, value)
		branch.sub(opPut, mutation)
	}
	m := newMsg()
	m.sub(txnCompare, cmp)
	m.sub(txnSuccess, branch)
	return m
}

func (w *mixedLoad) choose() *mixedCall {
	n := w.wk.rng.IntN(100)
	op := 0
	for i, p := range w.mix {
		n -= p
		if n < 0 {
			op = i
			break
		}
	}
	if op == 3 {
		var k *mixedKey
		if len(w.absent) > 0 && w.wk.rng.IntN(2) == 0 {
			k = w.absent[len(w.absent)-1]
			w.absent = w.absent[:len(w.absent)-1]
		} else {
			k = w.newKey(w.owner(), max(1, 1024/w.cfg.conns))
		}
		return &mixedCall{op: op, key: k, value: w.value(k)}
	}
	if len(w.live) == 0 {
		k := w.newKey(w.owner(), max(1, 1024/w.cfg.conns))
		return &mixedCall{op: 3, key: k, value: w.value(k)}
	}
	index := w.wk.rng.IntN(len(w.live))
	var k *mixedKey
	for offset := range len(w.live) {
		candidate := w.live[(index+offset)%len(w.live)]
		if !candidate.busy {
			k = candidate
			break
		}
	}
	if k == nil {
		return nil
	}
	call := &mixedCall{op: op, key: k}
	if op == 1 {
		call.group = &w.groups[k.group]
	}
	if op == 2 {
		call.value = w.value(k)
	}
	return call
}

func (w *mixedLoad) owner() int { return w.ownerID }

func (w *mixedLoad) start(c *conn, st *stream) bool {
	if w.pending == nil {
		w.pending = w.choose()
	}
	if w.pending == nil {
		return false
	}
	call := w.pending
	g := &w.groups[call.key.group]
	if call.key.busy || g.listing || (call.op == 1 && g.active != 0) {
		return false
	}
	m := newMsg()
	path := "/etcdserverpb.KV/Range"
	switch call.op {
	case 0:
		m.bytes(rangeKey, call.key.key)
	case 1:
		m.bytes(rangeKey, g.prefix)
		m.bytes(rangeEnd, prefixEnd(g.prefix))
		m.varint(rangeLimit, 500)
	case 2, 3, 4:
		m = mixedTxn(call.key, call.op, call.value)
		path = "/etcdserverpb.KV/Txn"
	}
	tpl, err := newTemplate(c.addr, path, m, true)
	if err != nil {
		c.err = err
		return false
	}
	if !c.canStart(tpl, w.cfg.streams) {
		return false
	}
	call.floor = w.ledger.revision.Load()
	c.startStream(st, tpl)
	w.calls[st.id] = *call
	call.key.busy = true
	g.active++
	if call.op == 1 {
		g.listing = true
	}
	w.pending = nil
	return true
}
func (w *mixedLoad) exhausted() bool { return false }

// checkKV validates all of the stored fields, not only key and size.
func checkMixedKV(m []byte, k *mixedKey) error {
	key, err := bytesField(m, kvKey)
	if err != nil {
		return err
	}
	value, err := bytesField(m, kvValue)
	if err != nil {
		return err
	}
	if !bytes.Equal(key, k.key) || !bytes.Equal(value, k.value) {
		return fmt.Errorf("wrong key or value for %q", k.key)
	}
	for _, f := range []struct {
		field protowire.Number
		want  int64
	}{{2, k.create}, {3, k.mod}, {4, k.version}, {6, 0}} {
		v, err := varintField(m, f.field)
		if err != nil {
			return err
		}
		if int64(v) != f.want {
			return fmt.Errorf("%q field %d = %d, want %d", k.key, f.field, v, f.want)
		}
	}
	return nil
}

func checkMixedRange(m []byte, keys []*mixedKey, page int) error {
	count, err := varintField(m, rangeCount)
	if err != nil {
		return err
	}
	more, err := varintField(m, rangeMore)
	if err != nil {
		return err
	}
	if int(count) != len(keys) || (more != 0) != (len(keys) > page) {
		return fmt.Errorf("range count %d more %d, want %d keys with limit %d", count, more, len(keys), page)
	}
	at := 0
	err = fields(m, func(n protowire.Number, _ uint64, b []byte) error {
		if n != rangeKVs {
			return nil
		}
		if at >= min(page, len(keys)) {
			return errors.New("extra range key")
		}
		err := checkMixedKV(b, keys[at])
		at++
		return err
	})
	if err != nil {
		return err
	}
	if at != min(page, len(keys)) {
		return errors.New("range missed keys")
	}
	return nil
}

func (w *mixedLoad) message(st *stream, m []byte) error {
	call := w.calls[st.id]
	k := call.key
	rev, err := responseRevision(m)
	if err != nil {
		return err
	}
	if rev < k.mod || rev < call.floor {
		return errors.New("mixed response revision went backwards")
	}
	if call.op == 0 {
		w.ledger.revisionMax(rev)
		return checkMixedRange(m, []*mixedKey{k}, 1)
	}
	if call.op == 1 {
		var live []*mixedKey
		for _, k := range call.group.keys {
			if k.mod != 0 {
				live = append(live, k)
			}
		}
		for _, k := range live {
			if k.mod > rev {
				return errors.New("LIST returned a key above its header revision")
			}
		}
		w.ledger.revisionMax(rev)
		slices.SortFunc(live, func(a, b *mixedKey) int { return bytes.Compare(a.key, b.key) })
		return checkMixedRange(m, live, 500)
	}
	ok, err := varintField(m, txnSucceeded)
	if err != nil {
		return err
	}
	if ok != 1 {
		return errCASFailed
	}
	responses := 0
	err = fields(m, func(n protowire.Number, _ uint64, b []byte) error {
		if n != txnResponses {
			return nil
		}
		responses++
		field := protowire.Number(opResponsePut)
		if call.op == 4 {
			field = 3
		}
		inner, err := bytesField(b, field)
		if err != nil {
			return err
		}
		if inner == nil {
			return errors.New("mixed txn returned wrong operation")
		}
		if call.op == 4 {
			deleted, err := varintField(inner, delDeleted)
			if err != nil {
				return err
			}
			if deleted != 1 {
				return errors.New("mixed delete did not remove one key")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if responses != 1 {
		return errors.New("mixed txn did not return one operation")
	}
	if rev <= k.mod || rev <= call.floor {
		return errors.New("mixed write did not advance revision")
	}
	if err := w.writes.add(rev); err != nil {
		return err
	}
	w.ledger.revisionMax(rev)
	prev := *k
	if call.op == 4 {
		k.value = nil
		k.mod = rev
		k.create = 0
		k.version = 0
	} else {
		if call.op == 3 {
			k.create = rev
			k.version = 0
		}
		k.mod = rev
		k.version++
		k.value = call.value
	}
	w.ledger.expect(rev, k, &prev, call.op == 4, st.intended)
	switch call.op {
	case 4:
		last := w.live[len(w.live)-1]
		w.live[k.liveIndex] = last
		last.liveIndex = k.liveIndex
		w.live = w.live[:len(w.live)-1]
		k.mod = 0
		w.absent = append(w.absent, k)
	case 3:
		k.liveIndex = len(w.live)
		w.live = append(w.live, k)
	}
	return nil
}

func (w *mixedLoad) done(st *stream, at int64, err error) {
	call := w.calls[st.id]
	delete(w.calls, st.id)
	call.key.busy = false
	g := &w.groups[call.key.group]
	g.active--
	if call.op == 1 {
		g.listing = false
	}
	w.wk.stats.complete(st, at, err)
}

// Preload uses at most 128 puts and 60 KiB per Txn. It also respects the
// peer's initial stream window, so no partially sent request needs a writer.
type mixedPreload struct {
	w    *mixedLoad
	next int
}

func (p *mixedPreload) exhausted() bool { return p.next == p.w.preload }
func (p *mixedPreload) start(c *conn, st *stream) bool {
	if p.exhausted() {
		return false
	}
	m := newMsg()
	var batch []*mixedKey
	for i := p.next; i < p.w.preload && len(batch) < 128; i++ {
		k := p.w.live[i]
		if k.value == nil {
			k.value = p.w.value(k)
		}
		put := newMsg()
		put.bytes(putKey, k.key)
		put.bytes(putValue, k.value)
		op := newMsg()
		op.sub(opPut, put)
		if len(m.b)+len(op.b)+16 > min(60<<10, int(c.streamWin)) {
			break
		}
		m.sub(txnSuccess, op)
		batch = append(batch, k)
	}
	tpl, err := newTemplate(c.addr, "/etcdserverpb.KV/Txn", m, true)
	if err != nil {
		c.err = err
		return false
	}
	if len(batch) == 0 {
		c.err = errors.New("preload value exceeds stream window")
		return false
	}
	if !c.canStart(tpl, min(8, p.w.cfg.streams)) {
		return false
	}
	c.startStream(st, tpl)
	p.w.calls[st.id] = mixedCall{batch: batch}
	p.next += len(batch)
	return true
}

func (p *mixedPreload) message(st *stream, m []byte) error {
	rev, err := responseRevision(m)
	if err != nil {
		return err
	}
	ok, err := varintField(m, txnSucceeded)
	if err != nil {
		return err
	}
	if ok != 1 {
		return errCASFailed
	}
	batch := p.w.calls[st.id].batch
	n := 0
	err = fields(m, func(field protowire.Number, _ uint64, b []byte) error {
		if field != txnResponses {
			return nil
		}
		put, err := bytesField(b, opResponsePut)
		if err != nil {
			return err
		}
		if put == nil {
			return errors.New("preload response is not a put")
		}
		n++
		return nil
	})
	if err != nil {
		return err
	}
	if n != len(batch) {
		return errors.New("preload response count differs")
	}
	if err := p.w.writes.add(rev); err != nil {
		return err
	}
	for _, k := range batch {
		k.mod = rev
		k.create = rev
		k.version = 1
	}
	return nil
}

func (p *mixedPreload) done(st *stream, at int64, err error) {
	delete(p.w.calls, st.id)
	p.w.wk.stats.complete(st, at, err)
}

// A watch can beat its write reply. Pair by revision and compare a digest
// of the full event, keeping only unmatched events rather than all history.
type (
	mixedEvent struct {
		digest       [32]byte
		at, intended int64
	}
	mixedLedger struct {
		mu                 sync.Mutex
		revision           atomic.Int64
		expected, observed map[int64]mixedEvent
		errs               []error
		events             int64
		lat                hist
		start, stop        int64
	}
)

func mixedKV(k *mixedKey) *msg {
	m := newMsg()
	m.bytes(kvKey, k.key)
	if k.create != 0 {
		m.varint(2, uint64(k.create))
	}
	m.varint(kvModRevision, uint64(k.mod))
	if k.version != 0 {
		m.varint(4, uint64(k.version))
	}
	if len(k.value) > 0 {
		m.bytes(kvValue, k.value)
	}
	return m
}

func eventDigest(k, prev *mixedKey, deleted bool) [32]byte {
	m := newMsg()
	if deleted {
		m.varint(eventType, eventTypeDelete)
	}
	m.sub(eventKV, mixedKV(k))
	if prev.mod != 0 {
		m.sub(3, mixedKV(prev))
	}
	return sha256.Sum256(m.b)
}

func (l *mixedLedger) revisionMax(rev int64) {
	for old := l.revision.Load(); old < rev; old = l.revision.Load() {
		if l.revision.CompareAndSwap(old, rev) {
			return
		}
	}
}

func (l *mixedLedger) pair(rev int64, e mixedEvent, expected bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	own, other := l.observed, l.expected
	if expected {
		own, other = l.expected, l.observed
	}
	if old, ok := other[rev]; ok {
		delete(other, rev)
		if old.digest != e.digest {
			l.errs = append(l.errs, fmt.Errorf("watch differs at revision %d", rev))
		}
		if expected {
			e.at = old.at
		} else {
			e.intended = old.intended
		}
		if e.intended >= l.start && e.intended < l.stop {
			l.lat.record(e.at - e.intended)
		}
	} else {
		if _, ok := own[rev]; ok {
			l.errs = append(l.errs, fmt.Errorf("duplicate mixed event revision %d", rev))
		}
		own[rev] = e
	}
}

func (l *mixedLedger) expect(rev int64, k, prev *mixedKey, deleted bool, intended int64) {
	l.pair(rev, mixedEvent{digest: eventDigest(k, prev, deleted), intended: intended}, true)
}

type mixedWatch struct {
	ledger  *mixedLedger
	prefix  []byte
	created bool
	failed  bool
	last    int64
}

func (w *mixedWatch) message(_ *stream, m []byte) error {
	if w.failed {
		return nil
	}
	if err := w.response(m); err != nil {
		w.failed = true
		w.ledger.mu.Lock()
		w.ledger.errs = append(w.ledger.errs, fmt.Errorf("watch %s: %w", w.prefix, err))
		w.ledger.mu.Unlock()
	}
	// A watch never ends, so validation errors must be counted now,
	// rather than left in stream.err for unary completion.
	return nil
}

func (w *mixedWatch) response(m []byte) error {
	header, err := responseRevision(m)
	if err != nil {
		return err
	}
	return fields(m, func(n protowire.Number, v uint64, b []byte) error {
		//exhaustive:ignore // other protobuf fields do not change watch state
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
			key, err := bytesField(kv, kvKey)
			if err != nil {
				return err
			}
			if !w.created || int64(rev) <= w.last || int64(rev) > header || !bytes.HasPrefix(key, w.prefix) {
				return errors.New("mixed watch event out of order, outside prefix or above header")
			}
			w.last = int64(rev)
			// Re-encode known fields for canonical hashing. This also catches
			// incorrect event type, previous value and every KV field.
			canonical := newMsg()
			typ, err := varintField(b, eventType)
			if err != nil {
				return err
			}
			if typ != 0 {
				canonical.varint(eventType, typ)
			}
			for _, field := range []protowire.Number{eventKV, 3} {
				raw, err := bytesField(b, field)
				if err != nil {
					return err
				}
				if raw == nil {
					continue
				}
				km := newMsg()
				for _, f := range []protowire.Number{kvKey, 2, 3, 4, kvValue, 6} {
					if f == kvKey || f == kvValue {
						val, err := bytesField(raw, f)
						if err != nil {
							return err
						}
						if len(val) > 0 {
							km.bytes(f, val)
						}
					} else {
						val, err := varintField(raw, f)
						if err != nil {
							return err
						}
						if val != 0 {
							km.varint(f, val)
						}
					}
				}
				canonical.sub(field, km)
			}
			at := now()
			w.ledger.pair(int64(rev), mixedEvent{digest: sha256.Sum256(canonical.b), at: at}, false)
			w.ledger.mu.Lock()
			if at >= w.ledger.start && at < w.ledger.stop {
				w.ledger.events++
			}
			w.ledger.mu.Unlock()
		}
		return nil
	})
}

func (w *mixedWatch) done(_ *stream, _ int64, err error) {
	if err == nil {
		err = errors.New("mixed watch ended")
	}
	w.ledger.mu.Lock()
	w.ledger.errs = append(w.ledger.errs, err)
	w.ledger.mu.Unlock()
}

func runMixed(cfg config) (*result, error) {
	if cfg.mix == "" {
		cfg.mix = defaultMix
	}
	writes := newRevisionSet()
	ledger := &mixedLedger{expected: make(map[int64]mixedEvent), observed: make(map[int64]mixedEvent)}
	var workers []*worker
	var loads []load
	var preloads []load
	defer func() {
		for _, wk := range workers {
			wk.c.close()
		}
	}()
	for i := range cfg.conns {
		c, err := dial(cfg.endpoints[i%len(cfg.endpoints)])
		if err != nil {
			return nil, err
		}
		wk := newWorker(c, cfg.seed+uint64(i))
		workers = append(workers, wk)
		w, err := newMixedLoad(wk, cfg, i, writes, ledger)
		if err != nil {
			return nil, err
		}
		loads = append(loads, w)
		preloads = append(preloads, &mixedPreload{w: w})
	}
	start := now()
	if err := runPhase(workers, preloads, phase{rate: cfg.preloadRate / float64(cfg.conns), start: start, stop: start + int64(preloadTimeout), drain: drainTimeout}); err != nil {
		return nil, fmt.Errorf("mixed preload: %w", err)
	}
	for _, p := range preloads {
		if !p.exhausted() {
			return nil, errors.New("mixed preload did not finish")
		}
	}
	if err := phaseErrors(workers); err != nil {
		return nil, err
	}
	var watchConns []*conn
	var watches []*mixedWatch
	stop := make(chan struct{})
	var control sync.WaitGroup
	defer func() {
		close(stop)
		control.Wait()
		for _, c := range watchConns {
			c.close()
		}
	}()
	// Watch from the revision after preload, not from now, to exercise the
	// kube-apiserver LIST/watch handoff. No mutations start until created.
	var revision int64
	for _, l := range loads {
		for _, k := range l.(*mixedLoad).live {
			revision = max(revision, k.mod)
		}
	}
	ledger.revision.Store(revision)
	for i, r := range k8sResources() {
		c, err := dial(cfg.endpoints[i%len(cfg.endpoints)])
		if err != nil {
			return nil, err
		}
		watchConns = append(watchConns, c)
		prefix := []byte("/registry/" + r + "/")
		m := watchRequest(prefix)
		create := newMsg()
		create.bytes(watchKey, prefix)
		create.bytes(watchRangeEnd, prefixEnd(prefix))
		create.varint(3, uint64(revision+1))
		create.varint(watchPrevKV, 1)
		m.b = nil
		m.sub(watchCreate, create)
		tpl, err := newTemplate(c.addr, "/etcdserverpb.Watch/Watch", m, false)
		if err != nil {
			return nil, err
		}
		w := &mixedWatch{ledger: ledger, prefix: prefix, last: revision}
		watches = append(watches, w)
		c.mu.Lock()
		c.startStream(c.newStream(w), tpl)
		c.mu.Unlock()
		if err := c.flush(); err != nil {
			return nil, err
		}
		control.Go(func() { c.serveControl(stop) })
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready := true
		for i, c := range watchConns {
			c.mu.Lock()
			ready = ready && watches[i].created
			err := c.err
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			return nil, errors.New("mixed watches not created")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t0 := now() + int64(50*time.Millisecond)
	t1 := t0 + int64(cfg.warmup)
	t2 := t1 + int64(cfg.duration)
	ledger.mu.Lock()
	ledger.start, ledger.stop = t1, t2
	ledger.mu.Unlock()
	for _, wk := range workers {
		wk.stats = stats{winStart: t1, winStop: t2, errs: make(map[string]int64)}
	}
	var samples [2]cpuSample
	var sampleErr error
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for i, at := range []int64{t1, t2} {
			time.Sleep(time.Duration(at - now()))
			samples[i], sampleErr = sampleCPU(cfg.pids)
			if sampleErr != nil {
				return
			}
		}
	})
	phaseErr := runPhase(workers, loads, phase{rate: cfg.rate / float64(cfg.conns), start: t0, stop: t2, drain: drainTimeout})
	sampler.Wait()
	deadline = time.Now().Add(watchCatchUp)
	for {
		ledger.mu.Lock()
		pending := len(ledger.expected) + len(ledger.observed)
		ledger.mu.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			phaseErr = errors.Join(phaseErr, fmt.Errorf("mixed watch has %d unmatched events", pending))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Closing first lets us inspect watch errors without racing the readers.
	for _, c := range watchConns {
		c.mu.Lock()
		ledger.mu.Lock()
		if c.err != nil {
			ledger.errs = append(ledger.errs, c.err)
		}
		for _, st := range c.streams {
			if st.err != nil {
				ledger.errs = append(ledger.errs, st.err)
			}
		}
		ledger.mu.Unlock()
		c.mu.Unlock()
		c.close()
	}
	watchConns = nil
	res := summarize(cfg, workers, nil, samples)
	res.ValueSize = 0
	res.SizeSource = "k8s-1.37-dump"
	res.Mix = cfg.mix
	for _, l := range loads {
		res.LiveKeys += len(l.(*mixedLoad).live)
	}
	ledger.mu.Lock()
	for _, err := range ledger.errs {
		res.Errors[err.Error()]++
	}
	res.Watch = &watchResult{Watchers: len(watches), Events: ledger.events, EventsPerS: float64(ledger.events) / cfg.duration.Seconds(), Latency: ledger.lat.summary()}
	ledger.mu.Unlock()
	if res.Ops > 0 {
		res.Watch.ServerCPUUsPerEvent = res.ServerCPUUsPerOp * float64(res.Ops) / float64(max(1, res.Watch.Events))
	}
	err := errors.Join(phaseErr, sampleErr)
	if len(cfg.pids) == 0 {
		err = errors.Join(err, errors.New("no server pids"))
	}
	if len(res.Errors) > 0 {
		err = errors.Join(err, errors.New("mixed response validation failed"))
	}
	res.Valid = err == nil
	return res, err
}
