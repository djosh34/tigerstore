package main

import (
	"errors"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// setupK8s draws the recording and each copy's start, writes every copy's
// keys and leases, and opens the recorded watches.
func (s *session) setupK8s() error {
	cfg := s.cfg
	d, err := k8strace.DrawSeed(cfg.traces, cfg.seed, cfg.copies)
	if err != nil {
		return err
	}
	t, rng := d.Trace, d.Rand
	s.trace = t
	if err := s.dial(); err != nil {
		return err
	}
	values := make([]byte, 8<<20)
	for i := range values {
		values[i] = byte(rng.Uint32())
	}
	keys := newK8sKeys(t)
	for i, wk := range s.workers {
		s.k8s = append(s.k8s, &k8sLoad{wk: wk, enc: encoder{values: values, off: i * len(values) / len(s.workers)}})
	}
	for id, k := range d.Starts {
		cp := newCopy(t, keys, t.From(k), id)
		w := s.k8s[id%len(s.k8s)]
		w.copies = append(w.copies, cp)
	}
	var preloads []load
	for _, w := range s.k8s {
		s.loads = append(s.loads, w)
		preloads = append(preloads, newK8sPreload(w))
	}
	if err := s.preload(preloads, cfg.preloadRate/float64(cfg.conns)); err != nil {
		return err
	}
	if err := s.openWatches(); err != nil {
		return err
	}
	for _, w := range s.k8s {
		w.watches = s.k8sWatches
	}
	return nil
}

// openWatches opens the recorded watch streams, from the revision after
// the preload, round robin over the request connections, which
// kube-apiserver's etcd clients also share between calls and watches, and
// waits until the store has created every watch. No worker runs yet, so
// it writes on the connections itself.
func (s *session) openWatches() error {
	// Keys of watches are the recorded ones, whatever the copy: any copy
	// writes them out the same.
	enc := encoder{cp: s.k8s[0].copies[0]}
	var m, create []byte
	for i, ws := range s.trace.Watches {
		c := s.workers[i%len(s.workers)].c
		kw := &k8sWatch{c: c, creates: len(ws.Creates), stats: stats{errs: make(map[string]int64)}}
		c.mu.Lock()
		kw.st = c.newStream(kw)
		kw.st.held = true
		c.held++
		c.out = c.openStream(c.out, kw.st, paths[k8strace.CallProgress])
		for _, x := range ws.Creates {
			create = enc.key(create[:0], watchKey, x.Key)
			create = enc.end(create, watchRangeEnd, x.End)
			create = appendBool(create, watchProgress, x.Progress)
			if len(x.Filters) > 0 {
				var packed []byte
				for _, f := range x.Filters {
					packed = protowire.AppendVarint(packed, uint64(f))
				}
				create = protowire.AppendBytes(protowire.AppendTag(create, watchFilters, protowire.BytesType), packed)
			}
			create = appendBool(create, watchPrevKV, x.PrevKV)
			m = protowire.AppendBytes(protowire.AppendTag(m[:0], watchCreate, protowire.BytesType), create)
			c.out = c.appendMessage(c.out, kw.st, m, false)
		}
		c.appendControl()
		c.mu.Unlock()
		if err := c.flush(); err != nil {
			return err
		}
		s.k8sWatches = append(s.k8sWatches, kw)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready := true
		for _, ws := range s.k8sWatches {
			ws.c.mu.Lock()
			ready = ready && ws.created == ws.creates
			err := errors.Join(ws.c.err, ws.err)
			ws.c.mu.Unlock()
			if err != nil {
				return err
			}
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("the recorded watches were not created within 30s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// k8sPreload writes the start of each copy of one connection: first its
// leases, then its keys, in Txns of at most 128 puts and 60 KiB, within
// the server's initial stream window.
type k8sPreload struct {
	w       *k8sLoad
	grants  []k8sItem // the leases to grant
	next    int       // the next grant to send
	waiting int       // grants sent and not answered
	items   []k8sItem
	item    int
	batch   map[uint32][]k8sItem
	grant   map[uint32]k8sItem
	buf     []byte
	put     []byte
	op      []byte
}

// minPreloadTTL is the least TTL of a lease the preload grants, in
// seconds: longer than a run, so that no lease of a copy's start, such as
// kube-apiserver's 15-second master leases, ends while the preload goes
// on and takes its keys with it.
const minPreloadTTL = 600

// k8sItem is a start item or lease of a copy.
type k8sItem struct {
	cp *k8sCopy
	i  int32
}

func newK8sPreload(w *k8sLoad) *k8sPreload {
	p := &k8sPreload{w: w, batch: map[uint32][]k8sItem{}, grant: map[uint32]k8sItem{}}
	for _, cp := range w.copies {
		for i := range cp.start.Leases {
			p.grants = append(p.grants, k8sItem{cp, int32(i)})
		}
		for i := range cp.start.Items {
			p.items = append(p.items, k8sItem{cp, int32(i)})
		}
	}
	return p
}

func (p *k8sPreload) exhausted() bool {
	return p.next == len(p.grants) && p.waiting == 0 && p.item == len(p.items)
}

func (p *k8sPreload) start(c *conn, st *stream, limit int) bool {
	if p.next < len(p.grants) {
		g := p.grants[p.next]
		p.buf = appendVarintField(p.buf[:0], leaseTTL, uint64(max(g.cp.start.Leases[g.i], minPreloadTTL)))
		if !c.canStart(messageLen(p.buf), limit) {
			return false
		}
		c.startMessage(st, paths[k8strace.CallLeaseGrant], p.buf)
		p.grant[st.id] = g
		p.next++
		p.waiting++
		return true
	}
	if p.waiting > 0 || p.item == len(p.items) {
		return false
	}
	p.buf = p.buf[:0]
	var batch []k8sItem
	for _, x := range p.items[p.item:] {
		if len(batch) == 128 {
			break
		}
		it := &x.cp.start.Items[x.i]
		p.w.enc.cp = x.cp
		p.put = p.w.enc.key(p.put[:0], putKey, it.Key)
		p.put = p.w.enc.value(p.put, putValue, int(it.Size))
		if it.Lease != 0 {
			p.put = appendVarintField(p.put, putLease, uint64(x.cp.leaseIDs[it.Lease-1]))
		}
		p.op = protowire.AppendBytes(protowire.AppendTag(p.op[:0], opPut, protowire.BytesType), p.put)
		if len(batch) > 0 && len(p.buf)+len(p.op)+16 > min(60<<10, int(c.streamWin)) {
			break
		}
		p.buf = protowire.AppendBytes(protowire.AppendTag(p.buf, txnSuccess, protowire.BytesType), p.op)
		batch = append(batch, x)
	}
	if !c.canStart(messageLen(p.buf), min(8, limit)) {
		return false
	}
	c.startMessage(st, paths[k8strace.CallTxn], p.buf)
	p.batch[st.id] = batch
	p.item += len(batch)
	return true
}

func (p *k8sPreload) message(st *stream, m []byte) error {
	if g, ok := p.grant[st.id]; ok {
		id, err := varintField(m, leaseID)
		g.cp.leaseIDs[g.i] = int64(id)
		return err
	}
	rev, ok, err := txnOutcome(m)
	if err != nil {
		return err
	}
	if !ok {
		return errCASFailed
	}
	for _, x := range p.batch[st.id] {
		x.cp.itemRevs[x.i] = rev
	}
	return nil
}

func (p *k8sPreload) done(st *stream, at int64, err error) {
	if _, ok := p.grant[st.id]; ok {
		delete(p.grant, st.id)
		p.waiting--
	}
	delete(p.batch, st.id)
	p.w.wk.stats.complete(st, at, err)
}

// k8sResult is what a k8s run replayed.
type k8sResult struct {
	Shape  string `json:"shape"`
	Seed   uint64 `json:"seed"`
	Copies int    `json:"copies"`
	// Calls is the recording's length, and Left the fewest calls a copy
	// had still to send at the end.
	Calls int `json:"calls"`
	Left  int `json:"left"`
	// Compacted counts reads at a revision another copy compacted away;
	// Skipped lease calls whose lease the copy's start no longer had.
	Compacted int64 `json:"compacted"`
	Skipped   int64 `json:"skipped"`
	// CacheReads counts reads of whole watched resources that copies
	// other than 0 left out (cacheRead).
	CacheReads int64 `json:"cache_reads"`
}

// k8sErrors returns what went wrong with the watches, or a copy that ran
// out of its recording.
func (s *session) k8sErrors() error {
	var errs []error
	for _, ws := range s.k8sWatches {
		ws.c.mu.Lock()
		errs = append(errs, ws.c.err, ws.err)
		ws.c.mu.Unlock()
	}
	for i, w := range s.k8s {
		c := s.workers[i].c
		c.mu.Lock()
		errs = append(errs, checkCopies(w.copies))
		c.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *session) summarizeK8s(res *result) {
	r := &k8sResult{Shape: s.trace.Shape, Seed: s.cfg.seed, Copies: s.cfg.copies, Calls: len(s.trace.Calls), Left: len(s.trace.Calls)}
	for i, w := range s.k8s {
		c := s.workers[i].c
		c.mu.Lock()
		r.Compacted += w.compacted
		r.Skipped += w.skipped
		r.CacheReads += w.cacheReads
		for _, cp := range w.copies {
			r.Left = min(r.Left, len(cp.t.Calls)-cp.next)
		}
		c.mu.Unlock()
	}
	var events int64
	for _, ws := range s.k8sWatches {
		ws.c.mu.Lock()
		events += ws.stats.events
		ws.c.mu.Unlock()
	}
	res.K8s = r
	res.ValueSize = 0
	res.SizeSource = "recording " + s.trace.Shape
	res.Watch = &watchResult{Watchers: len(s.trace.Watches), Events: events, EventsPerS: float64(events) / s.cfg.duration.Seconds()}
}
