package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

// The mix of a worker's calls, in percent, as loadgen's mixed workload:
// get, list, update, create, delete.
var mix = [5]int{25, 10, 40, 15, 10}

const (
	opGet = iota
	opList
	opUpdate
	opCreate
	opDelete
)

var opNames = [5]string{"get", "list", "update", "create", "delete"}

// leaseTTL is the TTL of the leases events are written with, as the
// apiserver's, and leaseEvery how often a worker grants a new one.
const (
	leaseTTL   = 120
	leaseEvery = time.Minute
)

// object is one Kubernetes-shaped object a worker owns. Only that worker
// writes it, one call at a time, so it knows what every read returns.
type object struct {
	key   string
	group int
	// bin is the size row the object's values are drawn from.
	bin int
	// present, val and mod are the object's state as of the last answer:
	// whether it exists, the hash of its value and its mod revision.
	present bool
	val     uint64
	mod     int64
	// leased objects vanish when their lease expires, which it may from
	// vanishAt on.
	leased   bool
	vanishAt time.Time
	// pending is a write whose answer was lost: it may have happened.
	pending *write
	// acked are the changes the object went through that no watch event
	// has matched yet, and seen the watch events not matched yet.
	acked, seen []change
}

// write is a call that changes an object: a put of a value, or a delete.
type write struct {
	del      bool
	val      uint64
	leased   bool
	vanishAt time.Time
	// after is the highest revision acked before the call.
	after int64
}

// change is a change of an object at a revision in [lo, hi]: exact for an
// acked write, a range for one learned later.
type change struct {
	lo, hi int64
	del    bool
	val    uint64
	// opt is set when the change may not have happened.
	opt bool
}

func (c change) String() string {
	what := fmt.Sprintf("put of value %016x", c.val)
	if c.del {
		what = "delete"
	}
	if c.lo == c.hi {
		return fmt.Sprintf("%s at revision %d", what, c.lo)
	}
	return fmt.Sprintf("%s at a revision from %d to %d", what, c.lo, c.hi)
}

// group is a worker's objects of one resource, under one prefix.
type group struct {
	resource string
	prefix   string
	objects  []*object
}

// worker is one sequential client of the churn.
type worker struct {
	l   *load
	id  int
	rng *rand.Rand
	// mu guards the objects, which the watches match changes against.
	mu     sync.Mutex
	groups []*group
	seq    uint64
	// last is the highest header revision this worker saw.
	last  int64
	lease clientv3.LeaseID
	// leaseAt is when the lease was granted, and leaseVanish when it may
	// expire.
	leaseAt, leaseVanish time.Time
}

// load is the churn of every worker and what it found.
type load struct {
	cfg     config
	cl      *clientv3.Client
	workers []*worker
	byKey   map[string]*object
	owner   map[string]*worker
	// acked is the highest revision of any answer so far: a call made
	// later gets an answer at least that high.
	acked atomic.Int64
	stats stats
	fail  func(error)
}

// stats counts what the load did.
type stats struct {
	mu        sync.Mutex
	Calls     map[string]int64 `json:"calls"`
	Ambiguous map[string]int64 `json:"ambiguous"`
	Errors    map[string]int64 `json:"errors"`
	Resolved  int64            `json:"resolved"`
	Vanished  int64            `json:"vanished"`
	Leases    int64            `json:"leases"`
	Bytes     int64            `json:"bytes_written"`
}

func (s *stats) count(m map[string]int64, k string) {
	s.mu.Lock()
	m[k]++
	s.mu.Unlock()
}

func newLoad(cfg config, cl *clientv3.Client, fail func(error)) *load {
	l := &load{cfg: cfg, cl: cl, byKey: map[string]*object{}, owner: map[string]*worker{}, fail: fail}
	l.stats.Calls, l.stats.Ambiguous, l.stats.Errors = map[string]int64{}, map[string]int64{}, map[string]int64{}
	resources := k8ssize.Resources()
	for id := range cfg.workers {
		w := &worker{l: l, id: id, rng: rand.New(rand.NewPCG(cfg.seed, uint64(id)))}
		byResource := map[string]*group{}
		for _, r := range resources {
			g := &group{resource: r, prefix: fmt.Sprintf("/registry/%s/soak-w%03d/", r, id)}
			byResource[r] = g
			w.groups = append(w.groups, g)
		}
		for k := range cfg.objects / cfg.workers {
			bin := k8ssize.Draw(w.rng, "")
			g := byResource[k8ssize.Buckets[bin].Resource]
			o := &object{key: fmt.Sprintf("%sobj-%07d", g.prefix, k), bin: bin, leased: g.resource == "events"}
			for gi, gg := range w.groups {
				if gg == g {
					o.group = gi
				}
			}
			g.objects = append(g.objects, o)
			l.byKey[o.key] = o
			l.owner[o.key] = w
		}
		l.workers = append(l.workers, w)
	}
	return l
}

// violation is a finding: the store answered what no order of the calls
// allows.
type violation struct{ msg string }

func (v *violation) Error() string { return v.msg }

func (w *worker) violate(format string, args ...any) error {
	err := &violation{fmt.Sprintf("worker %d: ", w.id) + fmt.Sprintf(format, args...)}
	w.l.fail(err)
	return err
}

// value makes the next value of o: a header that names it, and filler
// up to a size drawn from o's resource.
func (w *worker) value(o *object) []byte {
	w.seq++
	o.bin = k8ssize.Draw(w.rng, k8ssize.Buckets[o.bin].Resource)
	size := k8ssize.Buckets[o.bin].Size
	b := fmt.Appendf(make([]byte, 0, max(size, 64)), "soak|%s|%d|", o.key, w.seq)
	for len(b) < size {
		b = append(b, byte('a'+w.rng.IntN(26)))
	}
	w.l.stats.mu.Lock()
	w.l.stats.Bytes += int64(len(b))
	w.l.stats.mu.Unlock()
	return b
}

func hash(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// call runs one call and checks its header: no lower than any answer
// before it, as a linearizable store gives.
func (w *worker) call(ctx context.Context, kind string, f func(context.Context) (*etcdserverpb.ResponseHeader, error)) error {
	before := w.l.acked.Load()
	cctx, cancel := context.WithTimeout(ctx, w.l.cfg.requestTimeout)
	defer cancel()
	w.l.stats.count(w.l.stats.Calls, kind)
	h, err := f(cctx)
	if err != nil {
		return err
	}
	rev := h.Revision
	if rev < before || rev < w.last {
		return w.violate("%s answered at revision %d, below revision %d of an answer before its call", kind, rev, max(before, w.last))
	}
	w.last = rev
	for {
		cur := w.l.acked.Load()
		if rev <= cur || w.l.acked.CompareAndSwap(cur, rev) {
			break
		}
	}
	return nil
}

// txn runs a Txn through call.
func (w *worker) txn(ctx context.Context, kind string, build func(clientv3.Txn) clientv3.Txn) (*clientv3.TxnResponse, error) {
	var resp *clientv3.TxnResponse
	err := w.call(ctx, kind, func(c context.Context) (*etcdserverpb.ResponseHeader, error) {
		var err error
		if resp, err = build(w.l.cl.Txn(c)).Commit(); err != nil {
			return nil, err
		}
		return resp.Header, nil
	})
	return resp, err
}

// read runs a linearizable Range through call, as the apiserver reads.
func (w *worker) read(ctx context.Context, kind, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	err := w.call(ctx, kind, func(c context.Context) (*etcdserverpb.ResponseHeader, error) {
		var err error
		if resp, err = w.l.cl.Get(c, key, opts...); err != nil {
			return nil, err
		}
		return resp.Header, nil
	})
	return resp, err
}

// get reads o's key.
func (w *worker) get(ctx context.Context, o *object) (*mvccpb.KeyValue, int64, error) {
	resp, err := w.read(ctx, "get", o.key)
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, resp.Header.Revision, nil
	}
	return resp.Kvs[0], resp.Header.Revision, nil
}

// observe checks what an answer at revision rev shows of o: the state
// the worker knows, or, for a leased object whose lease may have expired,
// nothing.
func (w *worker) observe(o *object, kv *mvccpb.KeyValue, rev int64, what string) error {
	_, err := w.vanished(o, kv, rev, what)
	return err
}

// vanished is observe, and reports whether o vanished with its lease.
func (w *worker) vanished(o *object, kv *mvccpb.KeyValue, rev int64, what string) (bool, error) {
	switch {
	case kv == nil && !o.present:
		return false, nil
	case kv != nil && o.present && hash(kv.Value) == o.val && kv.ModRevision == o.mod:
		return false, nil
	case kv == nil && o.leased && !time.Now().Before(o.vanishAt):
		// Its lease expired.
		o.acked = append(o.acked, change{lo: o.mod + 1, hi: rev, del: true})
		o.present, o.val, o.mod = false, 0, 0
		w.l.stats.mu.Lock()
		w.l.stats.Vanished++
		w.l.stats.mu.Unlock()
		return true, nil
	}
	return false, w.violate("%s of %s at revision %d shows %s, but only this worker writes it, and it last left it %s", what, o.key, rev, describeKV(kv), describeState(o))
}

func describeKV(kv *mvccpb.KeyValue) string {
	if kv == nil {
		return "no key"
	}
	return fmt.Sprintf("value %016x at mod revision %d", hash(kv.Value), kv.ModRevision)
}

func describeState(o *object) string {
	if !o.present {
		return "absent"
	}
	s := fmt.Sprintf("with value %016x at mod revision %d", o.val, o.mod)
	if o.leased {
		s += " on a lease that may expire from " + o.vanishAt.Format(time.RFC3339Nano)
	}
	return s
}

// resolve learns what became of o's pending write, with a read.
func (w *worker) resolve(ctx context.Context, o *object) error {
	p := o.pending
	if p == nil {
		return nil
	}
	kv, rev, err := w.get(ctx, o)
	if err != nil {
		return err
	}
	o.pending = nil
	w.l.stats.mu.Lock()
	w.l.stats.Resolved++
	w.l.stats.mu.Unlock()
	switch {
	case !p.del && kv != nil && hash(kv.Value) == p.val && kv.ModRevision > max(o.mod, p.after):
		o.acked = append(o.acked, change{lo: kv.ModRevision, hi: kv.ModRevision, val: p.val})
		o.present, o.val, o.mod, o.leased, o.vanishAt = true, p.val, kv.ModRevision, p.leased, p.vanishAt
		return nil
	case p.del && kv == nil && o.present:
		o.acked = append(o.acked, change{lo: max(o.mod, p.after) + 1, hi: rev, del: true})
		o.present, o.val, o.mod = false, 0, 0
		return nil
	case !p.del && kv == nil && p.leased && !time.Now().Before(p.vanishAt):
		// The put may have happened and its lease expired since, or it
		// did not happen: watches may show the put and the delete, or
		// only the old object's delete, or nothing.
		lo := max(o.mod, p.after) + 1
		o.acked = append(o.acked, change{lo: lo, hi: rev, val: p.val, opt: true}, change{lo: lo, hi: rev, del: true, opt: true})
		o.present, o.val, o.mod = false, 0, 0
		return nil
	}
	return w.observe(o, kv, rev, "the read after an ambiguous write")
}

// step makes one call of the mix.
func (w *worker) step(ctx context.Context) error {
	n := w.rng.IntN(100)
	op := 0
	for n >= mix[op] {
		n -= mix[op]
		op++
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.renewLease(ctx); err != nil {
		return err
	}
	g := w.groups[w.rng.IntN(len(w.groups))]
	if len(g.objects) == 0 {
		return nil
	}
	if op == opList {
		return w.list(ctx, g)
	}
	o := g.objects[w.rng.IntN(len(g.objects))]
	if err := w.resolve(ctx, o); err != nil {
		return w.lost(opNames[opGet], err)
	}
	switch {
	case op == opCreate && o.present:
		op = opUpdate
	case (op == opUpdate || op == opDelete) && !o.present:
		op = opCreate
	}
	switch op {
	case opGet:
		kv, rev, err := w.get(ctx, o)
		if err != nil {
			return w.lost(opNames[op], err)
		}
		return w.observe(o, kv, rev, "a read")
	case opCreate, opUpdate:
		return w.put(ctx, o, op)
	default:
		return w.delete(ctx, o)
	}
}

// lost counts a call whose answer did not come.
func (w *worker) lost(kind string, err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	if _, ok := errors.AsType[*violation](err); ok {
		return err
	}
	w.l.stats.count(w.l.stats.Errors, kind+": "+errorKind(err))
	return nil
}

func errorKind(err error) string {
	if ev, ok := errors.AsType[rpctypes.EtcdError](err); ok {
		return ev.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline exceeded"
	}
	s := err.Error()
	if len(s) > 60 {
		s = s[:60]
	}
	return s
}

// put creates o if it is absent or updates it if not, as the apiserver
// does: a Txn that compares its create or mod revision.
func (w *worker) put(ctx context.Context, o *object, op int) error {
	v := w.value(o)
	p := &write{val: hash(v), after: w.l.acked.Load()}
	var opts []clientv3.OpOption
	if o.leased {
		opts = append(opts, clientv3.WithLease(w.lease))
		p.leased, p.vanishAt = true, w.leaseVanish
	}
	cmp := clientv3.Compare(clientv3.ModRevision(o.key), "=", o.mod)
	if op == opCreate {
		cmp = clientv3.Compare(clientv3.CreateRevision(o.key), "=", 0)
	}
	resp, err := w.txn(ctx, opNames[op], func(t clientv3.Txn) clientv3.Txn {
		return t.If(cmp).Then(clientv3.OpPut(o.key, string(v), opts...)).Else(clientv3.OpGet(o.key))
	})
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// Refused before it applied anything: the lease expired.
			w.leaseAt = time.Time{}
			return nil
		}
		o.pending = p
		w.l.stats.count(w.l.stats.Ambiguous, opNames[op])
		return w.lost(opNames[op], err)
	}
	rev := resp.Header.Revision
	if !resp.Succeeded {
		kvs := resp.Responses[0].GetResponseRange().Kvs
		var kv *mvccpb.KeyValue
		if len(kvs) > 0 {
			kv = kvs[0]
		}
		state := describeState(o)
		gone, err := w.vanished(o, kv, rev, "the "+opNames[op]+" that found its compare false")
		if err != nil || gone {
			return err
		}
		return w.violate("the %s of %s compared false at revision %d, but the object is %s", opNames[op], o.key, rev, state)
	}
	o.acked = append(o.acked, change{lo: rev, hi: rev, val: p.val})
	o.present, o.val, o.mod, o.leased, o.vanishAt = true, p.val, rev, p.leased, p.vanishAt
	return nil
}

// delete deletes o if its mod revision is the one the worker knows.
func (w *worker) delete(ctx context.Context, o *object) error {
	p := &write{del: true, after: w.l.acked.Load()}
	resp, err := w.txn(ctx, "delete", func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.ModRevision(o.key), "=", o.mod)).Then(clientv3.OpDelete(o.key)).Else(clientv3.OpGet(o.key))
	})
	if err != nil {
		o.pending = p
		w.l.stats.count(w.l.stats.Ambiguous, "delete")
		return w.lost("delete", err)
	}
	rev := resp.Header.Revision
	if !resp.Succeeded {
		kvs := resp.Responses[0].GetResponseRange().Kvs
		var kv *mvccpb.KeyValue
		if len(kvs) > 0 {
			kv = kvs[0]
		}
		state := describeState(o)
		gone, err := w.vanished(o, kv, rev, "the delete that found its compare false")
		if err != nil || gone {
			return err
		}
		return w.violate("the delete of %s compared false at revision %d, but the object is %s", o.key, rev, state)
	}
	if n := resp.Responses[0].GetResponseDeleteRange().Deleted; n != 1 {
		return w.violate("the delete of %s at revision %d deleted %d keys", o.key, rev, n)
	}
	o.acked = append(o.acked, change{lo: rev, hi: rev, del: true})
	o.present, o.val, o.mod = false, 0, 0
	return nil
}

// list reads one page of g's objects, as a paginated list of the
// apiserver does, and checks it holds exactly the objects the worker
// knows in its key range.
func (w *worker) list(ctx context.Context, g *group) error {
	for _, o := range g.objects {
		if err := w.resolve(ctx, o); err != nil {
			return w.lost("list", err)
		}
	}
	end := clientv3.GetPrefixRangeEnd(g.prefix)
	r, err := w.read(ctx, "list", g.prefix, clientv3.WithRange(end), clientv3.WithLimit(500))
	if err != nil {
		return w.lost("list", err)
	}
	rev := r.Header.Revision
	got := map[string]*mvccpb.KeyValue{}
	last := ""
	for _, kv := range r.Kvs {
		got[string(kv.Key)] = kv
		last = string(kv.Key)
	}
	for _, o := range g.objects {
		if r.More && o.key > last {
			continue
		}
		if err := w.observe(o, got[o.key], rev, "a list"); err != nil {
			return err
		}
		delete(got, o.key)
	}
	for k := range got {
		return w.violate("a list of %s at revision %d shows %s, which no worker writes", g.prefix, rev, k)
	}
	return nil
}

// renewLease grants the lease new events go on once a minute.
func (w *worker) renewLease(ctx context.Context) error {
	if !w.leaseAt.IsZero() && time.Since(w.leaseAt) < leaseEvery {
		return nil
	}
	at := time.Now()
	cctx, cancel := context.WithTimeout(ctx, w.l.cfg.requestTimeout)
	defer cancel()
	w.l.stats.count(w.l.stats.Calls, "lease grant")
	resp, err := w.l.cl.Grant(cctx, leaseTTL)
	if err != nil {
		return w.lost("lease grant", err)
	}
	w.lease, w.leaseAt = resp.ID, at
	// A lease expires its TTL after the leader granted it, which is after
	// the call; a leader change only extends it.
	w.leaseVanish = at.Add(leaseTTL * time.Second)
	w.l.stats.mu.Lock()
	w.l.stats.Leases++
	w.l.stats.mu.Unlock()
	return nil
}

// preload creates every object of the worker in Txns of up to 64.
func (w *worker) preload(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.renewLease(ctx); err != nil {
		return err
	}
	var all []*object
	for _, g := range w.groups {
		all = append(all, g.objects...)
	}
	for len(all) > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := w.renewLease(ctx); err != nil {
			return err
		}
		batch := all[:min(64, len(all))]
		var cmps []clientv3.Cmp
		var puts []clientv3.Op
		var ps []*write
		for _, o := range batch {
			v := w.value(o)
			p := &write{val: hash(v), after: w.l.acked.Load()}
			var opts []clientv3.OpOption
			if o.leased {
				opts = append(opts, clientv3.WithLease(w.lease))
				p.leased, p.vanishAt = true, w.leaseVanish
			}
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(o.key), "=", 0))
			puts = append(puts, clientv3.OpPut(o.key, string(v), opts...))
			ps = append(ps, p)
		}
		resp, err := w.txn(ctx, "preload", func(t clientv3.Txn) clientv3.Txn { return t.If(cmps...).Then(puts...) })
		if err != nil {
			if errors.Is(err, rpctypes.ErrLeaseNotFound) {
				w.leaseAt = time.Time{}
				continue
			}
			for k, o := range batch {
				o.pending = ps[k]
			}
			if err := w.lost("preload", err); err != nil {
				return err
			}
			for _, o := range batch {
				for o.pending != nil {
					if err := w.resolve(ctx, o); err != nil {
						if err := w.lost("get", err); err != nil {
							return err
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
			all = all[len(batch):]
			continue
		}
		if !resp.Succeeded {
			return w.violate("a preload Txn of %d new objects compared false at revision %d", len(batch), resp.Header.Revision)
		}
		rev := resp.Header.Revision
		for k, o := range batch {
			p := ps[k]
			o.acked = append(o.acked, change{lo: rev, hi: rev, val: p.val})
			o.present, o.val, o.mod, o.leased, o.vanishAt = true, p.val, rev, p.leased, p.vanishAt
		}
		all = all[len(batch):]
	}
	return nil
}

// run makes calls at the worker's share of the rate until ctx ends.
func (w *worker) run(ctx context.Context) error {
	every := time.Duration(float64(time.Second) * float64(w.l.cfg.workers) / w.l.cfg.rate)
	next := time.Now()
	for ctx.Err() == nil {
		if err := w.step(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		next = next.Add(every)
		if d := time.Until(next); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		} else {
			next = time.Now()
		}
	}
	return nil
}

// settle resolves every pending write and reads every object once, so
// the model knows the final state, which the last watch check needs.
func (w *worker) settle(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, g := range w.groups {
		for _, o := range g.objects {
			for o.pending != nil {
				if err := w.resolve(ctx, o); err != nil {
					if ctx.Err() != nil {
						return err
					}
					if err := w.lost("get", err); err != nil {
						return err
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
		end := clientv3.GetPrefixRangeEnd(g.prefix)
		from := g.prefix
		var rev int64
		got := map[string]*mvccpb.KeyValue{}
		for {
			opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(1000)}
			if rev > 0 {
				opts = append(opts, clientv3.WithRev(rev))
			}
			var r *clientv3.GetResponse
			var err error
			for try := 0; ; try++ {
				if r, err = w.read(ctx, "list", from, opts...); err == nil {
					break
				}
				if _, ok := errors.AsType[*violation](err); ok || ctx.Err() != nil || try == 30 {
					return fmt.Errorf("final list of %s: %w", g.prefix, err)
				}
				time.Sleep(time.Second)
			}
			if rev == 0 {
				rev = r.Header.Revision
			}
			for _, kv := range r.Kvs {
				got[string(kv.Key)] = kv
			}
			if !r.More || len(r.Kvs) == 0 {
				break
			}
			from = string(r.Kvs[len(r.Kvs)-1].Key) + "\x00"
		}
		for _, o := range g.objects {
			if err := w.observe(o, got[o.key], rev, "the final list"); err != nil {
				return err
			}
			delete(got, o.key)
		}
		for k := range got {
			return w.violate("the final list of %s at revision %d shows %s, which no worker writes", g.prefix, rev, k)
		}
	}
	return nil
}
