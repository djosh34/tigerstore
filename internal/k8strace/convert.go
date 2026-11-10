package k8strace

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
)

// Window is the part of a log that becomes the trace's calls: the calls
// sent in [Start, End), in ns since the Unix epoch.
type Window struct {
	Start, End int64
}

// Dropped counts what a conversion left out of the trace, by reason.
type Dropped map[string]int64

// errBadLog is a log the converter cannot read.
var errBadLog = errors.New("bad log")

// Convert reads a gzipped log and builds the trace of its window.
//
// The keys, values and leases alive when the window opened become the
// preload: every write sent before the window counts, answered before it
// or not, and a lease whose TTL ran out before the window is gone, with
// its keys. A watch that kube-apiserver cancelled and opened again is one
// stream. Every call sent in the window becomes a Call, in the order they
// were sent. A revision a call names is replaced by a reference to the
// reply that gave it, and the call waits on the reply it learned that
// revision from: a reply that carried the key at that revision, or, for
// a watch event, the reply to the write that made it. A lease is replaced
// by its grant, which the call waits on.
func Convert(r io.Reader, w Window) (*Trace, Dropped, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, nil, err
	}
	c := newConverter(w)
	dec := json.NewDecoder(gz)
	for {
		var rec Record
		err := dec.Decode(&rec)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", errBadLog, err)
		}
		if err := c.add(&rec); err != nil {
			return nil, nil, err
		}
	}
	return c.finish(), c.dropped, nil
}

// revKey is a key at a revision.
type revKey struct {
	key string
	rev int64
}

// delivery is where a client learned a key's revision: the reply to a
// window call (call >= 0), or a watch event (call -1), or something
// before the window (call -2).
type delivery struct {
	call int32
}

const (
	deliveredByWatch       = -1
	deliveredEarlier       = -2
	notInWindow      int32 = -1
)

type call struct {
	method  string
	index   int32 // in the trace, or notInWindow
	reqs    []*Request
	resps   []*Response
	start   int64
	sent    int64 // the window call's request
	touches []touch
}

// fixup is a revision of a window call to resolve once the whole log is
// read: the reply that made key at rev.
type fixup struct {
	at    *Rev
	owner int32
	key   string
	rev   int64
}

// lease is a lease granted before the window: its TTL in seconds, and
// when it was last granted or kept alive, in ns since the Unix epoch.
type lease struct {
	ttl, at int64
}

type progressCall struct {
	index  int32
	stream int64
}

type stateKV struct {
	mod, create int64
	size        int
	lease       int64
}

type watchStream struct {
	creates  []watchCreate
	inWindow bool // open when the window opened, or opened in it
	open     bool
}

type watchCreate struct {
	WatchCreate
	id       int64 // -1 until the store answers that it created it
	canceled bool  // before the window opened
}

type converter struct {
	w       Window
	dropped Dropped
	calls   map[int64]*call
	ended   map[int64]bool
	trace   []*Call
	keys    map[string]int32
	keyList []string
	// Before the window: the store's keys and leases.
	state  map[string]stateKV
	leases map[int64]lease
	// What clients have learned so far, and who made each revision.
	seen      map[revKey]delivery
	headers   map[int64]int32 // revision: the last window call whose reply had it as its header
	producers map[revKey]int32
	grants    map[int64]int32 // lease ID: the window call that granted it
	// By key, the window calls that had answered and wrote it, and those
	// that only read it, each but those another of them came after. A
	// call waits on every earlier call on its key that had answered before
	// it was sent, if either writes: conflicts on a key keep the order
	// they had, and calls that overlapped stay free to race. These lists
	// hold the latest such calls; the waits of those reach the rest.
	writes  map[string][]answered
	reads   map[string][]answered
	fixups  []fixup
	watches map[int64]*watchStream
	started bool
	// The window's progress requests and the watch calls they went on.
	progress []progressCall
	// The leases alive when the window opened, in the order the trace
	// names them.
	leaseOrder []int64
}

func newConverter(w Window) *converter {
	return &converter{
		w: w, dropped: Dropped{}, calls: map[int64]*call{}, ended: map[int64]bool{}, keys: map[string]int32{},
		state: map[string]stateKV{}, leases: map[int64]lease{},
		seen: map[revKey]delivery{}, headers: map[int64]int32{}, producers: map[revKey]int32{},
		grants: map[int64]int32{}, watches: map[int64]*watchStream{},
		writes: map[string][]answered{}, reads: map[string][]answered{},
	}
}

func (c *converter) key(k string) int32 {
	if i, ok := c.keys[k]; ok {
		return i
	}
	i := int32(len(c.keyList))
	c.keys[k] = i
	c.keyList = append(c.keyList, k)
	return i
}

func (c *converter) end(e string) int32 {
	if e == "" {
		return -1
	}
	return c.key(e)
}

func (c *converter) inWindow(t int64) bool { return t >= c.w.Start && t < c.w.End }

// at is a call's time in the window, in whole µs as a trace keeps it.
func (c *converter) at(t int64) int64 { return (t - c.w.Start) / 1000 * 1000 }

func (c *converter) add(r *Record) error {
	if r.Err != "" && r.Event != EventEnd {
		return fmt.Errorf("%w: call %d: %s", errBadLog, r.Call, r.Err)
	}
	switch r.Event {
	case EventOpen:
		c.calls[r.Call] = &call{method: r.Method, index: notInWindow, start: r.T}
		if r.Method == MethodWatch {
			c.watches[r.Call] = &watchStream{open: true, inWindow: c.inWindow(r.T)}
		}
		return nil
	case EventEnd:
		cl := c.calls[r.Call]
		if cl == nil {
			return fmt.Errorf("%w: end of call %d before its open", errBadLog, r.Call)
		}
		if r.Err != "" && cl.index != notInWindow {
			c.dropped["ended with an error: "+r.Err]++
		}
		if ws := c.watches[r.Call]; ws != nil {
			ws.open = false
		}
		delete(c.calls, r.Call)
		c.ended[r.Call] = true
		return nil
	}
	cl := c.calls[r.Call]
	if cl == nil && c.ended[r.Call] {
		// A request the client sent while its call was already ending.
		return nil
	}
	if cl == nil {
		return fmt.Errorf("%w: message of call %d before its open", errBadLog, r.Call)
	}
	if c.inWindow(r.T) {
		if !c.started {
			c.started = true
			for _, ws := range c.watches {
				ws.inWindow = ws.inWindow || ws.open
			}
			c.expire()
		}
		if ws := c.watches[r.Call]; ws != nil {
			ws.inWindow = true
		}
	}
	switch r.Event {
	case EventReq:
		return c.request(r, cl)
	case EventResp:
		c.response(r, cl)
		return nil
	}
	return fmt.Errorf("%w: event %q", errBadLog, r.Event)
}

var kinds = map[string]uint8{
	MethodRange: CallRange, MethodRangeStream: CallRangeStream, MethodPut: CallPut, MethodDelete: CallDelete,
	MethodTxn: CallTxn, MethodCompact: CallCompact, MethodLeaseGrant: CallLeaseGrant, MethodLeaseRevoke: CallLeaseRevoke,
	MethodLeaseKeepAlive: CallLeaseKeepAlive, MethodLeaseTTL: CallLeaseTTL,
}

func (c *converter) request(r *Record, cl *call) error {
	cl.reqs = append(cl.reqs, r.Req)
	if cl.method == MethodWatch {
		c.watchRequest(r, c.watches[r.Call])
		return nil
	}
	kind, ok := kinds[cl.method]
	if !ok {
		if c.inWindow(r.T) {
			c.dropped["method "+cl.method]++
		}
		return nil
	}
	if r.Req == nil {
		return fmt.Errorf("%w: call %d: %s without a request", errBadLog, r.Call, cl.method)
	}
	if !c.inWindow(r.T) {
		return nil
	}
	if kind == CallLeaseKeepAlive && len(cl.reqs) > 1 {
		// Each keepalive on the stream is a call of its own.
		cl.reqs = cl.reqs[len(cl.reqs)-1:]
		cl.resps = nil
	}
	cl.index = int32(len(c.trace))
	cl.sent = r.T
	t := &Call{At: c.at(r.T), Kind: kind}
	c.trace = append(c.trace, t)
	q := r.Req
	switch {
	case q.Range != nil:
		if !c.rangeOp(t, q.Range, &t.Op) {
			return c.drop(cl, "range with a sort or revision filter")
		}
	case q.Put != nil:
		c.putOp(t, q.Put, &t.Op)
	case q.Delete != nil:
		c.deleteOp(q.Delete, &t.Op)
	case q.Txn != nil:
		if !c.txnOp(t, q.Txn, &t.Op) {
			return c.drop(cl, "txn with a sort or revision filter")
		}
	case q.Compact != nil:
		c.headerRev(t, q.Compact.Rev, &t.Rev)
	case q.LeaseGrant != nil:
		if q.LeaseGrant.ID != 0 {
			return c.drop(cl, "lease grant with its own ID")
		}
		t.TTL = q.LeaseGrant.TTL
	case q.Lease != nil:
		t.Lease = c.leaseRef(t, *q.Lease)
	default:
		return fmt.Errorf("%w: call %d: %s with no request", errBadLog, r.Call, cl.method)
	}
	cl.touches = touches(q, cl.touches[:0])
	for _, k := range cl.touches {
		for _, w := range c.writes[k.key] {
			c.wait(t, w.call)
		}
		if k.write {
			for _, r := range c.reads[k.key] {
				c.wait(t, r.call)
			}
		}
	}
	return nil
}

// answered is a window call that has answered, and when it was sent.
type answered struct {
	call int32
	sent int64
	at   int64
}

// answer takes in that window call cl, sent at sent, answered at at: a
// write takes the place of the calls on its keys that had answered before
// it was sent, which it waits on; a read joins the reads.
func (c *converter) answer(cl *call, at int64) {
	a := answered{call: cl.index, sent: cl.sent, at: at}
	after := func(l []answered) []answered {
		return slices.DeleteFunc(l, func(x answered) bool { return x.at < a.sent })
	}
	for _, k := range cl.touches {
		if k.write {
			c.writes[k.key] = append(after(c.writes[k.key]), a)
			c.reads[k.key] = after(c.reads[k.key])
		} else {
			c.reads[k.key] = append(c.reads[k.key], a)
		}
	}
}

// touch is a key a call reads or writes.
type touch struct {
	key   string
	write bool
}

// touches appends the single keys request q reads or writes: what a
// Range, Put or Delete names, and a Txn's compares and the ops of both its
// branches. A key a Txn may put or delete in either branch counts as
// written. Ranges over many keys are left out.
func touches(q *Request, out []touch) []touch {
	add := func(k string, write bool) {
		for i := range out {
			if out[i].key == k {
				out[i].write = out[i].write || write
				return
			}
		}
		out = append(out, touch{k, write})
	}
	var op func(o *LogOp)
	var txn func(t *Txn)
	op = func(o *LogOp) {
		switch {
		case o.Range != nil && o.Range.End == "":
			add(o.Range.Key, false)
		case o.Put != nil:
			add(o.Put.Key, true)
		case o.Delete != nil && o.Delete.End == "":
			add(o.Delete.Key, true)
		case o.Txn != nil:
			txn(o.Txn)
		}
	}
	txn = func(t *Txn) {
		for _, c := range t.Compare {
			if c.End == "" {
				add(c.Key, false)
			}
		}
		for i := range t.Success {
			op(&t.Success[i])
		}
		for i := range t.Failure {
			op(&t.Failure[i])
		}
	}
	switch {
	case q.Txn != nil:
		txn(q.Txn)
	default:
		op(&LogOp{Range: q.Range, Put: q.Put, Delete: q.Delete})
	}
	return out
}

// drop takes the call just added out of the trace again.
func (c *converter) drop(cl *call, reason string) error {
	c.trace = c.trace[:len(c.trace)-1]
	c.fixups = slices.DeleteFunc(c.fixups, func(f fixup) bool { return f.owner == int32(len(c.trace)) })
	cl.index = notInWindow
	c.dropped[reason]++
	return nil
}

func (c *converter) owner() int32 { return int32(len(c.trace) - 1) }

func (c *converter) wait(t *Call, on int32) {
	if on >= 0 && on != c.owner() && !slices.Contains(t.Waits, on) {
		t.Waits = append(t.Waits, on)
	}
}

func (c *converter) rangeOp(t *Call, q *Range, op *Op) bool {
	if q.SortOrder != 0 || q.SortTarget != 0 || q.MinMod != 0 || q.MaxMod != 0 || q.MinCreate != 0 || q.MaxCreate != 0 {
		return false
	}
	*op = Op{Kind: OpRange, Key: c.key(q.Key), End: c.end(q.End), Limit: q.Limit}
	if q.Serializable {
		op.Flags |= FlagSerializable
	}
	if q.KeysOnly {
		op.Flags |= FlagKeysOnly
	}
	if q.CountOnly {
		op.Flags |= FlagCountOnly
	}
	c.headerRev(t, q.Rev, &op.Rev)
	return true
}

// headerRev resolves a revision a client read from a reply's header, such
// as the revision of a LIST's first page that its next pages read at.
func (c *converter) headerRev(t *Call, rev int64, at *Rev) {
	if rev == 0 {
		return
	}
	if d, ok := c.headers[rev]; ok {
		*at = Rev(d + 1)
		c.wait(t, d)
		return
	}
	// A revision from before the window reads at the newest revision.
	c.dropped["revision from before the window, read at the newest"]++
}

func (c *converter) putOp(t *Call, q *Put, op *Op) {
	*op = Op{Kind: OpPut, Key: c.key(q.Key), End: -1, Size: int32(q.Size)}
	if q.Lease != 0 {
		op.Lease = c.leaseRef(t, q.Lease)
	}
	if q.PrevKV {
		op.Flags |= FlagPrevKV
	}
	if q.IgnoreValue {
		op.Flags |= FlagIgnoreValue
	}
	if q.IgnoreLease {
		op.Flags |= FlagIgnoreLease
	}
}

func (c *converter) deleteOp(q *Delete, op *Op) {
	*op = Op{Kind: OpDelete, Key: c.key(q.Key), End: c.end(q.End)}
	if q.PrevKV {
		op.Flags |= FlagPrevKV
	}
}

func (c *converter) txnOp(t *Call, q *Txn, op *Op) bool {
	*op = Op{Kind: OpTxn, End: -1}
	for _, x := range q.Compare {
		cmp := Compare{Target: x.Target, Result: x.Result, Key: c.key(x.Key), End: c.end(x.End), Value: x.Value}
		op.Compare = append(op.Compare, cmp)
	}
	// The compares' revisions point into op.Compare, which no longer grows.
	for i, x := range q.Compare {
		if (x.Target == TargetMod || x.Target == TargetCreate) && x.Value != 0 {
			op.Compare[i].Value = 0
			c.learned(t, x.Key, x.Value, &op.Compare[i].Rev)
		}
		if x.Target == TargetLease && x.Value != 0 {
			op.Compare[i].Value = int64(c.leaseRef(t, x.Value))
		}
	}
	var ok bool
	if op.Success, ok = c.ops(t, q.Success); !ok {
		return false
	}
	op.Failure, ok = c.ops(t, q.Failure)
	return ok
}

func (c *converter) ops(t *Call, in []LogOp) ([]Op, bool) {
	if len(in) == 0 {
		return nil, true
	}
	out := make([]Op, len(in))
	for i, o := range in {
		switch {
		case o.Range != nil:
			if !c.rangeOp(t, o.Range, &out[i]) {
				return nil, false
			}
		case o.Put != nil:
			c.putOp(t, o.Put, &out[i])
		case o.Delete != nil:
			c.deleteOp(o.Delete, &out[i])
		case o.Txn != nil:
			if !c.txnOp(t, o.Txn, &out[i]) {
				return nil, false
			}
		}
	}
	return out, true
}

// learned resolves a key's mod or create revision that the call names: it
// waits on the reply the client learned it from, and, once the whole log
// is read, on the write that made it, whose reply gives its value.
func (c *converter) learned(t *Call, key string, rev int64, at *Rev) {
	if d, ok := c.seen[revKey{key, rev}]; ok && d.call >= 0 {
		c.wait(t, d.call)
	}
	c.fixups = append(c.fixups, fixup{at: at, owner: c.owner(), key: key, rev: rev})
}

func (c *converter) leaseRef(t *Call, id int64) LeaseRef {
	if g, ok := c.grants[id]; ok {
		c.wait(t, g)
		return LeaseRef(g + 1)
	}
	if _, ok := c.leases[id]; ok {
		return c.preloadLease(id)
	}
	c.dropped["lease unknown when the window opened, left out"]++
	return 0
}

// expire drops the leases whose TTL ran out before the window opened, and
// the keys the store deleted with them.
func (c *converter) expire() {
	for id, l := range c.leases {
		if l.at+l.ttl*1e9 >= c.w.Start {
			continue
		}
		delete(c.leases, id)
		for k, kv := range c.state {
			if kv.lease == id {
				delete(c.state, k)
			}
		}
	}
}

// preloadLease numbers the leases alive when the window opened in the
// order the trace first uses them; finish adds the rest.
func (c *converter) preloadLease(id int64) LeaseRef {
	for i, l := range c.leaseOrder {
		if l == id {
			return LeaseRef(-i - 1)
		}
	}
	c.leaseOrder = append(c.leaseOrder, id)
	return LeaseRef(-len(c.leaseOrder))
}
