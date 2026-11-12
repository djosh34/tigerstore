package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"

	"google.golang.org/protobuf/encoding/protowire"
)

// load is the request side of a workload on one connection. Its methods run
// with conn.mu held.
type load interface {
	handler
	// start begins one request on c for st, with at most limit requests
	// in flight on c, or returns false if it has nothing to send now.
	start(c *conn, st *stream, limit int) bool
	// exhausted reports that the load will never send again.
	exhausted() bool
}

// stats collects the completions of one connection.
type stats struct {
	// The window is [winStart, winStop). Throughput counts the successful
	// completions inside it. Latency counts the requests that arrived
	// inside it, however late they complete, so a slow tail is not cut
	// off at the end of the window.
	winStart, winStop int64
	ops               int64 // successful completions in the window
	acked             int64 // successful completions in the whole phase
	cohort            int64 // requests that arrived in the window, by outcome below
	cohortFailed      int64 // of those, the ones that failed
	// written sums stream.written over the successful completions in the
	// window.
	written int64
	c       counts
	// events counts the watch events that came in the window, in k8s.
	events  int64
	resp    hist // latency from the intended send time
	service hist // latency from the actual send time
	errs    map[string]int64
}

// messages is the error of a call that ended without error but with too
// few or too many response messages: none, or several to a unary call.
func messages(st *stream) error {
	if st.msgs == 0 || st.msgs > 1 && !st.many {
		return fmt.Errorf("%d response messages to a unary call", st.msgs)
	}
	return nil
}

// complete counts the end of a call. A success needs exactly one response
// message, or at least one on a stream that may send many (messages).
func (s *stats) complete(st *stream, at int64, err error) {
	if err == nil {
		err = messages(st)
	}
	inCohort := st.intended >= s.winStart && st.intended < s.winStop
	if inCohort {
		s.cohort++
	}
	if err != nil {
		s.errs[err.Error()]++
		if inCohort {
			s.cohortFailed++
		}
		return
	}
	s.acked++
	if at >= s.winStart && at < s.winStop {
		s.ops++
		s.written += st.written
		s.c.add(&st.c)
	}
	if inCohort {
		s.resp.record(at - st.intended)
		s.service.record(at - st.sent)
	}
}

// counts are what calls carried, for the CPU floor of cmd/benchreport.
// Out and In are the gRPC message bytes they sent and got, watch events
// included. KVs are the key-values that reads answered with, PrevKVs the
// previous key-values that writes answered with, and Events the
// key-values of watch events, previous ones included. Writes are the keys
// the calls put or deleted, of which Creates were new and Deletes
// deleted. Entries are the calls that made something durable, a log
// entry each. Checks are the keys that compares looked up and no op of
// the call touched. Leases are the lease calls, and the puts that attach
// a key to a lease: each looks up the lease.
type counts struct {
	Out, In                  int64
	KVs, PrevKVs, Events     int64
	Writes, Creates, Deletes int64
	Entries, Checks, Leases  int64
}

func (c *counts) add(o *counts) {
	c.Out += o.Out
	c.In += o.In
	c.KVs += o.KVs
	c.PrevKVs += o.PrevKVs
	c.Events += o.Events
	c.Writes += o.Writes
	c.Creates += o.Creates
	c.Deletes += o.Deletes
	c.Entries += o.Entries
	c.Checks += o.Checks
	c.Leases += o.Leases
}

// base is what every request load shares.
type base struct {
	tpl   *template
	ks    keys
	own   []int // the keys this connection uses
	vals  values
	rng   *rand.Rand
	stats *stats
	last  []byte // the pool value that fill copied last
}

func (b *base) done(st *stream, at int64, err error) { b.stats.complete(st, at, err) }

// fill copies key k and the next value into request r.
func (b *base) fill(r []byte, k int) []byte {
	for _, off := range b.tpl.keys {
		copy(r[off:], b.ks.key(k))
	}
	if b.tpl.value < 0 {
		return nil
	}
	v := r[b.tpl.value : b.tpl.value+b.vals.size]
	b.last = b.vals.next()
	copy(v, b.last)
	return v
}

// putLoad overwrites keys with Put.
type putLoad struct {
	base
	once  bool    // put each key once, in order, then stop
	next  int     // with once: the next key
	revs  []int64 // if set: the mod revision of each key after its put
	stamp bool    // write the intended send time into the value
}

func (w *putLoad) start(c *conn, st *stream, limit int) bool {
	if w.exhausted() || !c.canStart(w.tpl.dataLen, limit) {
		return false
	}
	var k int
	if w.once {
		k = w.own[w.next]
		w.next++
	} else {
		k = w.own[w.rng.IntN(len(w.own))]
	}
	st.slot = k
	st.written = int64(w.ks.size + w.vals.size)
	v := w.fill(c.startStream(st, w.tpl), k)
	if w.stamp {
		binary.LittleEndian.PutUint64(v, uint64(st.intended))
	}
	return true
}

func (w *putLoad) exhausted() bool { return w.once && w.next == len(w.own) }

func (w *putLoad) message(st *stream, m []byte) error {
	st.c.Writes, st.c.Entries = 1, 1
	if w.revs == nil {
		return nil
	}
	rev, err := responseRevision(m)
	w.revs[st.slot] = rev
	return err
}

var errCASFailed = errors.New("CAS txn did not succeed")

// revisionBytes is what a compare on a revision makes durable besides its
// key: the revision as a fixed 8-byte integer.
const revisionBytes = 8

// casLoad updates keys with the compare-and-swap Txn of Kubernetes. A key is
// never in flight twice, and each update compares against the mod revision
// of the previous one, so every Txn must succeed.
type casLoad struct {
	base
	revs []int64
	ring []int // own keys not in flight, oldest first
	head int
	n    int
}

func newCASLoad(b base, revs []int64) *casLoad {
	w := &casLoad{base: b, revs: revs, ring: make([]int, len(b.own)), n: len(b.own)}
	copy(w.ring, b.own)
	return w
}

func (w *casLoad) start(c *conn, st *stream, limit int) bool {
	if w.n == 0 || !c.canStart(w.tpl.dataLen, limit) {
		return false
	}
	k := w.ring[w.head]
	w.head = (w.head + 1) % len(w.ring)
	w.n--
	st.slot = k
	// The compare's key and revision, and the put's key and value.
	st.written = int64(2*w.ks.size + revisionBytes + w.vals.size)
	r := c.startStream(st, w.tpl)
	w.fill(r, k)
	patchRevision(r[w.tpl.rev:], w.revs[k])
	return true
}

func (w *casLoad) exhausted() bool { return false }

// message keeps the revision of the put for the next compare. A Txn that
// did not succeed means the generator lost track of its keys.
func (w *casLoad) message(st *stream, m []byte) error {
	rev, ok, err := txnOutcome(m)
	if err != nil {
		return err
	}
	if !ok {
		return errCASFailed
	}
	w.revs[st.slot] = rev
	st.c.Writes, st.c.Entries = 1, 1
	return nil
}

func (w *casLoad) done(st *stream, at int64, err error) {
	w.ring[(w.head+w.n)%len(w.ring)] = st.slot
	w.n++
	w.stats.complete(st, at, err)
}

// deleteLoad deletes each key once, in order.
type deleteLoad struct {
	base
	next int
}

func (w *deleteLoad) start(c *conn, st *stream, limit int) bool {
	if w.exhausted() || !c.canStart(w.tpl.dataLen, limit) {
		return false
	}
	k := w.own[w.next]
	w.next++
	st.slot = k
	st.written = int64(w.ks.size)
	w.fill(c.startStream(st, w.tpl), k)
	return true
}

func (w *deleteLoad) exhausted() bool { return w.next == len(w.own) }

func (w *deleteLoad) message(st *stream, _ []byte) error {
	st.c.Writes, st.c.Deletes, st.c.Entries = 1, 1, 1
	return nil
}

// rangeLoad reads single keys with linearizable Range.
type rangeLoad struct{ base }

func (w *rangeLoad) start(c *conn, st *stream, limit int) bool {
	if !c.canStart(w.tpl.dataLen, limit) {
		return false
	}
	k := w.own[w.rng.IntN(len(w.own))]
	st.slot = k
	w.fill(c.startStream(st, w.tpl), k)
	return true
}

func (w *rangeLoad) exhausted() bool { return false }

func (w *rangeLoad) message(st *stream, m []byte) error {
	kvs, err := rangeKVCount(m)
	st.c.KVs += kvs
	return err
}

// watchLoad holds the watch streams of one connection. Each event's value
// starts with the time its put was meant to be sent, which gives the delay
// from put to event.
type watchLoad struct {
	tpl     *template
	streams []watchStream
	lat     hist
	events  int64 // events received in the stats window
	stats   *stats
}

type watchStream struct {
	created bool
	failed  bool
	events  int64
}

// message reads one watch response. A watch never ends, so it counts its
// first bad response itself and ignores the stream after that.
func (w *watchLoad) message(st *stream, m []byte) error {
	ws := &w.streams[st.slot]
	if ws.failed {
		return nil
	}
	if err := w.response(ws, m, now()); err != nil {
		ws.failed = true
		w.stats.errs["watch: "+err.Error()]++
	}
	return nil
}

func (w *watchLoad) response(ws *watchStream, m []byte, at int64) error {
	return fields(m, func(num protowire.Number, v uint64, b []byte) error {
		//exhaustive:ignore // the default case handles the rest
		switch num {
		case watchCreated:
			ws.created = v == 1
		case watchCanceled:
			if v == 1 {
				reason, _ := bytesField(m, watchCancelMsg)
				return fmt.Errorf("watch canceled: %s", reason)
			}
		case watchCompactRev:
			if v != 0 {
				return fmt.Errorf("watch compacted at revision %d", v)
			}
		case watchEvents:
			return w.event(ws, b, at)
		default:
		}
		return nil
	})
}

func (w *watchLoad) event(ws *watchStream, e []byte, at int64) error {
	kv, err := bytesField(e, eventKV)
	if err != nil {
		return err
	}
	val, err := bytesField(kv, kvValue)
	if err != nil {
		return err
	}
	if len(val) < 8 {
		return errors.New("watch event value too short for a timestamp")
	}
	ws.events++
	// Event throughput counts deliveries in the window; latency counts
	// every event of a put sent in the window, however late it arrives.
	if at >= w.stats.winStart && at < w.stats.winStop {
		w.events++
	}
	if sent := int64(binary.LittleEndian.Uint64(val)); sent >= w.stats.winStart && sent < w.stats.winStop {
		w.lat.record(at - sent)
	}
	return nil
}

// done is only called when a watch stream ends while the connection is
// open, which a watch never should.
func (w *watchLoad) done(_ *stream, _ int64, err error) {
	if err == nil {
		err = errors.New("watch stream ended")
	}
	w.stats.errs["watch: "+err.Error()]++
}

// listLoad lists the first page of one namespace, as kube-apiserver
// starts a list of a resource in a namespace: a range over the
// namespace's prefix with a limit. It does not page further.
type listLoad struct {
	base
	starts [][]byte // per namespace: its prefix
	ends   [][]byte // per namespace: the end of its range
}

func newListLoad(b base) *listLoad {
	w := &listLoad{base: b}
	for ns := range nsCount {
		p := listPrefix(ns)
		w.starts = append(w.starts, p)
		w.ends = append(w.ends, prefixEnd(p))
	}
	return w
}

func (w *listLoad) start(c *conn, st *stream, limit int) bool {
	if !c.canStart(w.tpl.dataLen, limit) {
		return false
	}
	ns := w.rng.IntN(nsCount)
	st.slot = ns
	r := c.startStream(st, w.tpl)
	copy(r[w.tpl.keys[0]:], w.starts[ns])
	copy(r[w.tpl.keys[1]:], w.ends[ns])
	return true
}

func (w *listLoad) exhausted() bool { return false }

func (w *listLoad) message(st *stream, m []byte) error {
	kvs, err := rangeKVCount(m)
	st.c.KVs += kvs
	return err
}
