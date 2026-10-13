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
	// start begins one request on c for st, or returns false if it has
	// nothing to send right now.
	start(c *conn, st *stream) bool
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
	resp              hist  // latency from the intended send time
	service           hist  // latency from the actual send time
	errs              map[string]int64
}

// complete counts the end of a unary call. A success needs exactly one
// response message.
func (s *stats) complete(st *stream, at int64, err error) {
	if err == nil && st.msgs != 1 {
		err = fmt.Errorf("%d response messages to a unary call", st.msgs)
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
	}
	if inCohort {
		s.resp.record(at - st.intended)
		s.service.record(at - st.sent)
	}
}

// base is what every request load shares.
type base struct {
	tpl   *template
	ks    keys
	own   []int // the keys this connection uses
	vals  values
	rng   *rand.Rand
	stats *stats
	limit int // streams in flight on this connection
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
	copy(v, b.vals.next())
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

func (w *putLoad) start(c *conn, st *stream) bool {
	if w.exhausted() || !c.canStart(w.tpl, w.limit) {
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
	v := w.fill(c.startStream(st, w.tpl), k)
	if w.stamp {
		binary.LittleEndian.PutUint64(v, uint64(st.intended))
	}
	return true
}

func (w *putLoad) exhausted() bool { return w.once && w.next == len(w.own) }

func (w *putLoad) message(st *stream, m []byte) error {
	rev, err := responseRevision(m)
	if err != nil {
		return err
	}
	if w.revs != nil {
		w.revs[st.slot] = rev
	}
	return nil
}

var errCASFailed = errors.New("CAS txn did not succeed")

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

func (w *casLoad) start(c *conn, st *stream) bool {
	if w.n == 0 || !c.canStart(w.tpl, w.limit) {
		return false
	}
	k := w.ring[w.head]
	w.head = (w.head + 1) % len(w.ring)
	w.n--
	st.slot = k
	r := c.startStream(st, w.tpl)
	w.fill(r, k)
	patchRevision(r[w.tpl.rev:], w.revs[k])
	return true
}

func (w *casLoad) exhausted() bool { return false }

func (w *casLoad) message(st *stream, m []byte) error {
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
	w.revs[st.slot] = rev
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

func (w *deleteLoad) start(c *conn, st *stream) bool {
	if w.exhausted() || !c.canStart(w.tpl, w.limit) {
		return false
	}
	k := w.own[w.next]
	w.next++
	st.slot = k
	w.fill(c.startStream(st, w.tpl), k)
	return true
}

func (w *deleteLoad) exhausted() bool { return w.next == len(w.own) }

func (w *deleteLoad) message(_ *stream, m []byte) error {
	n, err := varintField(m, delDeleted)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("delete removed %d keys, want 1", n)
	}
	return nil
}

// rangeLoad reads single keys with linearizable Range.
type rangeLoad struct {
	base
	valueSize int
}

func (w *rangeLoad) start(c *conn, st *stream) bool {
	if !c.canStart(w.tpl, w.limit) {
		return false
	}
	k := w.own[w.rng.IntN(len(w.own))]
	st.slot = k
	w.fill(c.startStream(st, w.tpl), k)
	return true
}

func (w *rangeLoad) exhausted() bool { return false }

func (w *rangeLoad) message(_ *stream, m []byte) error {
	var count uint64
	kvs := 0
	err := fields(m, func(num protowire.Number, v uint64, b []byte) error {
		switch num {
		case rangeCount:
			count = v
		case rangeKVs:
			kvs++
			val, err := bytesField(b, kvValue)
			if err != nil {
				return err
			}
			if len(val) != w.valueSize {
				return fmt.Errorf("range returned a %d byte value, want %d", len(val), w.valueSize)
			}
		default:
		}
		return nil
	})
	if err != nil {
		return err
	}
	if count != 1 || kvs != 1 {
		return fmt.Errorf("range returned count %d with %d kvs, want 1", count, kvs)
	}
	return nil
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
	lastRev int64
	events  int64
}

// message checks one watch response. A watch never ends, so it counts its
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
	var kv []byte
	err := fields(e, func(num protowire.Number, v uint64, b []byte) error {
		switch num {
		case eventType:
			if v == eventTypeDelete {
				return errors.New("unexpected delete event")
			}
		case eventKV:
			kv = b
		default:
		}
		return nil
	})
	if err != nil {
		return err
	}
	var rev int64
	var val []byte
	err = fields(kv, func(num protowire.Number, v uint64, b []byte) error {
		switch num {
		case kvModRevision:
			rev = int64(v)
		case kvValue:
			val = b
		default:
		}
		return nil
	})
	if err != nil {
		return err
	}
	if rev <= ws.lastRev {
		return fmt.Errorf("watch event at revision %d after %d", rev, ws.lastRev)
	}
	if len(val) < 8 {
		return errors.New("watch event value too short for a timestamp")
	}
	ws.lastRev = rev
	ws.events++
	if at >= w.stats.winStart && at < w.stats.winStop {
		w.events++
		w.lat.record(at - int64(binary.LittleEndian.Uint64(val)))
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
