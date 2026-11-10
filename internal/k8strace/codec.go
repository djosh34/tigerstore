package k8strace

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// A trace file is gzipped: the magic line, then the trace's fields in the
// order of Trace, each number a varint. A call's time, in µs, and its
// waits are deltas from the call before, a revision that names a call is
// the distance back to it, and a key the call named just before is 0,
// which keeps the numbers small.
const magic = "k8strace 1\n"

// errBadTrace is a trace file that does not decode.
var errBadTrace = errors.New("bad trace")

type enc struct {
	b   []byte
	key int32 // the key last written in this call
}

// k writes a key of a call's request.
func (e *enc) k(k int32) {
	if k == e.key {
		e.u(0)
		return
	}
	e.u(uint64(k) + 1)
	e.key = k
}

func (e *enc) u(v uint64)   { e.b = binary.AppendUvarint(e.b, v) }
func (e *enc) i(v int64)    { e.b = binary.AppendVarint(e.b, v) }
func (e *enc) s(v string)   { e.u(uint64(len(v))); e.b = append(e.b, v...) }
func (e *enc) end(v int32)  { e.u(uint64(v + 1)) }
func (e *enc) byte(v uint8) { e.b = append(e.b, v) }
func (e *enc) bool(v bool) {
	if v {
		e.byte(1)
	} else {
		e.byte(0)
	}
}
func (e *enc) count(n int)      { e.u(uint64(n)) }
func (e *enc) lease(l LeaseRef) { e.i(int64(l)) }

// rev writes r as seen from the call owner: 0 zero, 1 stale, 2+2d the
// reply of the call d before owner, 3+2i preload item i.
func (e *enc) rev(r Rev, owner int32) {
	switch {
	case r == 0:
		e.u(0)
	case r == RevStale:
		e.u(1)
	case r > 0:
		e.u(2 + 2*uint64(owner-int32(r-1)))
	default:
		e.u(3 + 2*uint64(-r-1))
	}
}

func (t *Trace) encode() []byte {
	e := &enc{b: []byte(magic)}
	e.s(t.Shape)
	e.s(t.About)
	e.u(math.Float64bits(t.Seconds))
	e.count(len(t.Keys))
	for _, k := range t.Keys {
		e.s(k)
	}
	e.count(len(t.Leases))
	for _, l := range t.Leases {
		e.i(l)
	}
	e.count(len(t.Preload))
	for _, it := range t.Preload {
		e.u(uint64(it.Key))
		e.u(uint64(it.Size))
		e.lease(it.Lease)
	}
	e.count(len(t.Watches))
	for _, ws := range t.Watches {
		e.count(len(ws.Creates))
		for _, w := range ws.Creates {
			e.u(uint64(w.Key))
			e.end(w.End)
			e.bool(w.PrevKV)
			e.bool(w.Progress)
			e.count(len(w.Filters))
			for _, f := range w.Filters {
				e.i(int64(f))
			}
		}
	}
	e.count(len(t.Calls))
	var at int64
	for i, c := range t.Calls {
		owner := int32(i)
		e.i(c.At/1000 - at)
		at = c.At / 1000
		e.key = -1
		e.byte(c.Kind)
		e.bool(c.Failed)
		e.count(len(c.Waits))
		for _, w := range c.Waits {
			e.u(uint64(owner - w))
		}
		switch c.Kind {
		case CallRange, CallRangeStream, CallPut, CallDelete, CallTxn:
			e.op(&c.Op, owner)
		case CallCompact:
			e.rev(c.Rev, owner)
		case CallLeaseGrant:
			e.i(c.TTL)
		case CallProgress:
			e.u(uint64(c.Stream))
		default:
			e.lease(c.Lease)
		}
	}
	return e.b
}

func (e *enc) op(o *Op, owner int32) {
	e.byte(o.Kind)
	e.byte(o.Flags)
	e.k(o.Key)
	e.end(o.End)
	switch o.Kind {
	case OpRange:
		e.i(o.Limit)
		e.rev(o.Rev, owner)
	case OpPut:
		e.u(uint64(o.Size))
		e.lease(o.Lease)
	case OpTxn:
		e.count(len(o.Compare))
		for _, c := range o.Compare {
			e.byte(uint8(c.Target))
			e.byte(uint8(c.Result))
			e.k(c.Key)
			e.end(c.End)
			e.rev(c.Rev, owner)
			e.i(c.Value)
		}
		e.count(len(o.Success))
		for i := range o.Success {
			e.op(&o.Success[i], owner)
		}
		e.count(len(o.Failure))
		for i := range o.Failure {
			e.op(&o.Failure[i], owner)
		}
	}
}

type dec struct {
	b   []byte
	err error
	key int32
}

func (d *dec) k() int32 {
	v := d.u()
	if v != 0 {
		d.key = int32(v - 1)
	}
	return d.key
}

func (d *dec) fail(what string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %s", errBadTrace, what)
	}
}

func (d *dec) u() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail("short varint")
		d.b = nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) i() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail("short varint")
		d.b = nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) byte() uint8 {
	if len(d.b) == 0 {
		d.fail("short byte")
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *dec) bool() bool { return d.byte() != 0 }

func (d *dec) s() string {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.fail("short string")
		d.b = nil
		return ""
	}
	v := string(d.b[:n])
	d.b = d.b[n:]
	return v
}

// count reads a count, at most what the rest of the input could hold.
func (d *dec) count() int {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.fail("count past the end")
		d.b = nil
		return 0
	}
	return int(n)
}

func (d *dec) end() int32      { return int32(d.u()) - 1 }
func (d *dec) lease() LeaseRef { return LeaseRef(d.i()) }

func (d *dec) rev(owner int32) Rev {
	v := d.u()
	switch {
	case v == 0:
		return 0
	case v == 1:
		return RevStale
	case v%2 == 0:
		return Rev(owner - int32((v-2)/2) + 1)
	default:
		return Rev(-int64((v-3)/2) - 1)
	}
}

// sized makes a slice of n, nil for none, as the converter leaves them.
func sized[T any](n int) []T {
	if n == 0 {
		return nil
	}
	return make([]T, n)
}

func decode(b []byte) (*Trace, error) {
	if len(b) < len(magic) || string(b[:len(magic)]) != magic {
		return nil, fmt.Errorf("%w: not a trace file", errBadTrace)
	}
	d := &dec{b: b[len(magic):]}
	t := &Trace{Shape: d.s(), About: d.s(), Seconds: math.Float64frombits(d.u())}
	t.Keys = sized[string](d.count())
	for i := range t.Keys {
		t.Keys[i] = d.s()
	}
	t.Leases = sized[int64](d.count())
	for i := range t.Leases {
		t.Leases[i] = d.i()
	}
	t.Preload = sized[Item](d.count())
	for i := range t.Preload {
		t.Preload[i] = Item{Key: int32(d.u()), Size: int32(d.u()), Lease: d.lease()}
	}
	t.Watches = sized[WatchStream](d.count())
	for i := range t.Watches {
		ws := &t.Watches[i]
		ws.Creates = sized[Watch](d.count())
		for j := range ws.Creates {
			w := &ws.Creates[j]
			w.Key, w.End, w.PrevKV, w.Progress = int32(d.u()), d.end(), d.bool(), d.bool()
			for range d.count() {
				w.Filters = append(w.Filters, int32(d.i()))
			}
		}
	}
	t.Calls = sized[Call](d.count())
	var at int64
	for i := range t.Calls {
		owner := int32(i)
		c := &t.Calls[i]
		at += d.i()
		d.key = -1
		c.At, c.Kind, c.Failed = at*1000, d.byte(), d.bool()
		for range d.count() {
			c.Waits = append(c.Waits, owner-int32(d.u()))
		}
		switch c.Kind {
		case CallRange, CallRangeStream, CallPut, CallDelete, CallTxn:
			d.op(&c.Op, owner)
		case CallCompact:
			c.Rev = d.rev(owner)
		case CallLeaseGrant:
			c.TTL = d.i()
		case CallProgress:
			c.Stream = int32(d.u())
		default:
			c.Lease = d.lease()
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("bytes after the last call")
	}
	if d.err != nil {
		return nil, d.err
	}
	return t, t.check()
}

func (d *dec) op(o *Op, owner int32) {
	o.Kind, o.Flags, o.Key, o.End = d.byte(), d.byte(), d.k(), d.end()
	switch o.Kind {
	case OpRange:
		o.Limit = d.i()
		o.Rev = d.rev(owner)
	case OpPut:
		o.Size = int32(d.u())
		o.Lease = d.lease()
	case OpTxn:
		o.Compare = sized[Compare](d.count())
		for i := range o.Compare {
			c := &o.Compare[i]
			c.Target, c.Result, c.Key, c.End = int32(d.byte()), int32(d.byte()), d.k(), d.end()
			c.Rev, c.Value = d.rev(owner), d.i()
		}
		o.Success = sized[Op](d.count())
		for i := range o.Success {
			d.op(&o.Success[i], owner)
		}
		o.Failure = sized[Op](d.count())
		for i := range o.Failure {
			d.op(&o.Failure[i], owner)
		}
	}
}

// check makes sure every index in t is in range and every reference
// points back, so a replay can use them without checks of its own.
func (t *Trace) check() error {
	keys := int32(len(t.Keys))
	bad := func(what string, i int) error { return fmt.Errorf("%w: %s at call %d", errBadTrace, what, i) }
	badAt := func(what string, i int) error { return fmt.Errorf("%w: %s %d", errBadTrace, what, i) }
	key := func(k int32) bool { return k >= 0 && k < keys }
	end := func(k int32) bool { return k >= -1 && k < keys }
	lease := func(l LeaseRef, owner int32) bool {
		return l <= 0 && int(-l) <= len(t.Leases) || l > 0 && int32(l-1) < owner && t.Calls[l-1].Kind == CallLeaseGrant
	}
	rev := func(r Rev, owner int32) bool {
		return r == 0 || r == RevStale || r > 0 && int64(r-1) < int64(owner) || r < 0 && -int64(r) <= int64(len(t.Preload))
	}
	for i, it := range t.Preload {
		if !key(it.Key) || !lease(it.Lease, 0) {
			return badAt("preload item", i)
		}
	}
	for i, ws := range t.Watches {
		for _, w := range ws.Creates {
			if !key(w.Key) || !end(w.End) {
				return badAt("watch stream", i)
			}
		}
	}
	var op func(o *Op, owner int32) bool
	op = func(o *Op, owner int32) bool {
		if !key(o.Key) || !end(o.End) || !rev(o.Rev, owner) || !lease(o.Lease, owner) || o.Kind > OpTxn {
			return false
		}
		for _, c := range o.Compare {
			if !key(c.Key) || !end(c.End) || !rev(c.Rev, owner) || c.Target == TargetLease && !lease(LeaseRef(c.Value), owner) {
				return false
			}
		}
		for i := range o.Success {
			if !op(&o.Success[i], owner) {
				return false
			}
		}
		for i := range o.Failure {
			if !op(&o.Failure[i], owner) {
				return false
			}
		}
		return true
	}
	for i := range t.Calls {
		c := &t.Calls[i]
		owner := int32(i)
		if c.Kind >= CallKinds {
			return bad("call kind", i)
		}
		for _, w := range c.Waits {
			if w < 0 || w >= owner {
				return bad("wait", i)
			}
		}
		switch c.Kind {
		case CallRange, CallRangeStream, CallPut, CallDelete, CallTxn:
			if !op(&c.Op, owner) {
				return bad("request", i)
			}
		case CallCompact:
			if !rev(c.Rev, owner) {
				return bad("revision", i)
			}
		case CallProgress:
			if c.Stream < 0 || int(c.Stream) >= len(t.Watches) {
				return bad("watch stream", i)
			}
		case CallLeaseRevoke, CallLeaseKeepAlive, CallLeaseTTL:
			if !lease(c.Lease, owner) {
				return bad("lease", i)
			}
		}
	}
	return nil
}
