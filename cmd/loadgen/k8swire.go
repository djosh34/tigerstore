package main

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// Field numbers of the requests a replay sends, from etcd v3.7.2
// api/etcdserverpb/rpc.proto; wire.go has the rest.
const (
	rangeRevision     = 4 // RangeRequest
	rangeSerializable = 7
	rangeKeysOnly     = 8
	rangeCountOnly    = 9
	putPrevKV         = 4 // PutRequest
	putIgnoreValue    = 5
	putIgnoreLease    = 6
	delRangeEnd       = 2 // DeleteRangeRequest
	delPrevKV         = 3
	opTxn             = 4 // RequestOp
	cmpResult         = 1 // Compare
	cmpVersion        = 4
	cmpValue          = 7
	cmpLease          = 8
	cmpRangeEnd       = 64
	compactRevision   = 1 // CompactionRequest
	leaseRequestID    = 1 // LeaseRevokeRequest, LeaseKeepAliveRequest, LeaseTimeToLiveRequest
	watchFilters      = 5 // WatchCreateRequest
	watchProgressReq  = 3 // WatchRequest
	watchResponseID   = 2 // WatchResponse
)

// paths are the gRPC methods of the calls of a trace, by kind.
var paths = [k8strace.CallKinds]string{
	k8strace.CallRange:          "/etcdserverpb.KV/Range",
	k8strace.CallRangeStream:    "/etcdserverpb.KV/RangeStream",
	k8strace.CallPut:            "/etcdserverpb.KV/Put",
	k8strace.CallDelete:         "/etcdserverpb.KV/DeleteRange",
	k8strace.CallTxn:            "/etcdserverpb.KV/Txn",
	k8strace.CallCompact:        "/etcdserverpb.KV/Compact",
	k8strace.CallLeaseGrant:     "/etcdserverpb.Lease/LeaseGrant",
	k8strace.CallLeaseRevoke:    "/etcdserverpb.Lease/LeaseRevoke",
	k8strace.CallLeaseKeepAlive: "/etcdserverpb.Lease/LeaseKeepAlive",
	k8strace.CallLeaseTTL:       "/etcdserverpb.Lease/LeaseTimeToLive",
	k8strace.CallProgress:       "/etcdserverpb.Watch/Watch",
}

// encoder writes the requests of one copy's calls. A nested message is
// written in place, behind a length of fixed width that is filled in
// after it (openField, closeField), so no byte of a request is copied twice.
type encoder struct {
	cp     *k8sCopy
	call   int32
	values []byte // random bytes that values are cut from
	off    int
	// ok is what the call writes when its Txn's compares hold, failed
	// when they do not.
	ok, failed tally
}

// tally is what a request writes and looks up, as counts has it. Bytes
// is the fewest it makes durable on each node: the keys and values it
// puts, a put's lease ID, the keys it deletes, a granted lease's ID and
// TTL, a revoked lease's ID and a compaction's revision, as fixed 8-byte
// integers. A Txn's compares count their key and revision if the branch
// it takes writes: a Txn that only reads keeps nothing. A lease
// keepalive makes nothing durable: etcd renews leases on the leader
// alone. Creates are puts that compare a mod or create revision of 0.
type tally struct {
	bytes                    int64
	writes, creates, deletes int64
	entries, checks, leases  int64
}

// idBytes is what a lease ID, a TTL or a revision makes durable: a fixed
// 8-byte integer.
const idBytes = 8

// lengthWidth is the bytes a nested message's length takes: a varint
// padded with continuation bytes, which protobuf decoders accept, as they
// accept the padded revisions of putRevision. 4 bytes hold 2^28 - 1, more
// than a request may be.
const lengthWidth = 4

// openField appends the tag of a nested message num and room for its length,
// and returns where the message starts.
func openField(b []byte, num protowire.Number) ([]byte, int) {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return append(b, make([]byte, lengthWidth)...), len(b) + lengthWidth
}

// closeField writes the length of the nested message that starts at start and
// ends b.
func closeField(b []byte, start int) []byte {
	n := len(b) - start
	at := b[start-lengthWidth : start]
	for i := range lengthWidth - 1 {
		at[i] = byte(n&0x7f) | 0x80
		n >>= 7
	}
	at[lengthWidth-1] = byte(n)
	return b
}

func appendBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}
	return appendVarintField(b, num, 1)
}

func appendVarintNZ(b []byte, num protowire.Number, v int64) []byte {
	if v == 0 {
		return b
	}
	return appendVarintField(b, num, uint64(v))
}

// key appends key k of the copy as field num and returns its length. It
// looks the key up once: over a trace's tens of thousands of keys, each
// lookup is a cache miss.
func (e *encoder) key(b []byte, num protowire.Number, k int32) ([]byte, int) {
	key := &e.cp.keys.keys[k]
	name, at := key.text(), key.split
	extra := e.cp.tag
	switch at {
	case keepKey:
		extra = nil
	case prefixKey:
		extra = e.cp.prefix
	}
	n := len(name) + len(extra)
	b = protowire.AppendVarint(protowire.AppendTag(b, num, protowire.BytesType), uint64(n))
	switch at {
	case keepKey:
		b = append(b, name...)
	case prefixKey:
		b = append(append(b, extra...), name...)
	default:
		b = append(append(append(b, name[:at]...), extra...), name[at:]...)
	}
	return b, n
}

// end writes a range end, none for a single key.
func (e *encoder) end(b []byte, num protowire.Number, k int32) []byte {
	if k < 0 {
		return b
	}
	b, _ = e.key(b, num, k)
	return b
}

func (e *encoder) value(b []byte, num protowire.Number, n int) []byte {
	n = min(n, len(e.values))
	if e.off+n > len(e.values) {
		e.off = 0
	}
	b = protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), e.values[e.off:e.off+n])
	e.off += n
	return b
}

// request appends the request of call to b.
func (e *encoder) request(b []byte, c *k8strace.Call) []byte {
	e.ok, e.failed = tally{}, tally{}
	switch c.Kind {
	case k8strace.CallRange, k8strace.CallRangeStream:
		return e.op(b, &c.Op, &e.ok)
	case k8strace.CallPut, k8strace.CallDelete:
		e.ok.entries = 1
		return e.op(b, &c.Op, &e.ok)
	case k8strace.CallTxn:
		return e.txn(b, &c.Op, &e.ok, &e.failed)
	case k8strace.CallCompact:
		e.ok = tally{bytes: idBytes, entries: 1}
		return appendVarintNZ(b, compactRevision, e.cp.compactRev(e.call))
	case k8strace.CallLeaseGrant:
		e.ok = tally{bytes: 2 * idBytes, entries: 1, leases: 1}
		return appendVarintNZ(b, leaseTTL, c.TTL)
	case k8strace.CallProgress:
		return protowire.AppendBytes(protowire.AppendTag(b, watchProgressReq, protowire.BytesType), nil)
	case k8strace.CallLeaseRevoke:
		e.ok = tally{bytes: idBytes, entries: 1, leases: 1}
	default:
		e.ok.leases = 1
	}
	return appendVarintNZ(b, leaseRequestID, e.cp.lease(c.Lease))
}

// op appends the fields of a Range, Put or Delete request, or a Txn's.
func (e *encoder) op(b []byte, o *k8strace.Op, t *tally) []byte {
	switch o.Kind {
	case k8strace.OpRange:
		b, _ = e.key(b, rangeKey, o.Key)
		b = e.end(b, rangeEnd, o.End)
		b = appendVarintNZ(b, rangeLimit, o.Limit)
		b = appendVarintNZ(b, rangeRevision, e.cp.rev(o.Rev, -1))
		b = appendBool(b, rangeSerializable, o.Flags&k8strace.FlagSerializable != 0)
		b = appendBool(b, rangeKeysOnly, o.Flags&k8strace.FlagKeysOnly != 0)
		return appendBool(b, rangeCountOnly, o.Flags&k8strace.FlagCountOnly != 0)
	case k8strace.OpPut:
		var n int
		b, n = e.key(b, putKey, o.Key)
		b = e.value(b, putValue, int(o.Size))
		lease := e.cp.lease(o.Lease)
		b = appendVarintNZ(b, putLease, lease)
		b = appendBool(b, putPrevKV, o.Flags&k8strace.FlagPrevKV != 0)
		b = appendBool(b, putIgnoreValue, o.Flags&k8strace.FlagIgnoreValue != 0)
		b = appendBool(b, putIgnoreLease, o.Flags&k8strace.FlagIgnoreLease != 0)
		t.bytes += int64(n + int(o.Size))
		if lease != 0 {
			t.bytes += idBytes
			t.leases++
		}
		t.writes++
		return b
	case k8strace.OpDelete:
		var n int
		b, n = e.key(b, delKey, o.Key)
		b = e.end(b, delRangeEnd, o.End)
		t.bytes += int64(n)
		t.writes++
		t.deletes++
		return appendBool(b, delPrevKV, o.Flags&k8strace.FlagPrevKV != 0)
	}
	// A nested Txn's outcome is not known; its success branch counts.
	var discard tally
	return e.txn(b, o, t, &discard)
}

// txn appends a TxnRequest's fields, and adds what it writes to ok when
// its compares hold and to failed when they do not.
func (e *encoder) txn(b []byte, o *k8strace.Op, ok, failed *tally) []byte {
	var compared int64
	create := false
	before := [2]tally{*ok, *failed}
	for i := range o.Compare {
		c := &o.Compare[i]
		m, start := openField(b, txnCompare)
		m = appendVarintNZ(m, cmpResult, int64(c.Result))
		m = appendVarintNZ(m, cmpTarget, int64(c.Target))
		m, n := e.key(m, cmpKey, c.Key)
		switch c.Target {
		case k8strace.TargetVersion:
			m = appendVarintField(m, cmpVersion, uint64(c.Value))
		case k8strace.TargetCreate:
			m = appendVarintField(m, cmpCreateRevision, uint64(e.cp.rev(c.Rev, c.Key)))
		case k8strace.TargetMod:
			m = appendVarintField(m, cmpModRevision, uint64(e.cp.rev(c.Rev, c.Key)))
		case k8strace.TargetValue:
			m = e.value(m, cmpValue, int(c.Value))
		case k8strace.TargetLease:
			m = appendVarintField(m, cmpLease, uint64(e.cp.lease(k8strace.LeaseRef(c.Value))))
		}
		m = e.end(m, cmpRangeEnd, c.End)
		b = closeField(m, start)
		compared += int64(n) + revisionBytes
		create = create || (c.Target == k8strace.TargetMod || c.Target == k8strace.TargetCreate) && c.Rev == 0
	}
	puts := ok.writes - ok.deletes
	b = e.branch(b, txnSuccess, o.Success, ok)
	if create {
		ok.creates += ok.writes - ok.deletes - puts
	}
	b = e.branch(b, txnFailure, o.Failure, failed)
	for i, t := range [2]*tally{ok, failed} {
		ops := o.Success
		if i == 1 {
			ops = o.Failure
		}
		t.checks += unchecked(o.Compare, ops)
		if t.writes > before[i].writes {
			t.bytes += compared
			t.entries = 1
		}
	}
	return b
}

// unchecked returns the compares whose key no op of ops reads or writes:
// each looks the key up on its own.
func unchecked(cmps []k8strace.Compare, ops []k8strace.Op) int64 {
	var n int64
next:
	for i := range cmps {
		for j := range ops {
			if ops[j].Kind != k8strace.OpTxn && ops[j].Key == cmps[i].Key {
				continue next
			}
		}
		n++
	}
	return n
}

// opFields are the RequestOp fields of the op kinds.
var opFields = [...]protowire.Number{k8strace.OpRange: opRange, k8strace.OpPut: opPut, k8strace.OpDelete: opDeleteRange, k8strace.OpTxn: opTxn}

func (e *encoder) branch(b []byte, num protowire.Number, ops []k8strace.Op, t *tally) []byte {
	for i := range ops {
		o := &ops[i]
		var reqOp, body int
		b, reqOp = openField(b, num)
		b, body = openField(b, opFields[o.Kind])
		b = e.op(b, o, t)
		b = closeField(closeField(b, body), reqOp)
	}
	return b
}

func appendVarintField(b []byte, num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}
