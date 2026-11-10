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

// encoder writes the requests of one copy's calls. Nested messages are
// built in scratch buffers, one per depth, so a request allocates
// nothing once they have grown.
type encoder struct {
	cp      *k8sCopy
	call    int32
	values  []byte // random bytes that values are cut from
	off     int
	scratch [][]byte
	// ok is what the call writes when its Txn's compares hold, failed
	// when they do not.
	ok, failed tally
}

// tally is what a request writes. Bytes is the fewest it makes durable
// on each node: the keys and values it puts, the keys it deletes, and the
// key and revision of each compare. Creates are puts that compare a mod
// or create revision of 0.
type tally struct {
	bytes                    int64
	writes, creates, deletes int64
}

func (e *encoder) sub(depth int) []byte {
	for len(e.scratch) <= depth {
		e.scratch = append(e.scratch, nil)
	}
	return e.scratch[depth][:0]
}

func (e *encoder) keep(depth int, b []byte) { e.scratch[depth] = b }

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

func (e *encoder) key(b []byte, num protowire.Number, k int32) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(e.cp.keyLen(k)))
	return e.cp.appendKey(b, k)
}

// end writes a range end, none for a single key.
func (e *encoder) end(b []byte, num protowire.Number, k int32) []byte {
	if k < 0 {
		return b
	}
	return e.key(b, num, k)
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
	case k8strace.CallRange, k8strace.CallRangeStream, k8strace.CallPut, k8strace.CallDelete:
		return e.op(b, &c.Op, 0, &e.ok)
	case k8strace.CallTxn:
		return e.txn(b, &c.Op, 0, &e.ok, &e.failed)
	case k8strace.CallCompact:
		return appendVarintNZ(b, compactRevision, e.cp.rev(c.Rev, -1, e.call))
	case k8strace.CallLeaseGrant:
		return appendVarintNZ(b, leaseTTL, c.TTL)
	case k8strace.CallProgress:
		return protowire.AppendBytes(protowire.AppendTag(b, watchProgressReq, protowire.BytesType), nil)
	default:
		return appendVarintNZ(b, leaseRequestID, e.cp.lease(c.Lease, e.call))
	}
}

// op appends the fields of a Range, Put or Delete request, or a Txn's.
func (e *encoder) op(b []byte, o *k8strace.Op, depth int, t *tally) []byte {
	switch o.Kind {
	case k8strace.OpRange:
		b = e.key(b, rangeKey, o.Key)
		b = e.end(b, rangeEnd, o.End)
		b = appendVarintNZ(b, rangeLimit, o.Limit)
		b = appendVarintNZ(b, rangeRevision, e.cp.rev(o.Rev, -1, e.call))
		b = appendBool(b, rangeSerializable, o.Flags&k8strace.FlagSerializable != 0)
		b = appendBool(b, rangeKeysOnly, o.Flags&k8strace.FlagKeysOnly != 0)
		return appendBool(b, rangeCountOnly, o.Flags&k8strace.FlagCountOnly != 0)
	case k8strace.OpPut:
		b = e.key(b, putKey, o.Key)
		b = e.value(b, putValue, int(o.Size))
		b = appendVarintNZ(b, putLease, e.cp.lease(o.Lease, e.call))
		b = appendBool(b, putPrevKV, o.Flags&k8strace.FlagPrevKV != 0)
		b = appendBool(b, putIgnoreValue, o.Flags&k8strace.FlagIgnoreValue != 0)
		b = appendBool(b, putIgnoreLease, o.Flags&k8strace.FlagIgnoreLease != 0)
		t.bytes += int64(e.cp.keyLen(o.Key) + int(o.Size))
		t.writes++
		return b
	case k8strace.OpDelete:
		b = e.key(b, delKey, o.Key)
		b = e.end(b, delRangeEnd, o.End)
		t.bytes += int64(e.cp.keyLen(o.Key))
		t.writes++
		t.deletes++
		return appendBool(b, delPrevKV, o.Flags&k8strace.FlagPrevKV != 0)
	}
	// A nested Txn's outcome is not known; its success branch counts.
	var discard tally
	return e.txn(b, o, depth, t, &discard)
}

// txn appends a TxnRequest's fields, and adds what it writes to ok when
// its compares hold and to failed when they do not.
func (e *encoder) txn(b []byte, o *k8strace.Op, depth int, ok, failed *tally) []byte {
	var compared int64
	create := false
	for i := range o.Compare {
		c := &o.Compare[i]
		m := e.sub(depth)
		m = appendVarintNZ(m, cmpResult, int64(c.Result))
		m = appendVarintNZ(m, cmpTarget, int64(c.Target))
		m = e.key(m, cmpKey, c.Key)
		switch c.Target {
		case k8strace.TargetVersion:
			m = appendVarintField(m, cmpVersion, uint64(c.Value))
		case k8strace.TargetCreate:
			m = appendVarintField(m, cmpCreateRevision, uint64(e.cp.rev(c.Rev, c.Key, e.call)))
		case k8strace.TargetMod:
			m = appendVarintField(m, cmpModRevision, uint64(e.cp.rev(c.Rev, c.Key, e.call)))
		case k8strace.TargetValue:
			m = e.value(m, cmpValue, int(c.Value))
		case k8strace.TargetLease:
			m = appendVarintField(m, cmpLease, uint64(e.cp.lease(k8strace.LeaseRef(c.Value), e.call)))
		}
		m = e.end(m, cmpRangeEnd, c.End)
		b = protowire.AppendBytes(protowire.AppendTag(b, txnCompare, protowire.BytesType), m)
		e.keep(depth, m)
		compared += int64(e.cp.keyLen(c.Key)) + revisionBytes
		create = create || (c.Target == k8strace.TargetMod || c.Target == k8strace.TargetCreate) && c.Rev == 0
	}
	ok.bytes += compared
	failed.bytes += compared
	puts := ok.writes - ok.deletes
	b = e.branch(b, txnSuccess, o.Success, depth, ok)
	if create {
		ok.creates += ok.writes - ok.deletes - puts
	}
	return e.branch(b, txnFailure, o.Failure, depth, failed)
}

// opFields are the RequestOp fields of the op kinds.
var opFields = [...]protowire.Number{k8strace.OpRange: opRange, k8strace.OpPut: opPut, k8strace.OpDelete: opDeleteRange, k8strace.OpTxn: opTxn}

func (e *encoder) branch(b []byte, num protowire.Number, ops []k8strace.Op, depth int, t *tally) []byte {
	for i := range ops {
		o := &ops[i]
		body := e.sub(depth + 1)
		body = e.op(body, o, depth+2, t)
		e.keep(depth+1, body)
		m := e.sub(depth)
		m = protowire.AppendBytes(protowire.AppendTag(m, opFields[o.Kind], protowire.BytesType), body)
		e.keep(depth, m)
		b = protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), m)
	}
	return b
}

func appendVarintField(b []byte, num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}
