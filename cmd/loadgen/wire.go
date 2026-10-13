package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protowire"
)

// Field numbers from etcd v3.7.2 api/etcdserverpb/rpc.proto and
// api/mvccpb/kv.proto. The messages are written and read by hand so that a
// request costs a copy and a few patched bytes, not a marshal.
const (
	headerRevision = 3 // ResponseHeader.revision

	rangeKey   = 1 // RangeRequest
	rangeEnd   = 2
	rangeLimit = 3
	putKey     = 1 // PutRequest
	putValue   = 2
	delKey     = 1 // DeleteRangeRequest

	txnCompare = 1 // TxnRequest
	txnSuccess = 2
	txnFailure = 3

	cmpTarget      = 2 // Compare
	cmpKey         = 3
	cmpModRevision = 6
	cmpTargetMod   = 2 // Compare.CompareTarget MOD; result EQUAL is the default 0

	opRange = 1 // RequestOp
	opPut   = 2

	watchCreate      = 1 // WatchRequest
	watchKey         = 1 // WatchCreateRequest
	watchRangeEnd    = 2
	watchProgress    = 4
	watchPrevKV      = 6
	respHeader       = 1 // field 1 of every response
	txnSucceeded     = 2 // TxnResponse
	txnResponses     = 3
	opResponsePut    = 2 // ResponseOp
	rangeKVs         = 2 // RangeResponse
	rangeCount       = 4
	delDeleted       = 2 // DeleteRangeResponse
	watchCreated     = 3 // WatchResponse
	watchCanceled    = 4
	watchCompactRev  = 5
	watchCancelMsg   = 6
	watchEvents      = 11
	eventType        = 1 // mvccpb.Event
	eventKV          = 2
	kvKey            = 1 // mvccpb.KeyValue
	kvModRevision    = 3
	kvValue          = 5
	eventTypeDelete  = 1
	revisionVarintSz = 9 // a revision written as a padded varint, see putRevision
)

// msg builds one protobuf message and remembers where the variable parts
// are, so requests can be stamped out from it.
type msg struct {
	b     []byte
	keys  []int // offsets of key bytes
	value int   // offset of value bytes, -1 if none
	rev   int   // offset of a padded revision varint, -1 if none
}

func newMsg() *msg { return &msg{value: -1, rev: -1} }

func (m *msg) varint(num protowire.Number, v uint64) {
	m.b = protowire.AppendTag(m.b, num, protowire.VarintType)
	m.b = protowire.AppendVarint(m.b, v)
}

func (m *msg) bytes(num protowire.Number, v []byte) {
	m.b = protowire.AppendTag(m.b, num, protowire.BytesType)
	m.b = protowire.AppendBytes(m.b, v)
}

// key adds a key field of n bytes to be filled in per request.
func (m *msg) key(num protowire.Number, n int) {
	m.b = protowire.AppendTag(m.b, num, protowire.BytesType)
	m.b = protowire.AppendVarint(m.b, uint64(n))
	m.keys = append(m.keys, len(m.b))
	m.b = append(m.b, make([]byte, n)...)
}

// val adds a value field of n bytes to be filled in per request.
func (m *msg) val(num protowire.Number, n int) {
	m.b = protowire.AppendTag(m.b, num, protowire.BytesType)
	m.b = protowire.AppendVarint(m.b, uint64(n))
	m.value = len(m.b)
	m.b = append(m.b, make([]byte, n)...)
}

// revision adds an int64 field to be filled in per request with putRevision.
func (m *msg) revision(num protowire.Number) {
	m.b = protowire.AppendTag(m.b, num, protowire.VarintType)
	m.rev = len(m.b)
	m.b = putRevision(m.b, 0)
}

// sub embeds c as a nested message.
func (m *msg) sub(num protowire.Number, c *msg) {
	m.b = protowire.AppendTag(m.b, num, protowire.BytesType)
	m.b = protowire.AppendVarint(m.b, uint64(len(c.b)))
	off := len(m.b)
	m.b = append(m.b, c.b...)
	for _, k := range c.keys {
		m.keys = append(m.keys, off+k)
	}
	if c.value >= 0 {
		m.value = off + c.value
	}
	if c.rev >= 0 {
		m.rev = off + c.rev
	}
}

// putRevision writes v as a varint padded to a fixed 9 bytes, so a
// revision can be patched into a prebuilt request without moving anything.
// Protobuf decoders accept varints with redundant continuation bytes, and 9
// bytes hold 63 bits, every non-negative int64. It appends when len(b) is the
// write offset and overwrites in place when b is long enough.
func putRevision(b []byte, v int64) []byte {
	var p [revisionVarintSz]byte
	u := uint64(v)
	for i := range revisionVarintSz - 1 {
		p[i] = byte(u&0x7f) | 0x80
		u >>= 7
	}
	p[revisionVarintSz-1] = byte(u & 0x7f)
	return append(b, p[:]...)
}

func patchRevision(b []byte, v int64) { putRevision(b[:0], v) }

// template is a complete prebuilt gRPC request: an HTTP/2 HEADERS frame and
// one DATA frame. The stream ids, keys, value and revision are patched in
// per request.
type template struct {
	b       []byte
	data    int // offset of the DATA frame header
	dataLen int // DATA payload bytes, which count against flow control
	keys    []int
	value   int
	rev     int
}

const (
	frameHeaderLen = 9
	grpcPrefixLen  = 5
	// maxDataFrame is the HTTP/2 minimum of SETTINGS_MAX_FRAME_SIZE, which
	// every server accepts, so each request fits in one DATA frame.
	maxDataFrame = 16384
)

// headerBlock encodes the request headers of a gRPC call. Every field is
// marked sensitive, so the encoder never adds to its dynamic table and the
// block can be sent unchanged on any stream of any connection.
func headerBlock(authority, path string) []byte {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for _, f := range [][2]string{
		{":method", "POST"},
		{":scheme", "http"},
		{":path", path},
		{":authority", authority},
		{"content-type", "application/grpc"},
		{"te", "trailers"},
	} {
		// Writing to a bytes.Buffer cannot fail.
		_ = enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1], Sensitive: true})
	}
	return buf.Bytes()
}

// newTemplate frames m as the single request message of a gRPC call on path.
// A unary call ends the stream with the request; a watch keeps it open.
func newTemplate(authority, path string, m *msg, endStream bool) (*template, error) {
	payload := grpcPrefixLen + len(m.b)
	if payload > maxDataFrame {
		return nil, fmt.Errorf("request of %d bytes does not fit one %d byte DATA frame", payload, maxDataFrame)
	}
	block := headerBlock(authority, path)
	t := &template{dataLen: payload, value: -1, rev: -1}
	t.b = appendFrameHeader(t.b, len(block), http2.FrameHeaders, http2.FlagHeadersEndHeaders)
	t.b = append(t.b, block...)
	t.data = len(t.b)
	var flags http2.Flags
	if endStream {
		flags = http2.FlagDataEndStream
	}
	t.b = appendFrameHeader(t.b, payload, http2.FrameData, flags)
	t.b = append(t.b, 0)
	t.b = binary.BigEndian.AppendUint32(t.b, uint32(len(m.b)))
	off := len(t.b)
	t.b = append(t.b, m.b...)
	for _, k := range m.keys {
		t.keys = append(t.keys, off+k)
	}
	if m.value >= 0 {
		t.value = off + m.value
	}
	if m.rev >= 0 {
		t.rev = off + m.rev
	}
	return t, nil
}

// appendFrameHeader appends an HTTP/2 frame header with stream id 0; the id
// is patched per request.
func appendFrameHeader(b []byte, length int, typ http2.FrameType, flags http2.Flags) []byte {
	return append(b, byte(length>>16), byte(length>>8), byte(length), byte(typ), byte(flags), 0, 0, 0, 0)
}

func putRequest(keyLen, valueLen int) *msg {
	m := newMsg()
	m.key(putKey, keyLen)
	m.val(putValue, valueLen)
	return m
}

// listRequest is a page of a LIST: a range over a prefix of prefixLen
// bytes, filled in per request, with a limit.
func listRequest(prefixLen, page int) *msg {
	m := newMsg()
	m.key(rangeKey, prefixLen)
	m.key(rangeEnd, prefixLen)
	m.varint(rangeLimit, uint64(page))
	return m
}

func rangeRequest(keyLen int) *msg {
	m := newMsg()
	m.key(rangeKey, keyLen)
	return m
}

func deleteRequest(keyLen int) *msg {
	m := newMsg()
	m.key(delKey, keyLen)
	return m
}

// casRequest is the update Kubernetes sends: put the key if its mod revision
// is still the one the client read, else read the key.
func casRequest(keyLen, valueLen int) *msg {
	cmp := newMsg()
	cmp.varint(cmpTarget, cmpTargetMod)
	cmp.key(cmpKey, keyLen)
	cmp.revision(cmpModRevision)

	success := newMsg()
	success.sub(opPut, putRequest(keyLen, valueLen))
	failure := newMsg()
	failure.sub(opRange, rangeRequest(keyLen))

	m := newMsg()
	m.sub(txnCompare, cmp)
	m.sub(txnSuccess, success)
	m.sub(txnFailure, failure)
	return m
}

// watchRequest watches every key under prefix with previous values and
// progress notifications, as kube-apiserver does.
func watchRequest(prefix []byte) *msg {
	c := newMsg()
	c.bytes(watchKey, prefix)
	c.bytes(watchRangeEnd, prefixEnd(prefix))
	c.varint(watchProgress, 1)
	c.varint(watchPrevKV, 1)
	m := newMsg()
	m.sub(watchCreate, c)
	return m
}

// prefixEnd is the range end that covers every key starting with p.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}

var errBadProto = errors.New("malformed protobuf response")

// fields calls f for each top-level field of a protobuf message. For varint
// fields v is the value; for bytes fields b is the content.
func fields(m []byte, f func(num protowire.Number, v uint64, b []byte) error) error {
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return errBadProto
		}
		m = m[n:]
		var v uint64
		var b []byte
		//exhaustive:ignore // the default case handles the rest
		switch typ {
		case protowire.VarintType:
			v, n = protowire.ConsumeVarint(m)
		case protowire.BytesType:
			b, n = protowire.ConsumeBytes(m)
		default:
			n = protowire.ConsumeFieldValue(num, typ, m)
		}
		if n < 0 {
			return errBadProto
		}
		m = m[n:]
		if err := f(num, v, b); err != nil {
			return err
		}
	}
	return nil
}

// varintField returns the last value of varint field num in m, or 0.
func varintField(m []byte, num protowire.Number) (uint64, error) {
	var out uint64
	err := fields(m, func(n protowire.Number, v uint64, _ []byte) error {
		if n == num {
			out = v
		}
		return nil
	})
	return out, err
}

// bytesField returns the last value of bytes field num in m, or nil.
func bytesField(m []byte, num protowire.Number) ([]byte, error) {
	var out []byte
	err := fields(m, func(n protowire.Number, _ uint64, b []byte) error {
		if n == num {
			out = b
		}
		return nil
	})
	return out, err
}

// responseRevision returns header.revision of any etcd response.
func responseRevision(m []byte) (int64, error) {
	h, err := bytesField(m, respHeader)
	if err != nil {
		return 0, err
	}
	rev, err := varintField(h, headerRevision)
	if err != nil {
		return 0, err
	}
	if rev == 0 {
		return 0, errors.New("response has no header revision")
	}
	return int64(rev), nil
}
