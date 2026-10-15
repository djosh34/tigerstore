package main

import (
	"errors"
	"hash/maphash"

	"google.golang.org/protobuf/encoding/protowire"
)

// Field numbers from etcd v3.7.2 rpc.proto and kv.proto.
const (
	hdrRevision = 3

	rangeKey   = 1
	rrHeader   = 1
	rrKVs      = 2
	rrMore     = 3
	rrCount    = 4
	trHeader   = 1
	trSucc     = 2
	trResps    = 3
	roRange    = 1 // ResponseOp.response_range
	roPut      = 2
	roDelete   = 3
	delDeleted = 2

	kvKey     = 1
	kvCreate  = 2
	kvMod     = 3
	kvVersion = 4
	kvValue   = 5
	kvLease   = 6

	wrHeader   = 1
	wrCreated  = 3
	wrCanceled = 4
	wrCompact  = 5
	wrEvents   = 11
	evType     = 1
	evKV       = 2
	evPrevKV   = 3
)

var errProto = errors.New("malformed protobuf")

// field reads one field from m. For varints v is the value, for bytes b is
// the content. n is the bytes consumed, or -1.
func field(m []byte) (num protowire.Number, typ protowire.Type, v uint64, b []byte, n int) {
	num, typ, t := protowire.ConsumeTag(m)
	if t < 0 {
		return 0, 0, 0, nil, -1
	}
	var k int
	switch typ {
	case protowire.VarintType:
		v, k = protowire.ConsumeVarint(m[t:])
	case protowire.BytesType:
		b, k = protowire.ConsumeBytes(m[t:])
	default:
		k = protowire.ConsumeFieldValue(num, typ, m[t:])
	}
	if k < 0 {
		return 0, 0, 0, nil, -1
	}
	return num, typ, v, b, t + k
}

// headerRev returns ResponseHeader.revision.
func headerRev(h []byte) (int64, error) {
	var rev uint64
	for len(h) > 0 {
		num, typ, v, _, n := field(h)
		if n < 0 {
			return 0, errProto
		}
		if num == hdrRevision && typ == protowire.VarintType {
			rev = v
		}
		h = h[n:]
	}
	if rev == 0 {
		return 0, errors.New("no header revision")
	}
	return int64(rev), nil
}

// encKV appends the canonical mvccpb.KeyValue: fields 1..6 in order, zero
// values omitted, lease 0. This is etcd's own encoding.
func encKV(b, key []byte, create, mod, version int64, value []byte) []byte {
	b = append(b, kvKey<<3|2)
	b = protowire.AppendVarint(b, uint64(len(key)))
	b = append(b, key...)
	if create != 0 {
		b = append(b, kvCreate<<3)
		b = protowire.AppendVarint(b, uint64(create))
	}
	if mod != 0 {
		b = append(b, kvMod<<3)
		b = protowire.AppendVarint(b, uint64(mod))
	}
	if version != 0 {
		b = append(b, kvVersion<<3)
		b = protowire.AppendVarint(b, uint64(version))
	}
	if len(value) > 0 {
		b = append(b, kvValue<<3|2)
		b = protowire.AppendVarint(b, uint64(len(value)))
		b = append(b, value...)
	}
	return b
}

// canonKV returns the canonical encoding of a KeyValue and its key and
// mod_revision. If raw is already canonical (the usual case) it is returned
// as is; otherwise its known fields are re-encoded into scratch, the last
// copy of a field winning, unknown fields dropped.
func canonKV(raw, scratch []byte) (canon, key []byte, mod uint64, out []byte, err error) {
	last := protowire.Number(0)
	canonical := true
	var create, version, lease uint64
	var value []byte
	for m := raw; len(m) > 0; {
		num, typ, v, b, n := field(m)
		if n < 0 {
			return nil, nil, 0, scratch, errProto
		}
		if num <= last || num > kvLease {
			canonical = false
		}
		last = num
		switch num {
		case kvKey, kvValue:
			if typ != protowire.BytesType {
				return nil, nil, 0, scratch, errProto
			}
			if len(b) == 0 || n != 1+protowire.SizeVarint(uint64(len(b)))+len(b) {
				canonical = false
			}
			if num == kvKey {
				key = b
			} else {
				value = b
			}
		case kvCreate, kvMod, kvVersion, kvLease:
			if typ != protowire.VarintType {
				return nil, nil, 0, scratch, errProto
			}
			if v == 0 || n != 1+protowire.SizeVarint(v) {
				canonical = false
			}
			switch num {
			case kvCreate:
				create = v
			case kvMod:
				mod = v
			case kvVersion:
				version = v
			case kvLease:
				lease = v
			}
		}
		m = m[n:]
	}
	if canonical {
		return raw, key, mod, scratch, nil
	}
	b := scratch[:0]
	if len(key) > 0 {
		b = protowire.AppendTag(b, kvKey, protowire.BytesType)
		b = protowire.AppendBytes(b, key)
	}
	for _, f := range [...]struct {
		n protowire.Number
		v uint64
	}{{kvCreate, create}, {kvMod, mod}} {
		if f.v != 0 {
			b = protowire.AppendTag(b, f.n, protowire.VarintType)
			b = protowire.AppendVarint(b, f.v)
		}
	}
	if version != 0 {
		b = protowire.AppendTag(b, kvVersion, protowire.VarintType)
		b = protowire.AppendVarint(b, version)
	}
	if len(value) > 0 {
		b = protowire.AppendTag(b, kvValue, protowire.BytesType)
		b = protowire.AppendBytes(b, value)
	}
	if lease != 0 {
		b = protowire.AppendTag(b, kvLease, protowire.VarintType)
		b = protowire.AppendVarint(b, lease)
	}
	return b, key, mod, b, nil
}

// Fingerprint of a canonical event: type, kv and prev_kv (or its absence),
// from 64-bit runtime memhash (AES on arm64) of each KV with one
// process-wide seed, combined asymmetrically. Workers cache the hash of
// each key's current KV, so a write hashes only its new KV.
var seed = maphash.MakeSeed()

func kvHash(kv []byte) uint64 { return maphash.Bytes(seed, kv) }

func fingerprint(typ uint64, hkv, hprev uint64, hasPrev bool) uint64 {
	if !hasPrev {
		hprev = 0x9e3779b97f4a7c15
	}
	x := hkv*0xff51afd7ed558ccd + (hprev^0x2545f4914f6cdd1d)*0xc4ceb9fe1a85ec53 + typ*0x94d049bb133111eb
	x ^= x >> 31
	x *= 0xbf58476d1ce4e5b9
	return x ^ x>>29
}
