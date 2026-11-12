package wl

import "errors"

// Protobuf by hand: requests are read in place, replies written straight
// into records.

var errProto = errors.New("malformed protobuf")

func putUvarint(b []byte, v uint64) int {
	i := 0
	for v >= 0x80 {
		b[i] = byte(v) | 0x80
		v >>= 7
		i++
	}
	b[i] = byte(v)
	return i + 1
}

func appendUvarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func sizeVarint(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func appendTag(b []byte, num, typ int) []byte { return appendUvarint(b, uint64(num)<<3|uint64(typ)) }

func appendVarintField(b []byte, num int, v uint64) []byte {
	return appendUvarint(appendTag(b, num, 0), v)
}

func appendBytesField(b []byte, num int, p []byte) []byte {
	b = appendUvarint(appendTag(b, num, 2), uint64(len(p)))
	return append(b, p...)
}

// sizeVarintField is the size of a varint field, 0 for a zero value
// that proto3 leaves out.
func sizeVarintField(num int, v uint64) int {
	if v == 0 {
		return 0
	}
	return sizeVarint(uint64(num)<<3) + sizeVarint(v)
}

func sizeBytesField(num int, n int) int {
	return sizeVarint(uint64(num)<<3|2) + sizeVarint(uint64(n)) + n
}

func uvarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		v |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return v, i + 1
		}
	}
	return 0, -1
}

// field reads the next field of m: its number, wire type, varint value
// or bytes, and the rest of m.
func field(m []byte) (num int, typ int, v uint64, p []byte, rest []byte, err error) {
	t, n := uvarint(m)
	if n < 0 {
		return 0, 0, 0, nil, nil, errProto
	}
	m = m[n:]
	num, typ = int(t>>3), int(t&7)
	switch typ {
	case 0:
		v, n = uvarint(m)
		if n < 0 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, typ, v, nil, m[n:], nil
	case 2:
		l, n := uvarint(m)
		if n < 0 || uint64(len(m)-n) < l {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, typ, 0, m[n : n+int(l)], m[n+int(l):], nil
	case 1:
		if len(m) < 8 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, typ, 0, m[:8], m[8:], nil
	case 5:
		if len(m) < 4 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, typ, 0, m[:4], m[4:], nil
	}
	return 0, 0, 0, nil, nil, errProto
}

// rangeReq is a RangeRequest read in place.
type rangeReq struct {
	key, end     []byte
	limit, rev   int64
	serializable bool
	keysOnly     bool
	countOnly    bool
}

func parseRange(m []byte, r *rangeReq) error {
	*r = rangeReq{}
	for len(m) > 0 {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return err
		}
		m = rest
		switch num {
		case 1:
			r.key = p
		case 2:
			r.end = p
		case 3:
			r.limit = int64(v)
		case 4:
			r.rev = int64(v)
		case 7:
			r.serializable = v != 0
		case 8:
			r.keysOnly = v != 0
		case 9:
			r.countOnly = v != 0
		}
	}
	return nil
}

type putReq struct {
	key, value  []byte
	lease       int64
	prevKV      bool
	ignoreValue bool
	ignoreLease bool
}

func parsePut(m []byte, r *putReq) error {
	*r = putReq{}
	for len(m) > 0 {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return err
		}
		m = rest
		switch num {
		case 1:
			r.key = p
		case 2:
			r.value = p
		case 3:
			r.lease = int64(v)
		case 4:
			r.prevKV = v != 0
		case 5:
			r.ignoreValue = v != 0
		case 6:
			r.ignoreLease = v != 0
		}
	}
	return nil
}

type delReq struct {
	key, end []byte
	prevKV   bool
}

func parseDel(m []byte, r *delReq) error {
	*r = delReq{}
	for len(m) > 0 {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return err
		}
		m = rest
		switch num {
		case 1:
			r.key = p
		case 2:
			r.end = p
		case 3:
			r.prevKV = v != 0
		}
	}
	return nil
}

// compare is a Txn's Compare.
type compare struct {
	result, target int
	key, end       []byte
	num            int64  // version, create or mod revision, or lease
	value          []byte // for a value compare
}

func parseCompare(m []byte, c *compare) error {
	*c = compare{}
	for len(m) > 0 {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return err
		}
		m = rest
		switch num {
		case 1:
			c.result = int(v)
		case 2:
			c.target = int(v)
		case 3:
			c.key = p
		case 4, 5, 6, 8:
			c.num = int64(v)
		case 7:
			c.value = p
		case 64:
			c.end = p
		}
	}
	return nil
}
