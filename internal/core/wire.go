package core

import (
	"errors"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// Put is a PutRequest read in place from its wire form. Key and Value
// are slices of Wire, which nobody may change afterwards.
type Put struct {
	Wire []byte
	mvcc.PutOp
	// unknown is the bytes of unknown and mistyped fields, which protobuf
	// keeps and marshals again.
	unknown int
	// valueAt is where Value starts in Wire.
	valueAt int
}

var errBadPut = errors.New("core: malformed PutRequest")

// ParsePut reads a PutRequest from its wire form without copying it, with
// protobuf's rules: the last value of a field wins, and unknown fields and
// fields of the wrong wire type are skipped.
func ParsePut(b []byte) (Put, error) {
	p := Put{Wire: b}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return Put{}, errBadPut
		}
		b = b[n:]
		switch {
		case typ == protowire.BytesType && (num == 1 || num == 2):
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return Put{}, errBadPut
			}
			// Capped, so an append can never write into the bytes after
			// it, which are the log's.
			v = slices.Clip(v)
			b = b[n:]
			if num == 1 {
				p.Key = v
			} else {
				p.Value, p.valueAt = v, offsetIn(p.Wire, b, v)
			}
		case typ == protowire.VarintType && num >= 3 && num <= 6:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return Put{}, errBadPut
			}
			switch int(num) {
			case 3:
				p.Lease = int64(v)
			case 4:
				p.PrevKv = v != 0
			case 5:
				p.IgnoreValue = v != 0
			case 6:
				p.IgnoreLease = v != 0
			}
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return Put{}, errBadPut
			}
			// protobuf re-encodes the tag minimally.
			p.unknown += protowire.SizeTag(num) + n
			b = b[n:]
		}
	}
	return p, nil
}
