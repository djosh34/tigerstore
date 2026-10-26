// Copyright 2011 The Go Authors. All rights reserved.  Use of this source code
// is governed by a BSD-style license that can be found at
// https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

package gosimruntime

import (
	"encoding/binary"
	"math"
)

type (
	fnv64 uint64
)

const (
	fnv64init = 14695981039346656037
	prime64   = 1099511628211
)

// newFnv64 returns a new 64-bit FNV-1 hash.Hash.
// Its Sum method will lay the value out in big-endian byte order.
func newFnv64() fnv64 {
	var s fnv64 = fnv64init
	return s
}

func (s *fnv64) Sum() uint64 { return uint64(*s) }

func (s *fnv64) Hash(data []byte) {
	hash := *s
	for _, c := range data {
		hash *= prime64
		hash ^= fnv64(c)
	}
	*s = hash
}

func (s *fnv64) HashInt(data uint64) {
	switch {
	case data == 0:
	case data < math.MaxUint8:
		s.Hash([]byte{uint8(data)})
	case data < math.MaxUint16:
		var n [2]byte
		binary.LittleEndian.PutUint16(n[:], uint16(data))
		s.Hash(n[:])
	case data < math.MaxUint32:
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(data))
		s.Hash(n[:])
	default:
		var n [8]byte
		binary.LittleEndian.PutUint64(n[:], data)
		s.Hash(n[:])
	}
}
