package main

/*
#cgo LDFLAGS: -lzstd
#include <stdlib.h>
#include <zstd.h>
#include <zdict.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Thin cgo wrapper over the system libzstd, so that levels 1, 2 and 3 are
// the reference ones and dictionaries come from the reference trainer.

func zversion() string { return C.GoString(C.ZSTD_versionString()) }

type cz struct {
	cctx  *C.ZSTD_CCtx
	dctx  *C.ZSTD_DCtx
	level int
}

func newCZ(level int) *cz {
	return &cz{cctx: C.ZSTD_createCCtx(), dctx: C.ZSTD_createDCtx(), level: level}
}

type cdict struct {
	c *C.ZSTD_CDict
	d *C.ZSTD_DDict
}

func newCDict(dict []byte, level int) *cdict {
	p := unsafe.Pointer(&dict[0])
	return &cdict{C.ZSTD_createCDict(p, C.size_t(len(dict)), C.int(level)), C.ZSTD_createDDict(p, C.size_t(len(dict)))}
}

func (d *cdict) free() { C.ZSTD_freeCDict(d.c); C.ZSTD_freeDDict(d.d) }

func grow(dst []byte, n int) []byte {
	if cap(dst)-len(dst) < n {
		nd := make([]byte, len(dst), 2*cap(dst)+n)
		copy(nd, dst)
		dst = nd
	}
	return dst
}

// compress appends the zstd frame of src to dst. Frames carry the content
// size, no checksum (the log record has a crc), and the dictionary ID.
func (z *cz) compress(dst, src []byte, d *cdict) []byte {
	bound := int(C.ZSTD_compressBound(C.size_t(len(src))))
	dst = grow(dst, bound)
	out := unsafe.Pointer(unsafe.SliceData(dst[len(dst):cap(dst)]))
	in := unsafe.Pointer(unsafe.SliceData(src))
	var n C.size_t
	if d == nil {
		n = C.ZSTD_compressCCtx(z.cctx, out, C.size_t(bound), in, C.size_t(len(src)), C.int(z.level))
	} else {
		n = C.ZSTD_compress_usingCDict(z.cctx, out, C.size_t(bound), in, C.size_t(len(src)), d.c)
	}
	if C.ZSTD_isError(n) != 0 {
		panic(C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:len(dst)+int(n)]
}

func (z *cz) decompress(dst, src []byte, d *cdict) ([]byte, error) {
	in := unsafe.Pointer(unsafe.SliceData(src))
	size := C.ZSTD_getFrameContentSize(in, C.size_t(len(src)))
	if size == C.ZSTD_CONTENTSIZE_ERROR || size == C.ZSTD_CONTENTSIZE_UNKNOWN {
		return nil, errors.New("zstd: no content size")
	}
	if uint64(cap(dst)) < uint64(size) {
		dst = make([]byte, size)
	}
	dst = dst[:size]
	if size == 0 {
		return dst, nil
	}
	out := unsafe.Pointer(unsafe.SliceData(dst))
	var n C.size_t
	if d == nil {
		n = C.ZSTD_decompressDCtx(z.dctx, out, C.size_t(size), in, C.size_t(len(src)))
	} else {
		n = C.ZSTD_decompress_usingDDict(z.dctx, out, C.size_t(size), in, C.size_t(len(src)), d.d)
	}
	if C.ZSTD_isError(n) != 0 {
		return nil, errors.New(C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst, nil
}

// coldDecompress builds a fresh decoder (and dictionary tables) for one
// value, as a read would if no decoder were cached.
func coldDecompress(dst, src, dict []byte) ([]byte, error) {
	z := &cz{dctx: C.ZSTD_createDCtx()}
	defer C.ZSTD_freeDCtx(z.dctx)
	if dict == nil {
		return z.decompress(dst, src, nil)
	}
	dd := C.ZSTD_createDDict(unsafe.Pointer(&dict[0]), C.size_t(len(dict)))
	defer C.ZSTD_freeDDict(dd)
	return z.decompress(dst, src, &cdict{d: dd})
}

// train runs the reference trainer (ZDICT_trainFromBuffer, fastCover).
func trainDict(samples [][]byte, capacity int) ([]byte, error) {
	var all []byte
	sizes := make([]C.size_t, len(samples))
	for i, s := range samples {
		all = append(all, s...)
		sizes[i] = C.size_t(len(s))
	}
	if len(all) == 0 {
		return nil, errors.New("no samples")
	}
	call := C.malloc(C.size_t(len(all)))
	defer C.free(call)
	copy(unsafe.Slice((*byte)(call), len(all)), all)
	csizes := C.malloc(C.size_t(len(sizes)) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	defer C.free(csizes)
	copy(unsafe.Slice((*C.size_t)(csizes), len(sizes)), sizes)
	out := make([]byte, capacity)
	n := C.ZDICT_trainFromBuffer(unsafe.Pointer(&out[0]), C.size_t(capacity), call, (*C.size_t)(csizes), C.uint(len(samples)))
	if C.ZDICT_isError(n) != 0 {
		return nil, errors.New(C.GoString(C.ZDICT_getErrorName(n)))
	}
	return out[:n], nil
}
