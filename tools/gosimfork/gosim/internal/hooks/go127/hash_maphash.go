package go127

import (
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func HashMaphash_runtime_rand() uint64 {
	return gosimruntime.Fastrand64()
}

// HashMaphash_runtime_memhash hashes like runtime.memhash, with FNV-1a instead
// of the runtime's hash, which mixes in a random key chosen when the process
// starts. With it a seed's hashes are the same in every run.
func HashMaphash_runtime_memhash(p unsafe.Pointer, seed, s uintptr) uintptr {
	h := uint64(seed) ^ 14695981039346656037
	for _, b := range unsafe.Slice((*byte)(p), s) {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return uintptr(h)
}
