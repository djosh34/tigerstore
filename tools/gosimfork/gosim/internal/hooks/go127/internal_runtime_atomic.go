package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

// sync.Pool reads and writes its size with these.

//go:norace
func InternalRuntimeAtomic_LoadAcquintptr(ptr *uintptr) uintptr {
	if gosimruntime.AtomicYield {
		gosimruntime.Yield()
	}
	return *ptr
}

//go:norace
func InternalRuntimeAtomic_StoreReluintptr(ptr *uintptr, val uintptr) {
	if gosimruntime.AtomicYield {
		gosimruntime.Yield()
	}
	*ptr = val
}
