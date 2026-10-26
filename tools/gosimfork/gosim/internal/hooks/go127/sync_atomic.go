package go127

import (
	"sync/atomic" //gosim:notranslate
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/race"
)

func maybeAtomicYield() {
	if gosimruntime.AtomicYield {
		gosimruntime.Yield()
	}
}

func SyncAtomic_SwapInt32(addr *int32, new int32) (old int32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapInt32(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_SwapInt64(addr *int64, new int64) (old int64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapInt64(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_SwapUint32(addr *uint32, new uint32) (old uint32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapUint32(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_SwapUint64(addr *uint64, new uint64) (old uint64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapUint64(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_SwapUintptr(addr *uintptr, new uintptr) (old uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapUintptr(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_SwapPointer(addr *unsafe.Pointer, new unsafe.Pointer) (old unsafe.Pointer) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.SwapPointer(addr, new)
	}
	old = *addr
	*addr = new
	return
}

func SyncAtomic_CompareAndSwapInt32(addr *int32, old, new int32) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapInt32(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_CompareAndSwapInt64(addr *int64, old, new int64) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapInt64(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_CompareAndSwapUint32(addr *uint32, old, new uint32) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapUint32(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_CompareAndSwapUint64(addr *uint64, old, new uint64) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapUint64(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_CompareAndSwapUintptr(addr *uintptr, old, new uintptr) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapUintptr(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_CompareAndSwapPointer(addr *unsafe.Pointer, old, new unsafe.Pointer) (swapped bool) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.CompareAndSwapPointer(addr, old, new)
	}
	if *addr != old {
		return false
	}
	*addr = new
	return true
}

func SyncAtomic_AddInt32(addr *int32, delta int32) (new int32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AddInt32(addr, delta)
	}
	*addr += delta
	return *addr
}

func SyncAtomic_AddUint32(addr *uint32, delta uint32) (new uint32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AddUint32(addr, delta)
	}
	*addr += delta
	return *addr
}

func SyncAtomic_AddInt64(addr *int64, delta int64) (new int64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AddInt64(addr, delta)
	}
	*addr += delta
	return *addr
}

func SyncAtomic_AddUint64(addr *uint64, delta uint64) (new uint64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AddUint64(addr, delta)
	}
	*addr += delta
	return *addr
}

func SyncAtomic_AddUintptr(addr *uintptr, delta uintptr) (new uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AddUintptr(addr, delta)
	}
	*addr += delta
	return *addr
}

func SyncAtomic_AndInt32(addr *int32, delta int32) (new int32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AndInt32(addr, delta)
	}
	*addr &= delta
	return *addr
}

func SyncAtomic_AndUint32(addr *uint32, delta uint32) (new uint32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AndUint32(addr, delta)
	}
	*addr &= delta
	return *addr
}

func SyncAtomic_AndInt64(addr *int64, delta int64) (new int64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AndInt64(addr, delta)
	}
	*addr &= delta
	return *addr
}

func SyncAtomic_AndUint64(addr *uint64, delta uint64) (new uint64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AndUint64(addr, delta)
	}
	*addr &= delta
	return *addr
}

func SyncAtomic_AndUintptr(addr *uintptr, delta uintptr) (new uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.AndUintptr(addr, delta)
	}
	*addr &= delta
	return *addr
}

func SyncAtomic_OrInt32(addr *int32, delta int32) (new int32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.OrInt32(addr, delta)
	}
	*addr |= delta
	return *addr
}

func SyncAtomic_OrUint32(addr *uint32, delta uint32) (new uint32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.OrUint32(addr, delta)
	}
	*addr |= delta
	return *addr
}

func SyncAtomic_OrInt64(addr *int64, delta int64) (new int64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.OrInt64(addr, delta)
	}
	*addr |= delta
	return *addr
}

func SyncAtomic_OrUint64(addr *uint64, delta uint64) (new uint64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.OrUint64(addr, delta)
	}
	*addr |= delta
	return *addr
}

func SyncAtomic_OrUintptr(addr *uintptr, delta uintptr) (new uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.OrUintptr(addr, delta)
	}
	*addr |= delta
	return *addr
}

func SyncAtomic_LoadInt32(addr *int32) (val int32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadInt32(addr)
	}
	return *addr
}

func SyncAtomic_LoadInt64(addr *int64) (val int64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadInt64(addr)
	}
	return *addr
}

func SyncAtomic_LoadUint32(addr *uint32) (val uint32) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadUint32(addr)
	}
	return *addr
}

func SyncAtomic_LoadUint64(addr *uint64) (val uint64) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadUint64(addr)
	}
	return *addr
}

func SyncAtomic_LoadUintptr(addr *uintptr) (val uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadUintptr(addr)
	}
	return *addr
}

func SyncAtomic_LoadPointer(addr *unsafe.Pointer) (val unsafe.Pointer) {
	maybeAtomicYield()
	if race.Enabled {
		return atomic.LoadPointer(addr)
	}
	return *addr
}

func SyncAtomic_StoreInt32(addr *int32, val int32) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StoreInt32(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_StoreInt64(addr *int64, val int64) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StoreInt64(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_StoreUint32(addr *uint32, val uint32) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StoreUint32(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_StoreUint64(addr *uint64, val uint64) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StoreUint64(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_StoreUintptr(addr *uintptr, val uintptr) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StoreUintptr(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_StorePointer(addr *unsafe.Pointer, val unsafe.Pointer) {
	maybeAtomicYield()
	if race.Enabled {
		atomic.StorePointer(addr, val)
		return
	}
	*addr = val
}

func SyncAtomic_runtime_procPin() int {
	// noop
	// TODO: randomize?
	return 0
}

func SyncAtomic_runtime_procUnpin() {
	// noop
}
