package go123

import (
 "unsafe"
 "github.com/jellevandenhooff/gosim/gosimruntime"
)

// EXPERIMENT ONLY: strong-backed weak handles; NOT a weak/GC implementation.
// Go 1.27.2 weak/pointer.go:25-51 requires identity and permits Value to never
// return nil; retaining keys deliberately drops weak reachability and cleanup
// eviction. In the reached fips140cache/cache.go:33-47, this retains cached keys
// instead of exposing host-GC timing to the simulated scheduler.
func Weak_runtime_registerWeakPointer(p unsafe.Pointer) unsafe.Pointer {
 gosimruntime.Port333Reached("weak.runtime_registerWeakPointer")
 return p
}
func Weak_runtime_makeStrongFromWeak(p unsafe.Pointer) unsafe.Pointer {
 gosimruntime.Port333Reached("weak.runtime_makeStrongFromWeak")
 return p
}
