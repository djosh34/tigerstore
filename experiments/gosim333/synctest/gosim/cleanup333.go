package gosimruntime

import "runtime"

// EXPERIMENT ONLY: no cleanup scheduling or reachability validation. This pairs
// with strong-backed weak handles: a TLS cache cleanup captures a weak handle,
// which our approximation makes strong; host AddCleanup correctly rejects that
// closure (Go 1.27.2 runtime/mcleanup.go:164-181; crypto/tls/cache.go:31-38).
// Do not send translated callbacks to host-GC goroutines. runtime.Cleanup's
// zero handle has a no-op Stop (runtime/mcleanup.go:216-219).
func AddCleanup[T, S any](ptr *T, cleanup func(S), arg S) runtime.Cleanup {
 Port333Reached("runtime.AddCleanup(no-GC)")
 if ptr == nil { panic("runtime.AddCleanup: ptr is nil") }
 return runtime.Cleanup{}
}
