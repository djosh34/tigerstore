package go123

import "github.com/jellevandenhooff/gosim/gosimruntime"

// Go 1.27.2 src/internal/sync/mutex.go:105 and runtime.go:46-47 moved the
// spin decision from sync. Reuse pinned gosim's sync.go:47-50 (never host-spin).
func InternalSync_runtime_canSpin(i int) bool {
 gosimruntime.Port333Reached("internal/sync.runtime_canSpin")
 return Sync_runtime_canSpin(i)
}

// Moved mutex starvation clock: same simulated clock as the old sync hook
// (pinned gosim internal/hooks/go123/sync.go:41-43).
func InternalSync_runtime_nanotime() int64 {
 gosimruntime.Port333Reached("internal/sync.runtime_nanotime")
 return Sync_runtime_nanotime()
}

// Go 1.27.2 internal/sync/runtime.go:26, mutex.go:150; same signature as
// pinned gosim's internal/hooks/go123/sync.go:15-17.
func InternalSync_runtime_SemacquireMutex(addr *uint32, lifo bool, skipframes int) {
 gosimruntime.Port333Reached("internal/sync.runtime_SemacquireMutex")
 Sync_runtime_SemacquireMutex(addr, lifo, skipframes)
}
