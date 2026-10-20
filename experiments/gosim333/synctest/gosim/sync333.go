package go123

import "github.com/jellevandenhooff/gosim/gosimruntime"

// Go 1.27.2 src/internal/sync/mutex.go:105 and runtime.go:46-47 moved the
// spin decision from sync. Reuse pinned gosim's sync.go:47-50 (never host-spin).
func InternalSync_runtime_canSpin(i int) bool {
 gosimruntime.Port333Reached("internal/sync.runtime_canSpin")
 return Sync_runtime_canSpin(i)
}
