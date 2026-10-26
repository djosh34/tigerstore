package go127

import (
	"math"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func RuntimeDebug_WriteHeapDump(fd uintptr) {
	gosimruntime.Unimplemented("runtime/debug.WriteHeapDump")
}

func RuntimeDebug_SetTraceback(level string) {
	gosimruntime.Unimplemented("runtime/debug.SetTraceback")
}

func RuntimeDebug_modinfo() string {
	gosimruntime.Unimplemented("runtime/debug.modinfo")
	panic("unreachable")
}

// RuntimeDebug_readGCStats reports that no garbage collection ran: the
// collector is not part of the simulation.
func RuntimeDebug_readGCStats(pauses *[]time.Duration) {
	// The last collection, the number of collections and the total pause.
	*pauses = append((*pauses)[:0], 0, 0, 0)
}

// The garbage collector is not part of the simulation, so its settings
// change nothing; they are kept so that setting one returns the one before.

func RuntimeDebug_freeOSMemory() {}

var (
	gcPercent   int32 = 100
	memoryLimit int64 = math.MaxInt64
)

func RuntimeDebug_setMaxStack(int) int {
	gosimruntime.Unimplemented("runtime/debug.setMaxStack")
	panic("unreachable")
}

func RuntimeDebug_setGCPercent(percent int32) int32 {
	old := gcPercent
	gcPercent = percent
	return old
}

func RuntimeDebug_setPanicOnFault(bool) bool {
	gosimruntime.Unimplemented("runtime/debug.setPanicOnFault")
	panic("unreachable")
}

func RuntimeDebug_setMaxThreads(int) int {
	gosimruntime.Unimplemented("runtime/debug.setMaxThreads")
	panic("unreachable")
}

func RuntimeDebug_setMemoryLimit(limit int64) int64 {
	old := memoryLimit
	if limit >= 0 {
		memoryLimit = limit
	}
	return old
}
