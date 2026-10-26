package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

// Go 1.24 moved Mutex into internal/sync, along with its hooks.

func InternalSync_runtime_SemacquireMutex(addr *uint32, lifo bool, skipframes int) {
	gosimruntime.Semacquire(addr, lifo)
}

func InternalSync_runtime_Semrelease(addr *uint32, handoff bool, skipframes int) {
	gosimruntime.Semrelease(addr)
}

// runtime_canSpin reports whether spinning makes sense at the moment. It never
// does: a simulated goroutine that spins keeps the others from running.
func InternalSync_runtime_canSpin(i int) bool {
	return false
}

// runtime_doSpin is never called, since runtime_canSpin is false.
func InternalSync_runtime_doSpin() {}

func InternalSync_runtime_nanotime() int64 {
	return gosimruntime.Nanotime()
}

func InternalSync_runtime_rand() uint64 {
	return gosimruntime.Fastrand64()
}

func InternalSync_throw(str string) {
	gosimruntime.Fatal(str)
}

func InternalSync_fatal(str string) {
	gosimruntime.Fatal(str)
}
