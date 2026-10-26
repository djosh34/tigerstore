package go127

import (
	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func Sync_runtime_Semacquire(addr *uint32) {
	gosimruntime.Semacquire(addr, false)
}

func Sync_runtime_Semrelease(addr *uint32, handoff bool, skipframes int) {
	gosimruntime.Semrelease(addr)
}

func Sync_runtime_SemacquireRWMutexR(addr *uint32, lifo bool, skipframes int) {
	gosimruntime.Semacquire(addr, lifo)
}

func Sync_runtime_SemacquireRWMutex(addr *uint32, lifo bool, skipframes int) {
	gosimruntime.Semacquire(addr, lifo)
}

func Sync_runtime_registerPoolCleanup(cleanup func()) {
	// noop
}

func Sync_runtime_procPin() int {
	// noop
	// TODO: randomize?
	return 0
}

func Sync_runtime_procUnpin() {
	// noop
}

type NotifyList struct {
	inner gosimruntime.NotifyList
}

func Sync_runtime_notifyListAdd(l *NotifyList) uint32 {
	return l.inner.Add()
}

func Sync_runtime_notifyListWait(l *NotifyList, t uint32) {
	l.inner.Wait(t)
}

func Sync_runtime_notifyListNotifyAll(l *NotifyList) {
	l.inner.NotifyAll()
}

func Sync_runtime_notifyListNotifyOne(l *NotifyList) {
	l.inner.NotifyOne()
}

// Ensure that sync and runtime agree on size of notifyList.
func Sync_runtime_notifyListCheck(size uintptr) {
	// noop
}

func Sync_runtime_randn(n uint32) uint32 {
	return gosimruntime.Fastrandn(n)
}

func Sync_runtime_SemacquireWaitGroup(addr *uint32, synctestDurable bool) {
	// gosim has no synctest bubbles, so synctestDurable is always false.
	gosimruntime.Semacquire(addr, false)
}

func Sync_throw(str string) {
	gosimruntime.Fatal(str)
}

func Sync_fatal(str string) {
	gosimruntime.Fatal(str)
}
