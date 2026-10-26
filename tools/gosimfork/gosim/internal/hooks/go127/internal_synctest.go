package go127

import "unsafe"

// gosim runs no synctest bubbles: the whole simulation already has a fake
// clock and a deterministic scheduler. A goroutine is never in a bubble.
func InternalSynctest_IsInBubble() bool {
	return false
}

// Outside a bubble, nothing is associated with one.

func InternalSynctest_associate(p unsafe.Pointer) int {
	return 0 // internal/synctest.Unbubbled
}

func InternalSynctest_disassociate(p unsafe.Pointer) {}

func InternalSynctest_isAssociated(p unsafe.Pointer) bool {
	return false
}
