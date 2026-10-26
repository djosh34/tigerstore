package go127

import "unsafe"

// A weak pointer holds its object strongly, so it never becomes nil. The
// garbage collector is not part of the simulation, and Go does not promise
// that it collects an unreachable object.

func Weak_runtime_registerWeakPointer(p unsafe.Pointer) unsafe.Pointer {
	return p
}

func Weak_runtime_makeStrongFromWeak(u unsafe.Pointer) unsafe.Pointer {
	return u
}
