package go127

import "unsafe"

func InternalGodebug_setUpdate(update func(string, string)) {
}

func InternalGodebug_registerMetric(name string, read func() uint64) {
}

func InternalGodebug_setNewIncNonDefault(newIncNonDefault func(string) func()) {
}

func InternalGodebug_write(fd uintptr, p unsafe.Pointer, n int32) int32 {
	return n
}
