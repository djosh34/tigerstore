package main

import (
	"testing"
	"unsafe"
)

func TestKernelABI(t *testing.T) {
	checks := []struct {
		name      string
		got, want uintptr
	}{
		{"params size", unsafe.Sizeof(params{}), 120},
		{"params sq offsets", unsafe.Offsetof(params{}.S), 40},
		{"params cq offsets", unsafe.Offsetof(params{}.C), 80},
		{"SQ array offset", unsafe.Offsetof(offsets{}.Array), 24},
		{"CQ cqe offset", unsafe.Offsetof(cqOffsets{}.CQEs), 20},
		{"CQ overflow offset", unsafe.Offsetof(cqOffsets{}.Overflow), 16},
		{"sqe size", unsafe.Sizeof(sqe{}), 64},
		{"sqe user data", unsafe.Offsetof(sqe{}.Data), 32},
		{"sqe buffer index", unsafe.Offsetof(sqe{}.Buf), 40},
		{"cqe size", unsafe.Sizeof(cqe{}), 16},
		{"buffer registration size", unsafe.Sizeof(bufreg{}), 40},
		{"provided buffer size", unsafe.Sizeof(pbuf{}), 16},
		{"provided buffer tail overlay", unsafe.Offsetof(pbuf{}.Reserved), 14},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}
