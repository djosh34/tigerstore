package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

func InternalCpu_getisar0() uint64 {
	gosimruntime.Unimplemented("internal/cpu.getisar0")
	panic("unreachable")
}

func InternalCpu_getMIDR() uint64 {
	gosimruntime.Unimplemented("internal/cpu.getMIDR")
	panic("unreachable")
}

func InternalCpu_cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32) {
	gosimruntime.Unimplemented("internal/cpu.cpuid")
	panic("unreachable")
}

func InternalCpu_xgetbv() (eax, edx uint32) {
	gosimruntime.Unimplemented("internal/cpu.xgetbv")
	panic("unreachable")
}

func InternalCpu_getGOAMD64level() int32 {
	gosimruntime.Unimplemented("internal/cpu.getGOAMD64level")
	panic("unreachable")
}
