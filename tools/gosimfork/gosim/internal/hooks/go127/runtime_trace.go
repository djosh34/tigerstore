package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

func RuntimeTrace_userTaskCreate(id, parentID uint64, taskType string) {
	gosimruntime.Unimplemented("runtime/trace.userTaskCreate")
}

func RuntimeTrace_userTaskEnd(id uint64) {
	gosimruntime.Unimplemented("runtime/trace.userTaskEnd")
}

func RuntimeTrace_userRegion(id, mode uint64, regionType string) {
	gosimruntime.Unimplemented("runtime/trace.userRegion")
}

func RuntimeTrace_userLog(id uint64, category, message string) {
	gosimruntime.Unimplemented("runtime/trace.userLog")
}
