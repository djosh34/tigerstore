package main

import (
	"fmt"
)

type retainedSnapshot struct {
	ForcedGC    bool              `json:"forced_gc"`
	HeapAlloc   uint64            `json:"heap_alloc_bytes"`
	HeapObjects uint64            `json:"heap_objects"`
	HeapInuse   uint64            `json:"heap_inuse_bytes"`
	HeapSys     uint64            `json:"heap_sys_bytes"`
	ProfileRate int               `json:"mem_profile_rate"`
	StrategyEnv string            `json:"strategy_env"`
	Allocator   map[string]uint64 `json:"allocator"`
}

// This checks the managed workspace only, not live database/log memory or all
// process mallocs. The latter remain separately measured, never claimed zero.
func validateAllocator(a map[string]uint64, mode uint64) error {
	if a == nil {
		if mode == 0 {
			return nil
		} // Pristine base has no prototype stats method.
		return fmt.Errorf("allocator stats unavailable for mode %d", mode)
	}
	for _, key := range []string{"mode", "slot_budget", "byte_budget", "per_connection_slots", "held_slots", "held_bytes", "peak_slots", "peak_bytes", "denied_count", "fallback_allocations"} {
		if _, ok := a[key]; !ok {
			return fmt.Errorf("allocator stat missing %s", key)
		}
	}
	if a["mode"] != mode {
		return fmt.Errorf("allocator mode=%d, want %d", a["mode"], mode)
	}
	if a["held_slots"] > a["slot_budget"] || a["peak_slots"] > a["slot_budget"] || a["held_bytes"] > a["byte_budget"] || a["peak_bytes"] > a["byte_budget"] {
		return fmt.Errorf("allocator budget exceeded: %v", a)
	}
	if mode != 0 && (a["slot_budget"] == 0 || a["byte_budget"] == 0 || a["per_connection_slots"] == 0 || a["per_connection_slots"] > a["slot_budget"]) {
		return fmt.Errorf("invalid allocator quotas: %v", a)
	}
	if a["fallback_allocations"] != 0 {
		return fmt.Errorf("unbounded allocation fallback: %v", a)
	}
	return nil
}
