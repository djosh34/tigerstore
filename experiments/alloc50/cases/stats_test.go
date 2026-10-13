package main

import "testing"

func TestAllocatorBounds(t *testing.T) {
	fixture := func() map[string]uint64 {
		return map[string]uint64{
			"mode": 1, "slot_budget": 4096, "byte_budget": 64 << 20, "per_connection_slots": 256,
			"held_slots": 256, "held_bytes": 1 << 20, "peak_slots": 257, "peak_bytes": 2 << 20, "denied_count": 100, "fallback_allocations": 0,
		}
	}
	if err := validateAllocator(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := validateAllocator(nil, 1); err == nil {
		t.Fatal("missing prototype stats accepted")
	}
	if err := validateAllocator(fixture(), 1); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"held_slots", "peak_slots", "held_bytes", "peak_bytes", "fallback_allocations", "mode", "per_connection_slots"} {
		a := fixture()
		switch key {
		case "held_slots", "peak_slots":
			a[key] = a["slot_budget"] + 1
		case "held_bytes", "peak_bytes":
			a[key] = a["byte_budget"] + 1
		case "fallback_allocations":
			a[key] = 1
		case "mode":
			a[key] = 2
		case "per_connection_slots":
			a[key] = 0
		}
		if err := validateAllocator(a, 1); err == nil {
			t.Fatalf("invalid %s accepted", key)
		}
	}
	a := fixture()
	delete(a, "held_bytes")
	if err := validateAllocator(a, 1); err == nil {
		t.Fatal("missing gauge accepted")
	}
}
