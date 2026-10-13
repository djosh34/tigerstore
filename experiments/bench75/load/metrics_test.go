package main

import (
	"math"
	"testing"
)

func TestHeapMetrics(t *testing.T) {
	cases := []struct {
		kind, input string
		gc          bool
	}{
		{"runtime", `{"/gc/heap/allocs:objects":100,"/gc/heap/allocs:bytes":2048,"/memory/classes/heap/objects:bytes":1024,"/cpu/classes/gc/total:cpu-seconds":2,"/cpu/classes/total:cpu-seconds":20,"/cpu/classes/idle:cpu-seconds":5}`, true},
		{"etcd", "# HELP comment\ngo_memstats_mallocs_total 100\ngo_memstats_alloc_bytes_total 2048\ngo_memstats_heap_alloc_bytes 1024\n", false},
	}
	for _, c := range cases {
		t.Run(c.kind, func(t *testing.T) {
			before, err := parseHeap([]byte(c.input), c.kind)
			if err != nil {
				t.Fatal(err)
			}
			if before.Objects != 100 || before.Bytes != 2048 || before.HeapBytes != 1024 || before.HasGCCPU != c.gc {
				t.Fatalf("bad snapshot: %+v", before)
			}
			after := before
			after.Objects += 100
			after.Bytes += 2048
			after.GCCPU += 1
			after.BusyCPU += 10
			delta, err := heapDelta(before, after, 10)
			if err != nil {
				t.Fatal(err)
			}
			if delta["objects_per_put"] != float64(10) || delta["bytes_per_put"] != 204.8 {
				t.Fatalf("bad delta: %+v", delta)
			}
			if c.gc && math.Abs(delta["gc_cpu_share"].(float64)-.1) > 1e-12 {
				t.Fatalf("bad GC share: %+v", delta)
			}
			if !c.gc && delta["gc_cpu_share"] != nil {
				t.Fatal("must not report pause time as GC CPU")
			}
		})
	}
}

func TestBadHeapMetrics(t *testing.T) {
	for _, kind := range []string{"runtime", "etcd", "unknown"} {
		if _, err := parseHeap([]byte("{}"), kind); err == nil {
			t.Fatalf("accepted missing metrics for %s", kind)
		}
	}
	if _, err := heapDelta(heapSnapshot{Objects: 2}, heapSnapshot{Objects: 1}, 10); err == nil {
		t.Fatal("accepted decreasing counter")
	}
}
