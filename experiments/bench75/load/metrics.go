package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// These counters include all leader-process allocations, not only RPC handlers.
type heapSnapshot struct {
	Objects   float64            `json:"allocated_objects"`
	Bytes     float64            `json:"allocated_bytes"`
	HeapBytes float64            `json:"heap_object_bytes"`
	GCCPU     float64            `json:"gc_cpu_seconds"`
	BusyCPU   float64            `json:"runtime_nonidle_cpu_seconds"`
	HasGCCPU  bool               `json:"has_gc_cpu"`
	Raw       map[string]float64 `json:"raw"`
}

func parseHeap(b []byte, kind string) (heapSnapshot, error) {
	m := map[string]float64{}
	var objects, allocated, heap string
	switch kind {
	case "runtime":
		if err := json.Unmarshal(b, &m); err != nil {
			return heapSnapshot{}, err
		}
		objects, allocated, heap = "/gc/heap/allocs:objects", "/gc/heap/allocs:bytes", "/memory/classes/heap/objects:bytes"
	case "etcd":
		for line := range strings.SplitSeq(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && strings.HasPrefix(f[0], "go_memstats_") {
				v, err := strconv.ParseFloat(f[1], 64)
				if err != nil {
					return heapSnapshot{}, err
				}
				m[f[0]] = v
			}
		}
		objects, allocated, heap = "go_memstats_mallocs_total", "go_memstats_alloc_bytes_total", "go_memstats_heap_alloc_bytes"
	default:
		return heapSnapshot{}, errors.New("unknown metrics kind")
	}
	for _, k := range []string{objects, allocated, heap} {
		if _, ok := m[k]; !ok {
			return heapSnapshot{}, fmt.Errorf("missing heap metric %s", k)
		}
	}
	s := heapSnapshot{Objects: m[objects], Bytes: m[allocated], HeapBytes: m[heap], Raw: m}
	gc, hasGC := m["/cpu/classes/gc/total:cpu-seconds"]
	total, hasTotal := m["/cpu/classes/total:cpu-seconds"]
	idle, hasIdle := m["/cpu/classes/idle:cpu-seconds"]
	if hasGC && hasTotal && hasIdle {
		s.HasGCCPU, s.GCCPU, s.BusyCPU = true, gc, total-idle
	}
	return s, nil
}

func sampleHeap(url, kind string) (heapSnapshot, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	r, err := client.Get(url)
	if err != nil {
		return heapSnapshot{}, err
	}
	b, readErr := io.ReadAll(r.Body)
	closeErr := r.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return heapSnapshot{}, err
	}
	if r.StatusCode != http.StatusOK {
		return heapSnapshot{}, fmt.Errorf("metrics returned %s", r.Status)
	}
	return parseHeap(b, kind)
}

func heapDelta(before, after heapSnapshot, n int) (map[string]any, error) {
	if n < 1 || after.Objects < before.Objects || after.Bytes < before.Bytes {
		return nil, errors.New("invalid allocation delta")
	}
	result := map[string]any{
		"objects_per_put":  (after.Objects - before.Objects) / float64(n),
		"bytes_per_put":    (after.Bytes - before.Bytes) / float64(n),
		"heap_bytes_after": after.HeapBytes,
		"before":           before,
		"after":            after,
	}
	// Keep the original JSON names for the archived Put-only reports.
	result["objects_per_op"] = result["objects_per_put"]
	result["bytes_per_op"] = result["bytes_per_put"]
	if before.HasGCCPU && after.HasGCCPU {
		gc, busy := after.GCCPU-before.GCCPU, after.BusyCPU-before.BusyCPU
		if gc < 0 || busy <= 0 {
			return nil, errors.New("invalid GC CPU delta")
		}
		result["gc_cpu_share"] = gc / busy
		result["gc_cpu_us_per_put"] = gc * 1e6 / float64(n)
		result["gc_cpu_us_per_op"] = result["gc_cpu_us_per_put"]
	} else {
		result["gc_cpu_share"] = nil
	}
	return result, nil
}
