package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type allocations struct {
	Mallocs      uint64          `json:"mallocs"`
	Bytes        uint64          `json:"bytes"`
	GCCPUSeconds float64         `json:"gc_cpu_seconds"`
	HeapAlloc    uint64          `json:"heap_alloc"`
	HeapInuse    uint64          `json:"heap_inuse"`
	HeapObjects  uint64          `json:"heap_objects"`
	Sys          uint64          `json:"sys"`
	NumGC        uint64          `json:"num_gc"`
	Batch        json.RawMessage `json:"batch"`
}

// Scrapes happen before and after the CPU window, after gRPC connection setup.
// Both stores expose cumulative runtime counters, not sampled heap profiles.
func sampleAllocations(urls string) ([]allocations, error) {
	if urls == "" {
		return nil, nil
	}
	client := &http.Client{Timeout: 15 * time.Second}
	var result []allocations
	for _, url := range strings.Split(urls, ",") {
		r, err := client.Get(url)
		if err != nil {
			return nil, err
		}
		b, readErr := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		closeErr := r.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if r.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("allocation scrape %s: %s", url, r.Status)
		}
		var a allocations
		if strings.HasPrefix(strings.TrimSpace(string(b)), "{") {
			if err := json.Unmarshal(b, &a); err != nil {
				return nil, err
			}
		} else {
			seen := 0
			for line := range strings.SplitSeq(string(b), "\n") {
				f := strings.Fields(line)
				if len(f) != 2 || (f[0] != "go_memstats_mallocs_total" && f[0] != "go_memstats_alloc_bytes_total") {
					continue
				}
				v, err := strconv.ParseFloat(f[1], 64)
				if err != nil || v < 0 {
					return nil, fmt.Errorf("invalid allocation metric %q", line)
				}
				if f[0] == "go_memstats_mallocs_total" {
					a.Mallocs = uint64(v)
				} else {
					a.Bytes = uint64(v)
				}
				seen++
			}
			if seen != 2 {
				return nil, fmt.Errorf("missing allocation metrics at %s", url)
			}
		}
		if a.Mallocs == 0 || a.Bytes == 0 {
			return nil, fmt.Errorf("empty allocation counters at %s", url)
		}
		result = append(result, a)
	}
	return result, nil
}
