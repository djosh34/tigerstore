package main

// Issue #484 measurement only: profile exactly the measurement window and
// emit boundary markers for the external server profiler/counter sampler.
import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/pprof"
)

type researchOp struct {
	Ops          int64 `json:"ops"`
	RequestBytes int64 `json:"request_pb_bytes"`
	ReplyBytes   int64 `json:"reply_pb_bytes"`
	ListKeys     int64 `json:"list_keys"`
	LatencyNS    int64 `json:"latency_ns"`
}

var researchProfile *os.File

func researchBoundary(i int) {
	dir := os.Getenv("R484_DIR")
	if dir == "" {
		return
	}
	if i == 0 {
		if os.Getenv("R484_PROFILE") == "1" {
			f, err := os.Create(filepath.Join(dir, "generator.pprof"))
			if err != nil {
				panic(err)
			}
			researchProfile = f
			if err := pprof.StartCPUProfile(f); err != nil {
				panic(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "start"), nil, 0644); err != nil {
			panic(err)
		}
	} else {
		if err := os.WriteFile(filepath.Join(dir, "stop"), nil, 0644); err != nil {
			panic(err)
		}
		if researchProfile != nil {
			pprof.StopCPUProfile()
			researchProfile.Close()
		}
	}
}
func writeResearch(loads []load, watchBytes int64) {
	dir := os.Getenv("R484_DIR")
	if dir == "" {
		return
	}
	var sum [5]researchOp
	for _, l := range loads {
		for i, r := range l.(*mixedLoad).research {
			sum[i].Ops += r.Ops
			sum[i].RequestBytes += r.RequestBytes
			sum[i].ReplyBytes += r.ReplyBytes
			sum[i].ListKeys += r.ListKeys
			sum[i].LatencyNS += r.LatencyNS
		}
	}
	b, err := json.MarshalIndent(struct {
		Ops        [5]researchOp `json:"ops_get_list_update_create_delete"`
		WatchBytes int64         `json:"watch_pb_bytes"`
	}{sum, watchBytes}, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "detail.json"), b, 0644); err != nil {
		panic(err)
	}
}
