// Package dataprobe is host-only instrumentation for throwaway research #449.
// Native imports deliberately bypass gosim's virtual clock/filesystem. None
// of the measurements is read back into the simulation's decisions.
package dataprobe

import (
	"encoding/json" //gosim:notranslate
	"os"            //gosim:notranslate
	"runtime"       //gosim:notranslate
	"runtime/pprof" //gosim:notranslate
	"time"          //gosim:notranslate
)

func Mark(phase string) {
	f, err := os.OpenFile(os.Getenv("DATA449_PHASES"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	_ = json.NewEncoder(f).Encode(struct {
		Phase                         string
		WallNS                        int64
		HeapAlloc, HeapInuse, HeapSys uint64
	}{phase, time.Now().UnixNano(), m.HeapAlloc, m.HeapInuse, m.HeapSys})
	_ = f.Close()
}

func Profile(phase string) {
	path := os.Getenv("DATA449_HEAP")
	if path == "" {
		return
	}
	f, err := os.Create(path + "-" + phase + ".pprof")
	if err != nil {
		return
	}
	// No explicit GC: avoid changing the measured scenario's GC schedule.
	_ = pprof.WriteHeapProfile(f)
	_ = f.Close()
}
