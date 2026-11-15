package wl

import (
	"bytes"
	"encoding/base64"
	"runtime/pprof"
)

// dumpHeap logs a heap profile, base64 of pprof's gzipped protobuf, each
// time Go's live heap passes the next 256 MiB step. A diagnostic of #602's
// memory overrun, to be removed.
func (l *loop) dumpHeap(m memStats) {
	step := m.goLive >> 28
	if step <= l.heapStep {
		return
	}
	l.heapStep = step
	var b bytes.Buffer
	if err := pprof.Lookup("heap").WriteTo(&b, 0); err != nil {
		l.logger.Printf("heap profile: %v", err)
		return
	}
	l.logger.Printf("heap profile at live %d MB: %s", m.goLive>>20, base64.StdEncoding.EncodeToString(b.Bytes()))
}
