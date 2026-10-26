package sim

import (
	"github.com/djosh34/tigerstore/internal/simfs"
	"google.golang.org/protobuf/proto"
	"unsafe"
)

// mem448 measures reachable payloads, not allocator sizes. The corresponding
// full-GC heap profiles include allocations unreachable from these roots too.
func (s *Sim) mem448(stage string) {
	reqBytes, respBytes, logBytes, traceBytes, valuesBytes, stringsBytes := 0, 0, 0, 0, 0, 0
	for _, r := range s.history {
		if r.req != nil {
			reqBytes += proto.Size(r.req)
		}
		if r.Resp != nil {
			respBytes += proto.Size(r.Resp)
		}
	}
	for _, r := range s.log {
		logBytes += len(r.data)
	}
	for _, r := range s.trace {
		traceBytes += len(r)
	}
	if s.values != nil {
		for _, v := range s.values.bytes {
			valuesBytes += len(v)
		}
		for _, v := range s.values.strings {
			stringsBytes += len(v)
		}
	}
	watchResponses, watchEvents, watchBytes := 0, 0, 0
	add := func(rr []WatchRecord) {
		for _, r := range rr {
			watchResponses++
			watchEvents += len(r.Resp.Events)
			watchBytes += proto.Size(r.Resp)
		}
	}
	for _, w := range s.watchers {
		add(w.responses)
		for _, sg := range w.ended {
			add(sg.responses)
		}
	}
	mark("MEM448.%s virtual=%d objects=%d ops=%d record-size=%d request-proto=%d response-proto=%d watch-responses=%d watch-events=%d watch-proto=%d watch-record-size=%d log-records=%d log-data=%d log-record-size=%d trace-lines=%d trace-bytes=%d values-bytes=%d values-strings=%d checkpoints=%d budget=%d capacity=%d", stage, s.now()/Second, s.cfg.Objects, len(s.history), unsafe.Sizeof(Record{}), reqBytes, respBytes, watchResponses, watchEvents, watchBytes, unsafe.Sizeof(WatchRecord{}), len(s.log), logBytes, unsafe.Sizeof(logRec{}), len(s.trace), traceBytes, valuesBytes, stringsBytes, len(s.checkpoints), s.cfg.Faults.DiskBudget, s.cfg.Faults.DiskCapacity)
	for _, n := range s.nodes {
		mark("DISK448.%s node=%d memory=%+v crash-image=%d", stage, n.id, n.disk.Memory(), mapBytes448(n.crashImage))
	}
	var ck simfs.Memory
	for _, c := range s.checkpoints {
		for _, d := range c.disks {
			m := d.Memory()
			ck.Live += m.Live
			ck.DataCap += m.DataCap
			ck.DurableCap += m.DurableCap
			ck.PendingCap += m.PendingCap
			ck.Files += m.Files
		}
	}
	mark("CHECKPOINT448.%s memory=%+v", stage, ck)
}

func mapBytes448(m map[string][]byte) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}
