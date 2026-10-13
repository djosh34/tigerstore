package peer

import "slices"

// QueueObservation copies existing queue accounting without adding work to
// Send or the peer writers. Byte counts are retained-message estimates, not
// serialized wire bytes. A reservation can be visible before its enqueue.
// Fields load independently and exclude the writer's current message,
// buffered TLS/frame bytes and kernel socket buffers.
type QueueObservation struct {
	ID              uint64 `json:"id"`
	ControlBytes    int64  `json:"control_bytes"`
	DataBytes       int64  `json:"data_bytes"`
	ControlMessages int    `json:"control_messages"`
	DataMessages    int    `json:"data_messages"`
}

// ObserveQueues may run concurrently with Send, writers and Close. The
// sender map is immutable after Start. Results are sorted by Raft peer ID.
func (t *TCP) ObserveQueues() []QueueObservation {
	out := make([]QueueObservation, 0, len(t.senders))
	for id, s := range t.senders {
		out = append(out, QueueObservation{
			ID: id, ControlBytes: s.ctrlBytes.Load(), DataBytes: s.dataBytes.Load(),
			ControlMessages: len(s.ctrl), DataMessages: len(s.data),
		})
	}
	slices.SortFunc(out, func(a, b QueueObservation) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return out
}
