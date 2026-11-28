package wl

import (
	"encoding/binary"
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
)

// TestATurnsBatchIsCappedAtTheRequestLimit is #816: a turn that takes in
// four puts of 600 KiB, 2.4 MiB in all, proposes them as two entries of
// two puts, each within the request limit (requestLimit), not as one
// entry of 2.4 MiB, which neither a ring sized for the largest request
// nor a peer's message (maxPeerMsg) is sized for.
func TestATurnsBatchIsCappedAtTheRequestLimit(t *testing.T) {
	n, ms := leaderNode(t)
	value := make([]byte, 600<<10)
	for i := range 4 {
		n.addOp(noHandle, kPut, appendBytesField(appendBytesField(nil, 1, fmt.Appendf(nil, "/big/%d", i)), 2, value))
	}
	n.proposeBatch(n.now)
	var sizes []int
	puts := 0
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		for _, m := range rd.Messages {
			if m.GetTo() == raft.LocalAppendThread {
				if err := ms.Append(m.GetEntries()); err != nil {
					t.Fatal(err)
				}
				for _, e := range m.GetEntries() {
					if isBatch(e.GetData()) {
						sizes = append(sizes, len(e.GetData()))
						puts += int(binary.LittleEndian.Uint32(e.GetData()[12:]))
					}
				}
			}
			for _, r := range m.GetResponses() {
				_ = n.rn.Step(r)
			}
		}
	}
	if len(sizes) != 2 || puts != 4 {
		t.Fatalf("the turn's 4 puts of 600 KiB went into entries of %v bytes, %d puts in all; want 2 entries of 2 puts", sizes, puts)
	}
	for _, k := range sizes {
		if k > maxRequestBytes {
			t.Fatalf("an entry of %d bytes, past the request limit %d", k, maxRequestBytes)
		}
	}
}
