package wl

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
)

// proposedBatch is a batch a test node proposed, as its log took it: the
// keys of its puts, in order, and the callers its forward table answers.
type proposedBatch struct {
	size    int
	keys    []string
	callers []handle
}

// proposed hands n's Raft Ready to ms and returns the batches its log
// took.
func proposed(t *testing.T, n *node, ms *raft.MemoryStorage) []proposedBatch {
	t.Helper()
	var out []proposedBatch
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		for _, m := range rd.Messages {
			if m.GetTo() == raft.LocalAppendThread {
				if err := ms.Append(m.GetEntries()); err != nil {
					t.Fatal(err)
				}
				for _, e := range m.GetEntries() {
					data := e.GetData()
					if !isBatch(data) {
						continue
					}
					b := proposedBatch{size: len(data)}
					for p := batchHdr; p+opHdr <= len(data); {
						l := int(binary.LittleEndian.Uint32(data[p+1:]))
						if _, _, _, key, _, err := field(data[p+opHdr : p+opHdr+l]); err == nil {
							b.keys = append(b.keys, string(key))
						}
						p += opHdr + l
					}
					seq := binary.LittleEndian.Uint64(data[4:])
					b.callers = slices.Clone(n.fwd[seq&(fwdSlots-1)].handles)
					out = append(out, b)
				}
			}
			for _, r := range m.GetResponses() {
				_ = n.rn.Step(r)
			}
		}
	}
	return out
}

func putReqOf(key string, value []byte) []byte {
	return appendBytesField(appendBytesField(nil, 1, []byte(key)), 2, value)
}

// TestATurnsBatchIsCappedAtTheRequestLimit is #816: a turn that takes in
// four puts of 600 KiB, 2.4 MiB in all, proposes them as two entries of
// two puts, each within the request limit (requestLimit), not as one
// entry of 2.4 MiB, which neither a ring sized for the largest request
// nor a peer's message (maxPeerMsg) is sized for. The puts keep their
// order across the split, and each batch answers its own callers. A put
// exactly at the limit is taken, alone in its batch after a small one,
// and one a byte larger is refused as too large.
func TestATurnsBatchIsCappedAtTheRequestLimit(t *testing.T) {
	n, ms := leaderNode(t)
	value := make([]byte, 600<<10)
	for i := range 4 {
		n.addOp(handle(i+1), kPut, putReqOf(fmt.Sprintf("/big/%d", i), value))
	}
	n.proposeBatch(n.now)
	got := proposed(t, n, ms)
	want := []proposedBatch{
		{keys: []string{"/big/0", "/big/1"}, callers: []handle{1, 2}},
		{keys: []string{"/big/2", "/big/3"}, callers: []handle{3, 4}},
	}
	if len(got) != len(want) {
		t.Fatalf("the turn's 4 puts of 600 KiB went into %d entries (%+v); want 2 of 2 puts", len(got), got)
	}
	for i, b := range got {
		if !slices.Equal(b.keys, want[i].keys) || !slices.Equal(b.callers, want[i].callers) || b.size > maxRequestBytes {
			t.Fatalf("entry %d: %d bytes, keys %q, callers %v; want at most %d bytes, keys %q, callers %v",
				i, b.size, b.keys, b.callers, maxRequestBytes, want[i].keys, want[i].callers)
		}
	}

	// A put exactly at the limit, as checkRequest counts it.
	size := func(m []byte) int { return raftRequestBytes(len(m)) + len(m) }
	vl := maxRequestBytes - size(putReqOf("/cap", make([]byte, maxRequestBytes-64))) + maxRequestBytes - 64
	at := putReqOf("/cap", make([]byte, vl))
	if size(at) != maxRequestBytes {
		t.Fatalf("no put of exactly %d bytes: %d", maxRequestBytes, size(at))
	}
	if err := checkRequest(kPut, at, 0, n.requestLimit()); err != nil {
		t.Fatalf("a put at the limit: %v", err)
	}
	if err := checkRequest(kPut, putReqOf("/cap", make([]byte, vl+1)), 0, n.requestLimit()); err == nil {
		t.Fatal("a put a byte past the limit was taken")
	}
	n.addOp(handle(5), kPut, putReqOf("/small", []byte("v")))
	n.addOp(handle(6), kPut, at)
	n.proposeBatch(n.now)
	got = proposed(t, n, ms)
	if len(got) != 2 || !slices.Equal(got[0].keys, []string{"/small"}) || !slices.Equal(got[1].keys, []string{"/cap"}) ||
		!slices.Equal(got[0].callers, []handle{5}) || !slices.Equal(got[1].callers, []handle{6}) {
		t.Fatalf("a small put, then one at the limit: %+v; want each alone in its entry", got)
	}
}
