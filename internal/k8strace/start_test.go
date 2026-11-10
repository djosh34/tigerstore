package k8strace

import (
	"testing"
)

// TestFrom applies the calls before a start to the preload: a create, an
// update whose Txn failed and so wrote nothing, a delete, a grant and a
// put with its lease.
func TestFrom(t *testing.T) {
	tr := &Trace{
		Keys:    []string{"/a", "/b", "/c"},
		Leases:  []int64{60},
		Preload: []Item{{Key: 0, Size: 10}, {Key: 1, Size: 20, Lease: -1}},
		Calls: []Call{
			{Kind: CallTxn, Op: Op{Kind: OpTxn, Compare: []Compare{{Target: TargetMod, Key: 2}}, Success: []Op{{Kind: OpPut, Key: 2, End: -1, Size: 30}}}},
			{Kind: CallTxn, Failed: true, Op: Op{Kind: OpTxn, Compare: []Compare{{Target: TargetMod, Key: 0, Rev: -1}}, Success: []Op{{Kind: OpPut, Key: 0, End: -1, Size: 99}}, Failure: []Op{{Kind: OpRange, Key: 0, End: -1}}}},
			{Kind: CallDelete, Op: Op{Kind: OpDelete, Key: 1, End: -1}},
			{Kind: CallLeaseGrant, TTL: 3600},
			{Kind: CallPut, Op: Op{Kind: OpPut, Key: 1, End: -1, Size: 40, Lease: 4}},
			{Kind: CallRange, Op: Op{Kind: OpRange, Key: 0, End: -1}},
		},
	}
	s := tr.From(5)
	want := []StartItem{
		{Key: 0, Size: 10, Writer: -1},
		{Key: 1, Size: 40, Lease: 2, Writer: 5},
		{Key: 2, Size: 30, Writer: 1},
	}
	if len(s.Items) != len(want) {
		t.Fatalf("items %+v, want %+v", s.Items, want)
	}
	for i := range want {
		if s.Items[i] != want[i] {
			t.Errorf("item %d is %+v, want %+v", i, s.Items[i], want[i])
		}
	}
	if len(s.Leases) != 2 || s.Leases[0] != 60 || s.Leases[1] != 3600 {
		t.Errorf("leases %v, want the preload's 60 and the grant's 3600", s.Leases)
	}
	// A compare against the write that left a key finds its item; one
	// against a write the key no longer shows does not.
	if i, ok := s.Item(2, 1); !ok || i != 2 {
		t.Errorf("/c from call 0: item %d, %v", i, ok)
	}
	if _, ok := s.Item(1, -2); ok {
		t.Error("/b was deleted and put again: the preload's write is gone")
	}
	if !s.Before(1) || !s.Before(-1) || s.Before(6) || s.Before(0) || s.Before(RevStale) {
		t.Error("Before: calls 0 to 4 and the preload are before the start of call 5")
	}
}
