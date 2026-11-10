package k8strace

import (
	"math"
	"slices"
	"testing"
	"time"
)

// TestChain follows the longest path through the waits: a write, a read
// that waits on it, and a write that waits on the read take the sum of
// their least times; a call that waits on nothing runs alongside.
func TestChain(t *testing.T) {
	tr := &Trace{Keys: []string{"/a"}, Calls: []Call{
		{Kind: CallPut},
		{Kind: CallRange, Waits: []int32{0}, Op: Op{End: -1}},
		{Kind: CallTxn, Waits: []int32{1}, Op: Op{Kind: OpTxn, Success: []Op{{Kind: OpPut}}}},
		{Kind: CallPut},
		{Kind: CallRange, Op: Op{End: -1, Flags: FlagSerializable}},
	}}
	l := Least{Write: 10 * time.Millisecond, Read: 2 * time.Millisecond}
	if got, want := tr.Chain(0, l, nil), 22*time.Millisecond; got != want {
		t.Errorf("chain from 0 is %v, want %v", got, want)
	}
	// From call 1 on, call 0 is done before the start.
	if got, want := tr.Chain(1, l, nil), 12*time.Millisecond; got != want {
		t.Errorf("chain from 1 is %v, want %v", got, want)
	}
	d := &Draw{Trace: tr, Starts: []int{0, 1}}
	if got, want := d.Bound(l), 5/0.022+4/0.012; math.Abs(got-want) > 1e-6 {
		t.Errorf("bound %v, want %v", got, want)
	}
	if got := d.Bound(Least{}); !math.IsInf(got, 1) {
		t.Errorf("bound with no least times is %v, want +Inf", got)
	}
}

// TestBoundLeavesCacheToCopy0 counts a CAS that may fail into a read as a
// read's time, and leaves the watch cache's list and progress request to
// copy 0: copy 1 neither sends them nor waits for them.
func TestBoundLeavesCacheToCopy0(t *testing.T) {
	cas := Op{
		Kind: OpTxn, Compare: []Compare{{Key: 2}},
		Success: []Op{{Kind: OpPut, Key: 2}}, Failure: []Op{{Kind: OpRange, Key: 2, End: -1}},
	}
	tr := &Trace{
		Keys:    []string{"/registry/pods/", "/registry/pods0", "/registry/pods/ns/p"},
		Watches: []WatchStream{{Creates: []Watch{{Key: 0, End: 1}}}},
		Calls: []Call{
			{Kind: CallRangeStream, Op: Op{Kind: OpRange, Key: 0, End: 1, Limit: 10000}},
			{Kind: CallTxn, Waits: []int32{0}, Op: cas},
			{Kind: CallProgress, Waits: []int32{1}},
			{Kind: CallPut, Waits: []int32{2}, Op: Op{Kind: OpPut, Key: 2}},
		},
	}
	if got, want := tr.WatchCache(), []bool{true, false, true, false}; !slices.Equal(got, want) {
		t.Fatalf("watch cache calls %v, want %v", got, want)
	}
	l := Least{Write: 10 * time.Millisecond, Read: 2 * time.Millisecond}
	// Copy 0: the list, the CAS as a read, the progress request, the put.
	// Copy 1: the CAS and the put, 2 of the 4 calls.
	d := &Draw{Trace: tr, Starts: []int{0, 0}}
	if got, want := d.Bound(l), 4/0.014+2/0.012; math.Abs(got-want) > 1e-6 {
		t.Errorf("bound %v, want %v", got, want)
	}
}
