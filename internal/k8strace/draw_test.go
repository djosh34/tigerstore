package k8strace

import (
	"math"
	"testing"
	"time"
)

// TestChain follows the longest path through the waits: a write, a read
// that waits on it, and a write that waits on the read take the sum of
// their least times; a call that waits on nothing runs alongside.
func TestChain(t *testing.T) {
	tr := &Trace{Calls: []Call{
		{Kind: CallPut},
		{Kind: CallRange, Waits: []int32{0}},
		{Kind: CallTxn, Waits: []int32{1}, Op: Op{Kind: OpTxn, Success: []Op{{Kind: OpPut}}}},
		{Kind: CallPut},
		{Kind: CallRange, Op: Op{Flags: FlagSerializable}},
	}}
	l := Least{Write: 10 * time.Millisecond, Read: 2 * time.Millisecond}
	if got, want := tr.Chain(0, l), 22*time.Millisecond; got != want {
		t.Errorf("chain from 0 is %v, want %v", got, want)
	}
	// From call 1 on, call 0 is done before the start.
	if got, want := tr.Chain(1, l), 12*time.Millisecond; got != want {
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
