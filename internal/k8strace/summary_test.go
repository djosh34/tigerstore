package k8strace

import (
	"strings"
	"testing"
)

// A trace's summary counts its calls by kind, the calls that write (a
// Txn only when a branch writes), the failed Txns, the calls that wait on
// an earlier reply and the longest such chain, the preload's keys and
// bytes, its leases and watches; Markdown prints it with what the
// conversion dropped, in the reasons' order.
func TestASummaryCountsWhatATraceHolds(t *testing.T) {
	tr := &Trace{
		Shape: "test", About: "a hand-made trace", Seconds: 2,
		Keys:    []string{"/a", "/bb"},
		Leases:  []int64{60},
		Preload: []Item{{Key: 0, Size: 10}, {Key: 1, Size: 20}},
		Watches: []WatchStream{{Creates: []Watch{{Key: 0}, {Key: 1}}}, {Creates: []Watch{{Key: 0}}}},
		Calls: []Call{
			{Kind: CallRange, Op: Op{Kind: OpRange, Key: 0, End: -1}},
			{Kind: CallPut, Waits: []int32{0}, Op: Op{Kind: OpPut, Key: 0}},
			// A Txn that reads in both branches does not write.
			{Kind: CallTxn, Waits: []int32{1}, Op: Op{Kind: OpTxn, Success: []Op{{Kind: OpRange}}, Failure: []Op{{Kind: OpRange}}}},
			// One that writes in its failure branch does, and failed.
			{Kind: CallTxn, Waits: []int32{2}, Failed: true, Op: Op{Kind: OpTxn, Success: []Op{{Kind: OpRange}}, Failure: []Op{{Kind: OpDelete}}}},
			// One that writes in a nested Txn does.
			{Kind: CallTxn, Op: Op{Kind: OpTxn, Success: []Op{{Kind: OpTxn, Success: []Op{{Kind: OpPut}}}}}},
			{Kind: CallDelete, Op: Op{Kind: OpDelete, Key: 1, End: -1}},
		},
	}
	s := Summarize(tr)
	want := Summary{Calls: 6, PerSecond: 3, Writes: 4, Failed: 1, Waiting: 3, Chain: 4, PreloadKeys: 2, PreloadBytes: 2 + 10 + 3 + 20, Leases: 1, Watches: 3, WatchStreams: 2}
	want.ByKind[CallRange], want.ByKind[CallPut], want.ByKind[CallTxn], want.ByKind[CallDelete] = 1, 1, 3, 1
	if s != want {
		t.Fatalf("the summary is\n%+v\nwant\n%+v", s, want)
	}
	md := Markdown(tr, Dropped{"b reason": 2, "a reason": 1})
	for _, line := range []string{
		"## Trace test", "a hand-made trace",
		"- 6 calls in 2 s: 3 calls/s",
		"- 4 write calls; 1 Txns whose compares failed",
		"- 3 calls wait on an earlier reply; the longest chain is 4 calls",
		"- 3 watches on 2 streams",
		"| txn | 3 | 50.0% |",
		"| a reason | 1 |\n| b reason | 2 |",
	} {
		if !strings.Contains(md, line) {
			t.Errorf("the summary lacks %q:\n%s", line, md)
		}
	}
	if strings.Contains(md, "| compact |") {
		t.Errorf("the summary lists a kind with no calls:\n%s", md)
	}
}
