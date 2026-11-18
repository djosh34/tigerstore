package sim

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// The checkers agree on a linearizable history with many ambiguous
// writes that never took effect, as #729's seeds 286 and 423404944 had
// while their clusters formed: a put called early whose answer came late,
// a read between that saw the empty store, and 40 deletes cut off by lost
// connections. Porcupine places the put at its call, finds the read
// cannot follow it, and must take back every delete placed since, each
// of which it may place anywhere after or never; that ran past its
// 1,000,000 steps. The log shows the deletes never took effect, and
// without them it decides in milliseconds.
func TestTheCheckersAgreeOnLostAmbiguousWrites(t *testing.T) {
	h := &history.History{Log: []history.Request{{Put: &history.PutRequest{Key: "a", Value: "1"}}}}
	h.Ops = append(h.Ops,
		history.Op{Client: 1, Call: 10, Return: 100_000, Req: history.Request{Put: &history.PutRequest{Key: "a", Value: "1"}}, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
		history.Op{Client: 2, Call: 50_000, Return: 50_100, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 1, Range: &history.RangeResponse{}}},
	)
	for i := range int64(40) {
		h.Ops = append(h.Ops, history.Op{
			Client: 3, Call: 20 + i*1000, Return: 20 + i*1000 + 500,
			Req: history.Request{Delete: &history.DeleteRequest{Key: "b"}},
			Err: &history.Error{Code: "Unavailable", Message: "sim: connection reset", Ambiguous: true},
		})
	}
	if err := agree(h); err != nil {
		t.Fatal(err)
	}
}
