package check

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

var bothModes = []WitnessMode{FromLog, FromRevisions}

// unusedTxnLease is #290 cut down: a lease expires without keys and a put
// finds it gone, then a txn names it in the branch that does not run, the
// success branch or, with succeeded, the failure branch. Only the branch
// that ran shows a lease in use.
func unusedTxnLease(t *testing.T, succeeded bool) *history.History {
	b := newBuilder(t, "a", "b")
	grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
	expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
	b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "1", Lease: 42}})
	b.put(0, "a", "x")
	read := []history.TxnOp{{Range: &history.RangeRequest{Key: "a"}}}
	unused := []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "2", Lease: 42}}}
	txn := &history.TxnRequest{
		Compare: []history.Compare{{Key: "a", Target: history.TargetVersion, Result: history.Equal, Number: 0}},
		Success: unused, Failure: read,
	}
	if succeeded {
		txn.Compare[0].Number = 1
		txn.Success, txn.Failure = read, unused
	}
	b.do(0, history.Request{Txn: txn})
	h := b.done()
	h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
	h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
	return h
}

// TestWitnessedGood passes linearizable histories in both modes.
func TestWitnessedGood(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T) *history.History
	}{
		{"sequential workload", func(t *testing.T) *history.History { return workload(t).done() }},
		{"read inside a concurrent write", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[put].Call, h.Ops[put].Return = 10, 50
			h.Ops[get].Call, h.Ops[get].Return = 20, 30
			return h
		}},
		{"indeterminate write lands after a later read saw it missing", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			first := b.get(1, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			// The put is called before the first read and times out; it
			// takes effect after that read.
			h.Ops[put].Call, h.Ops[put].Return = h.Ops[first].Call-5, h.Ops[first].Call-1
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			return h
		}},
		{"indeterminate write never lands", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
			h.Log = nil
			h.Ops = append(h.Ops,
				history.Op{Client: 1, Call: 100, Return: 110, Req: history.Request{Put: &history.PutRequest{Key: "b", Value: "2"}}, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
				history.Op{Client: 1, Call: 120, Return: 130, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 2, Range: &history.RangeResponse{}}})
			h.Log = []history.Request{h.Ops[1].Req}
			return h
		}},
		{"rejected request had no effect", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 5}})
			b.get(0, "a")
			b.put(0, "a", "y")
			return b.done()
		}},
		{"lease expiry modelled as an ambiguous revoke", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.get(0, "a")
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Return+1, h.Ops[grant].Return+2
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			return h
		}},
		{"lease expiry without keys, found gone later", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.put(1, "a", "1")
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			return h
		}},
		{"lease expiry that deletes keys, found gone by the last call", func(t *testing.T) *history.History {
			// The expiry writes a revision that no answer shows (#237,
			// seed 28187).
			b := newBuilder(t, "a", "b")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "x", Lease: 42}})
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "y", Lease: 42}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			return h
		}},
		{"lease expiry before a compaction of the same revision", func(t *testing.T) *history.History {
			// A read below the compaction is called after the rejection
			// that found the lease gone, and returns before the compaction
			// does: the expiry goes before the compaction (#237, seed
			// 85441).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.put(0, "a", "2")
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			gone := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			read := b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			compact := b.do(3, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			h.Ops[compact].Call, h.Ops[compact].Return = h.Ops[gone].Call-1, h.Ops[read].Return+1
			return h
		}},
		{"ambiguous compaction after an acked one at the same revision", func(t *testing.T) *history.History {
			// The acked compaction to 1 answered at revision 3, and the
			// ambiguous one to 3 must follow it. A read below that found
			// its revision compacted returned before the next put was
			// called, so the ambiguous one goes in at revision 3 (#258,
			// seed 10609).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 1}})
			compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			b.put(0, "a", "3")
			h := b.done()
			h.Ops[compact].Resp, h.Ops[compact].Err = nil, errUnavailable
			return h
		}},
		{"two ambiguous compactions due at once, in call order", func(t *testing.T) *history.History {
			// The read below both found its revision compacted. Only the
			// compaction to 3 was called before it returned, so it goes in
			// first, though the one to 1 comes first in the history, and
			// the one to 1 then does nothing (TestSeeds seed 36260 on
			// #268).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.put(0, "a", "3")
			early := b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 1}})
			compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			read := b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
			put := b.put(0, "a", "4")
			h := b.done()
			h.Log = slices.DeleteFunc(h.Log, func(req history.Request) bool { return req.Compact != nil && req.Compact.Revision == 1 })
			h.Ops[early].Call, h.Ops[early].Return = h.Ops[read].Return+1, h.Ops[put].Return+10
			h.Ops[compact].Return = h.Ops[put].Return + 10
			for _, i := range []int{early, compact} {
				h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
			}
			return h
		}},
		{"ambiguous compaction between two reads of one revision", func(t *testing.T) *history.History {
			// A read at revision 2 succeeded at revision 3, a later one
			// found 2 compacted, and a read after that still answered at
			// revision 3: the ambiguous compaction goes in at revision 3,
			// between the two reads (#277, seed 8143538).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			b.do(4, history.Request{Range: &history.RangeRequest{Key: "a"}})
			b.put(0, "a", "3")
			h := b.done()
			h.Ops[compact].Resp, h.Ops[compact].Err = nil, errUnavailable
			return h
		}},
		{"ambiguous compaction after the grant a read below it follows", func(t *testing.T) *history.History {
			// The compaction to 3 was called before the grant, and the read
			// at revision 2 after the grant returned. A later rejection
			// makes the compaction due within revision 3, after the grant,
			// so that the read can still come before it (#277, seed
			// 8200093).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			grant := b.do(1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 100}})
			b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			compact := b.do(3, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			b.do(4, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			b.do(5, history.Request{Range: &history.RangeRequest{Key: "a"}})
			b.put(0, "a", "3")
			h := b.done()
			h.Ops[compact].Call = h.Ops[grant].Call - 1
			h.Ops[compact].Resp, h.Ops[compact].Err = nil, errUnavailable
			return h
		}},
		{"ambiguous compaction after a smaller one that a read below it needs", func(t *testing.T) *history.History {
			// A compaction to 2 was rejected as compacted before a read at
			// 2 succeeded, so the ambiguous compaction to 2 comes before
			// both, and the one to 3, which a later read found, after the
			// read (#277, seed 8238917).
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			small := b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			rejected := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			large := b.do(4, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			b.do(5, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			b.do(6, history.Request{Range: &history.RangeRequest{Key: "a"}})
			put := b.put(0, "a", "3")
			h := b.done()
			for _, i := range []int{small, large} {
				h.Ops[i].Call, h.Ops[i].Return = h.Ops[rejected].Call-1, h.Ops[put].Return+10
				h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
			}
			return h
		}},
		{"lease expiry before an ambiguous compaction called after its rejection", func(t *testing.T) *history.History {
			// An early rejection makes the compaction due from revision
			// 3, where it goes in before a put that was called before the
			// put that found the lease gone returned. The compaction was
			// called after that, so the expiry goes in before it
			// (TestSlowFetch seed 5022 on #268).
			b := newBuilder(t, "a", "b")
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 1}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "1", Lease: 42}})
			b.put(0, "a", "2")
			b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			expiry := b.do(5, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			gone := b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "x", Lease: 42}})
			compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			put := b.put(3, "b", "y")
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			h.Ops[expiry].Resp, h.Ops[expiry].Err = nil, errUnavailable
			h.Ops[gone].Call, h.Ops[gone].Return = 1000, 1010
			h.Ops[put].Call, h.Ops[put].Return = 1005, 1030
			h.Ops[compact].Call, h.Ops[compact].Return = 1011, 1040
			h.Ops[compact].Resp, h.Ops[compact].Err = nil, errUnavailable
			return h
		}},
		{"ambiguous revoke of a lease ID granted again", func(t *testing.T) *history.History {
			// The second grant's put shows the ID in use after the
			// revoke's revision, which says nothing about the first.
			b := newBuilder(t, "a", "b")
			b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 60}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "1", Lease: 42}})
			revoke := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 60}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Value: "2", Lease: 42}})
			h := b.done()
			h.Ops[revoke].Resp, h.Ops[revoke].Err = nil, errUnavailable
			return h
		}},
		{"historical reads, inside a txn too", func(*testing.T) *history.History { return historicalTxn() }},
		{"acked compaction after the grant a read below it follows", func(t *testing.T) *history.History { return grantReadCompact(t, false) }},
		{"lease expired before a txn names it in the success branch that did not run", func(t *testing.T) *history.History { return unusedTxnLease(t, false) }},
		{"lease expired before a txn names it in the failure branch that did not run", func(t *testing.T) *history.History { return unusedTxnLease(t, true) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range bothModes {
				mustPass(t, "Witnessed "+mode.String(), Witnessed(c.build(t), mode, timeout))
			}
		})
	}
}

// historicalTxn puts a twice, then runs a txn that reads a at revision 2,
// puts it again and reads it at revision 3.
func historicalTxn() *history.History {
	put := func(v string) history.Request { return history.Request{Put: &history.PutRequest{Key: "a", Value: v}} }
	txn := history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Range: &history.RangeRequest{Key: "a", Revision: 2}},
		{Put: &history.PutRequest{Key: "a", Value: "3"}},
		{Range: &history.RangeRequest{Key: "a", Revision: 3}},
	}}}
	at := func(v string, mod, version int64) *history.RangeResponse {
		return &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: v, CreateRevision: 2, ModRevision: mod, Version: version}}}
	}
	return &history.History{
		Ops: []history.Op{
			{Client: 0, Call: 1, Return: 2, Req: put("1"), Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
			{Client: 0, Call: 3, Return: 4, Req: put("2"), Resp: &history.Response{Revision: 3, Put: &history.PutResponse{}}},
			{Client: 1, Call: 5, Return: 6, Req: txn, Resp: &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{
				{Range: at("1", 2, 1)}, {Put: &history.PutResponse{}}, {Range: at("2", 3, 2)},
			}}}},
		},
		Log: []history.Request{put("1"), put("2"), txn},
	}
}

// TestWitnessedBroken fails histories that are not linearizable, in both
// modes: #112's stale read, lost acked write and write applied twice, and
// Linearizable's broken cases that need no Effect.
func TestWitnessedBroken(t *testing.T) {
	cases := []struct {
		name  string
		want  string
		build func(t *testing.T) *history.History
	}{
		{"stale read", "answers it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0] = history.KeyValue{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 2, Version: 1}
			return h
		}},
		{"lost acked write", "answers it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp = &history.Response{Revision: 2, Range: &history.RangeResponse{}}
			return h
		}},
		{"write applied twice", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "1")
			b.put(1, "b", "2")
			h := b.done()
			// A replay of the first put after the second write.
			h.Log = append(h.Log, h.Log[0])
			h.Ops = append(h.Ops, history.Op{
				Client: 2, Call: 100, Return: 110, Req: history.Request{Range: &history.RangeRequest{Key: "a"}},
				Resp: &history.Response{Revision: 4, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "1", CreateRevision: 2, ModRevision: 4, Version: 2}}}},
			})
			return h
		}},
		{"two CAS on one revision both win", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.cas(1, "a", 2, "x")
			second := b.cas(2, "a", 2, "y")
			h := b.done()
			h.Ops[second].Resp = &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Put: &history.PutResponse{}}}}}
			return h
		}},
		{"put reports a wrong revision", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp.Revision = 3
			return h
		}},
		{"rejected write took effect", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			b.get(1, "a")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, &history.Error{Code: "InvalidArgument", Message: "etcdserver: request is too large"}
			return h
		}},
		{"read at the compaction revision fails as compacted", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			read := b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp, h.Ops[read].Err = nil, errCompacted
			return h
		}},
		{"read of a value nobody wrote", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0].Value = "2"
			return h
		}},
		{"read sees a write that starts after it returned", "", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			put := b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Call, h.Ops[get].Return = 1, 2
			h.Ops[put].Call, h.Ops[put].Return = 5, 6
			return h
		}},
		{"historical read inside a txn sees a later value", "", func(*testing.T) *history.History {
			h := historicalTxn()
			h.Ops[2].Resp.Txn.Results[0].Range = h.Ops[2].Resp.Txn.Results[2].Range
			return h
		}},
		{"read below a compaction that returned before it", "", func(t *testing.T) *history.History { return grantReadCompact(t, true) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range bothModes {
				mustFail(t, "Witnessed "+mode.String(), Witnessed(c.build(t), mode, timeout), c.want)
			}
		})
	}
}

// TestWitnessedUnseenGaps has ten writes that no answer shows, each of
// a revision no acked write holds, and eight older ambiguous deletes that
// never took effect, which fit every one of those revisions. Only the
// count of a delete after them shows that none of the eight came in
// between. The search tried their orders until it timed out (#251, seed
// 62147) and must now finish at once.
func TestWitnessedUnseenGaps(t *testing.T) {
	keys := []string{"z"}
	for i := range 8 {
		keys = append(keys, fmt.Sprintf("k%d", i))
	}
	b := newBuilder(t, keys...)
	for i := range 8 {
		b.put(0, fmt.Sprintf("k%d", i), "v")
	}
	var unseen []int
	for i := range 10 {
		unseen = append(unseen, b.put(10+i, "z", fmt.Sprintf("z%d", i)))
	}
	b.do(0, history.Request{Delete: &history.DeleteRequest{Key: "k0", End: "k8"}})
	h := b.done()
	for _, i := range unseen {
		h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
	}
	for i := range 8 {
		h.Ops = append(h.Ops, history.Op{
			Client: 30 + i, Call: 1, Return: 2,
			Req: history.Request{Delete: &history.DeleteRequest{Key: fmt.Sprintf("k%d", i)}},
			Err: errUnavailable,
		})
	}
	mustPass(t, "Witnessed revisions", Witnessed(h, FromRevisions, 10*time.Second))
}

// TestWitnessedTimeout checks that running out of time is a failure that
// says how far the check got, also for a history too small to check the
// clock on the way.
func TestWitnessedTimeout(t *testing.T) {
	b := newBuilder(t, "a")
	for i := range 4000 {
		b.put(i%16, "a", strconv.Itoa(i))
	}
	small := newBuilder(t, "a")
	small.put(0, "a", "1")
	for _, h := range []*history.History{b.done(), small.done()} {
		for _, mode := range bothModes {
			mustFail(t, "Witnessed "+mode.String(), Witnessed(h, mode, time.Nanosecond), "timed out; the order held")
		}
	}
}

// TestWitnessedMaxSteps checks that running out of steps is a failure
// too, which the clock does not decide.
func TestWitnessedMaxSteps(t *testing.T) {
	b := newBuilder(t, "a")
	for i := range 4000 {
		b.put(i%16, "a", strconv.Itoa(i))
	}
	h := b.done()
	for _, mode := range bothModes {
		mustFail(t, "Witnessed "+mode.String(), Witnessed(h, mode, time.Hour, MaxSteps(100)), "timed out after 100 steps; the order held")
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, time.Hour, MaxSteps(1_000_000)))
	}
}

// randomHistory runs random puts, deletes, compare-and-swaps and reads on
// three keys, one at a time in order, from four clients whose calls
// overlap their neighbours'. A quarter of the writes time out; half of
// those took effect, and the log has them.
func randomHistory(rng *rand.Rand) *history.History {
	keys := []string{"a", "b", "c"}
	st := newState(keys)
	h := &history.History{}
	for i := range 40 {
		at := int64(i+1) * 10
		key := keys[rng.IntN(len(keys))]
		var req history.Request
		switch rng.IntN(5) {
		case 0:
			req.Put = &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i), PrevKV: true}
		case 1:
			req.Delete = &history.DeleteRequest{Key: key, PrevKV: true}
		case 2:
			kv, _ := st.get(key)
			req.Txn = &history.TxnRequest{
				Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: kv.ModRevision - rng.Int64N(2)}},
				Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i)}}},
				Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}},
			}
		case 3:
			req.Range = &history.RangeRequest{Key: "a", End: history.AllKeys, Limit: rng.Int64N(3)}
		default:
			req.Range = &history.RangeRequest{Key: key}
		}
		op := history.Op{Client: i % 4, Call: at - rng.Int64N(16), Return: at + rng.Int64N(16), Req: req}
		if req.Mutates() && rng.IntN(4) == 0 {
			op.Err = &history.Error{Code: "Unavailable", Message: "timed out", Ambiguous: true}
			if rng.IntN(2) == 0 {
				if o := st.step(req, false); o.err == nil && changes(req, o) {
					h.Log = append(h.Log, req)
				}
			}
		} else {
			o := st.step(req, false)
			op.Resp, op.Err = o.resp, o.err
			if o.err == nil && changes(req, o) {
				h.Log = append(h.Log, req)
			}
		}
		h.Ops = append(h.Ops, op)
	}
	return h
}

// TestWitnessedAgreesWithPorcupine checks Witnessed against Linearizable
// on random small histories, true ones and copies with a stale read: both
// modes must give Porcupine's verdict on every one.
func TestWitnessedAgreesWithPorcupine(t *testing.T) {
	rng := rand.New(rand.NewPCG(112, 148))
	rejected := 0
	for i := range 300 {
		h := randomHistory(rng)
		if i%2 == 1 && !staleAnswer(rng, h) {
			continue
		}
		lin := Linearizable(h, timeout)
		if i%2 == 0 && lin != nil {
			t.Fatalf("history %d: Porcupine rejects a true history: %v", i, lin)
		}
		if lin != nil {
			rejected++
		}
		for _, mode := range bothModes {
			if err := Witnessed(h, mode, timeout); (err == nil) != (lin == nil) {
				t.Fatalf("history %d: Porcupine says %v, Witnessed (%s) says %v", i, lin, mode, err)
			}
		}
	}
	if rejected < 50 {
		t.Fatalf("only %d histories were rejected", rejected)
	}
	t.Logf("%d histories rejected by all three", rejected)
}

// staleAnswer makes one successful read of a single key in h answer with
// an older value of it, from an acked put before it, and reports whether
// there was one.
func staleAnswer(rng *rand.Rand, h *history.History) bool {
	start := rng.IntN(len(h.Ops))
	for j := range h.Ops {
		i := (start + j) % len(h.Ops)
		op := &h.Ops[i]
		if op.Req.Range == nil || op.Req.Range.End != "" || op.Resp == nil || len(op.Resp.Range.KVs) == 0 {
			continue
		}
		got := op.Resp.Range.KVs[0]
		for k := i - 1; k >= 0; k-- {
			p := h.Ops[k]
			if p.Req.Put == nil || p.Req.Put.Key != got.Key || p.Resp == nil || p.Req.Put.Value == got.Value {
				continue
			}
			resp := *op.Resp
			rr := *resp.Range
			rr.KVs = []history.KeyValue{{Key: got.Key, Value: p.Req.Put.Value, CreateRevision: got.CreateRevision, ModRevision: p.Resp.Revision, Version: got.Version}}
			resp.Range = &rr
			op.Resp = &resp
			return true
		}
	}
	return false
}

// TestWitnessedTxnRevisions replays the difftest case txn-revisions with
// etcd v3.7.2's recorded answers (difftest/testdata/etcd-v3.7.2.json): a
// read at a past revision inside a txn, reads at the txn's start revision
// and the current one after its own write, and failure branches that
// write. The model must give each answer, in both modes, before a failure
// of the witness checker can be trusted (#112). The recording's revisions
// start one lower: its first put is revision 1 above the case's start.
// The answers are copied from the recording by hand, so a new recording
// of the case needs them copied again.
func TestWitnessedTxnRevisions(t *testing.T) {
	kv := func(key, value string, create, mod, version int64) history.KeyValue {
		return history.KeyValue{Key: key, Value: value, CreateRevision: create + 1, ModRevision: mod + 1, Version: version}
	}
	one := func(k history.KeyValue) *history.RangeResponse {
		return &history.RangeResponse{Count: 1, KVs: []history.KeyValue{k}}
	}
	at := func(key string, rev int64) history.TxnOp {
		return history.TxnOp{Range: &history.RangeRequest{Key: key, Revision: rev + 1}}
	}
	get := func(key string) history.TxnOp { return history.TxnOp{Range: &history.RangeRequest{Key: key}} }
	put := func(key, value string) history.TxnOp {
		return history.TxnOp{Put: &history.PutRequest{Key: key, Value: value}}
	}
	del := func(key string) history.TxnOp { return history.TxnOp{Delete: &history.DeleteRequest{Key: key}} }
	mod := func(key string, rev int64) []history.Compare {
		return []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: rev}}
	}
	txn := func(cmp []history.Compare, success, failure []history.TxnOp) history.Request {
		return history.Request{Txn: &history.TxnRequest{Compare: cmp, Success: success, Failure: failure}}
	}
	results := func(rev int64, ok bool, rs ...history.TxnOpResponse) *history.Response {
		return &history.Response{Revision: rev + 1, Txn: &history.TxnResponse{Succeeded: ok, Results: rs}}
	}
	a1, a3, a4 := kv("t/a", "1", 1, 1, 1), kv("t/a", "3", 1, 4, 3), kv("t/a", "4", 1, 6, 4)
	b1, b2 := kv("t/b", "1", 3, 3, 1), kv("t/b", "2", 3, 5, 2)
	steps := []struct {
		req  history.Request
		resp *history.Response
	}{
		{history.Request{Put: &history.PutRequest{Key: "t/a", Value: "1"}}, &history.Response{Revision: 2, Put: &history.PutResponse{}}},
		{history.Request{Put: &history.PutRequest{Key: "t/a", Value: "2"}}, &history.Response{Revision: 3, Put: &history.PutResponse{}}},
		{history.Request{Put: &history.PutRequest{Key: "t/b", Value: "1"}}, &history.Response{Revision: 4, Put: &history.PutResponse{}}},
		{txn(nil, []history.TxnOp{at("t/a", 1)}, nil), results(3, true, history.TxnOpResponse{Range: one(a1)})},
		{
			txn(nil, []history.TxnOp{at("t/a", 1), put("t/a", "3"), at("t/a", 1), get("t/a")}, nil),
			results(4, true, history.TxnOpResponse{Range: one(a1)}, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(a1)}, history.TxnOpResponse{Range: one(a3)}),
		},
		{
			txn(nil, []history.TxnOp{put("t/b", "2"), at("t/b", 4), get("t/b")}, nil),
			results(5, true, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(b1)}, history.TxnOpResponse{Range: one(b2)}),
		},
		{
			txn(mod("t/a", 2), []history.TxnOp{get("t/a")}, []history.TxnOp{put("t/a", "4"), get("t/a")}),
			results(6, false, history.TxnOpResponse{Put: &history.PutResponse{}}, history.TxnOpResponse{Range: one(a4)}),
		},
		{
			txn(mod("t/b", 0), []history.TxnOp{get("t/b")}, []history.TxnOp{del("t/b"), get("t/b")}),
			results(7, false, history.TxnOpResponse{Delete: &history.DeleteResponse{Deleted: 1}}, history.TxnOpResponse{Range: &history.RangeResponse{}}),
		},
		{
			txn(mod("t/b", 6), []history.TxnOp{get("t/b")}, []history.TxnOp{del("t/none")}),
			results(7, false, history.TxnOpResponse{Delete: &history.DeleteResponse{}}),
		},
		{history.Request{Range: &history.RangeRequest{Key: "t/a", Revision: 5}}, &history.Response{Revision: 8, Range: one(a3)}},
		{history.Request{Range: &history.RangeRequest{Key: "t/b", Revision: 7}}, &history.Response{Revision: 8, Range: one(b2)}},
	}
	h := &history.History{}
	for i, s := range steps {
		h.Ops = append(h.Ops, history.Op{Client: 0, Call: int64(2*i + 1), Return: int64(2*i + 2), Req: s.req, Resp: s.resp})
		if s.resp.Revision > int64(len(h.Log)+1) {
			h.Log = append(h.Log, s.req)
		}
	}
	for _, mode := range bothModes {
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout))
	}
}

// TestWitnessedAmbiguousCases are three true histories that an earlier
// log-free search failed (review of #197): an ambiguous grant that a put
// in the next revision needs; a keys-only read, which shows no value, of
// a key that one of two ambiguous puts wrote; and a gap that the later of
// two ambiguous candidates fits but only the earlier explains, as a
// rejection in between needs it. Porcupine passes each, and so must both
// modes.
func TestWitnessedAmbiguousCases(t *testing.T) {
	amb := func(client int, call int64, req history.Request) history.Op {
		return history.Op{Client: client, Call: call, Return: call + 1, Req: req, Err: errUnavailable}
	}
	acked := func(client int, call int64, req history.Request, resp *history.Response) history.Op {
		return history.Op{Client: client, Call: call, Return: call + 1, Req: req, Resp: resp}
	}
	put := func(key, value string, lease int64) history.Request {
		return history.Request{Put: &history.PutRequest{Key: key, Value: value, Lease: lease}}
	}
	grant := history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 60}}
	revoke := history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}}
	cases := map[string]*history.History{
		"grant the next put needs": {
			Ops: []history.Op{
				amb(0, 1, grant),
				acked(1, 3, put("a", "x", 42), &history.Response{Revision: 2, Put: &history.PutResponse{}}),
			},
			Log: []history.Request{grant, put("a", "x", 42)},
		},
		"keys-only read of an ambiguous put": {
			Ops: []history.Op{
				amb(0, 1, put("a", "x", 0)),
				amb(1, 1, put("a", "", 0)),
				acked(2, 5, history.Request{Range: &history.RangeRequest{Key: "a", KeysOnly: true}},
					&history.Response{Revision: 2, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", CreateRevision: 2, ModRevision: 2, Version: 1}}}}),
				acked(3, 7, history.Request{Txn: &history.TxnRequest{
					Compare: []history.Compare{{Key: "a", Target: history.TargetValue, Result: history.Equal, Value: "x"}},
				}}, &history.Response{Revision: 2, Txn: &history.TxnResponse{Succeeded: true}}),
			},
			Log: []history.Request{put("a", "x", 0)},
		},
		"the earlier candidate of a gap": {
			Ops: []history.Op{
				acked(0, 1, grant, &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 42, TTL: 60}}),
				acked(0, 3, put("k", "v", 42), &history.Response{Revision: 2, Put: &history.PutResponse{}}),
				amb(1, 5, revoke),
				{Client: 2, Call: 7, Return: 8, Req: put("k2", "w", 42), Err: errLeaseNotFound},
				amb(3, 9, history.Request{Delete: &history.DeleteRequest{Key: "k"}}),
				acked(4, 11, put("z", "1", 0), &history.Response{Revision: 4, Put: &history.PutResponse{}}),
			},
			Log: []history.Request{grant, put("k", "v", 42), revoke, put("z", "1", 0)},
		},
	}
	// An expiry that only an ambiguous put's later life shows came after
	// that put: it must not go in right after the grant (seed 1797098110).
	cases["expiry after an ambiguous put that used the lease"] = &history.History{
		Ops: []history.Op{
			acked(0, 1, grant, &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 42, TTL: 60}}),
			{Client: 9, Call: 1, Return: 1, Req: revoke, Err: errUnavailable},
			amb(1, 3, put("a", "x", 42)),
			acked(2, 5, put("a", "y", 0), &history.Response{Revision: 3, Put: &history.PutResponse{}}),
			acked(2, 7, history.Request{Range: &history.RangeRequest{Key: "a"}},
				&history.Response{Revision: 3, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "a", Value: "y", CreateRevision: 2, ModRevision: 3, Version: 2}}}}),
			{Client: 3, Call: 9, Return: 10, Req: put("c", "z", 42), Err: errLeaseNotFound},
		},
		Log: []history.Request{grant, put("a", "x", 42), put("a", "y", 0), revoke},
	}
	// The same with the rejection called before the put: it may still
	// take effect after it, up to its return (review of #197).
	overlap := *cases["expiry after an ambiguous put that used the lease"]
	overlap.Ops = slices.Clone(overlap.Ops)
	overlap.Ops[5].Call = 3
	overlap.Ops[5].Client = 4
	cases["expiry after an ambiguous put, with the rejection overlapping it"] = &overlap
	// The same with an ambiguous put called after the rejection returned,
	// and an acked one after that: the expiry must go in before the
	// ambiguous one, which no acked op's revision shows (review of #197).
	later := overlap
	later.Ops = append(slices.Clone(overlap.Ops),
		amb(5, 11, put("d", "1", 0)),
		acked(6, 13, put("e", "2", 0), &history.Response{Revision: 5, Put: &history.PutResponse{}}))
	later.Log = append(slices.Clone(overlap.Log), put("d", "1", 0), put("e", "2", 0))
	cases["expiry before an ambiguous put called after the rejection returned"] = &later
	// Only ambiguous puts follow the rejection, and a read shows where they
	// went: the expiry goes in before the first (CI planted B27, seed 83).
	cases["expiry before ambiguous puts that a later read places"] = &history.History{
		Ops: []history.Op{
			acked(0, 1, grant, &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 42, TTL: 60}}),
			{Client: 9, Call: 1, Return: 1, Req: revoke, Err: errUnavailable},
			{Client: 3, Call: 3, Return: 4, Req: put("c", "z", 42), Err: errLeaseNotFound},
			amb(4, 5, put("a", "x", 0)),
			amb(5, 5, put("b", "y", 0)),
			acked(6, 8, history.Request{Range: &history.RangeRequest{Key: "a", End: "c"}},
				&history.Response{Revision: 3, Range: &history.RangeResponse{Count: 2, KVs: []history.KeyValue{
					{Key: "a", Value: "x", CreateRevision: 2, ModRevision: 2, Version: 1},
					{Key: "b", Value: "y", CreateRevision: 3, ModRevision: 3, Version: 1},
				}}}),
		},
		Log: []history.Request{grant, revoke, put("a", "x", 0), put("b", "y", 0)},
	}
	// A read called after the rejection returned sees the revision before
	// a concurrent put's: the expiry goes in by the end of the read's
	// revision, though no op called after the rejection changes anything
	// (CI planted B27, seed 2).
	cases["expiry by the revision of a read called after the rejection returned"] = &history.History{
		Ops: []history.Op{
			acked(0, 0, grant, &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: 42, TTL: 60}}),
			{Client: 9, Call: 1, Return: 1, Req: revoke, Err: errUnavailable},
			{Client: 3, Call: 2, Return: 4, Req: put("c", "z", 42), Err: errLeaseNotFound},
			{Client: 4, Call: 1, Return: 10, Req: put("a", "x", 0), Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
			acked(5, 5, history.Request{Range: &history.RangeRequest{Key: "a"}}, &history.Response{Revision: 1, Range: &history.RangeResponse{}}),
		},
		Log: []history.Request{grant, revoke, put("a", "x", 0)},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			mustPass(t, "Linearizable", Linearizable(h, timeout))
			for _, mode := range bothModes {
				mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout))
			}
		})
	}
}

// grantReadCompact is seed 30120 of #284 cut down: a compaction to 3 is
// called before a grant, and a read at revision 2 after the grant
// returned; all three answer at revision 3. The grant goes before the
// compaction, so that the read can come between them. With early, the
// compaction returns before the read is called, and the read is stale.
func grantReadCompact(t *testing.T, early bool) *history.History {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	grant := b.do(1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 100}})
	read := b.do(2, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
	compact := b.do(3, history.Request{Compact: &history.CompactRequest{Revision: 3}})
	h := b.done()
	h.Ops[compact].Call = h.Ops[grant].Call - 1
	h.Ops[compact].Return = h.Ops[read].Return + 1
	if early {
		h.Ops[compact].Return = h.Ops[read].Call - 1
	}
	return h
}

// TestWitnessedSupersededCompaction has an ambiguous compaction to 2 that
// an acked one to 3 made pointless, a read below it that found 1
// compacted, and then thousands of ops. The ambiguous compaction stays
// due to the end, and the witness placed its reads again at every event
// before it asked the model, which refuses it at once: quadratic, so that
// seed 7004's 12,130 ops took over a minute under -race (#289).
func TestWitnessedSupersededCompaction(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	read := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
	early := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 2}})
	b.put(0, "a", "3")
	b.do(3, history.Request{Compact: &history.CompactRequest{Revision: 3}})
	b.do(4, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
	last := 0
	for i := range 4000 {
		last = b.put(0, "a", strconv.Itoa(i))
		b.get(1, "a")
	}
	h := b.done()
	h.Ops[early].Call, h.Ops[early].Return = h.Ops[read].Return+1, h.Ops[last].Return+10
	h.Ops[early].Resp, h.Ops[early].Err = nil, errUnavailable
	for _, mode := range bothModes {
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, 2*time.Second))
	}
}

// TestLateCompactionKeepsLeases has a compaction, ambiguous and called
// late, that could explain an old rejection of a compacted read but cannot
// go in before its call. Trying it at an earlier revision must not expire
// the lease, which made an ambiguous put of it fail (#376: TestSeeds seeds
// 401579 and 477576). A checker bug: histories built by hand show it.
func TestLateCompactionKeepsLeases(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "overwritten"
		if deleting {
			name = "deleted"
		}
		t.Run(name, func(t *testing.T) {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "initial")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
			grant := b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 60}})
			put := b.do(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "leased", Lease: 42}})
			if deleting {
				b.del(0, "a")
			} else {
				b.put(0, "a", "unleased")
				b.get(0, "a")
			}
			expiry := b.do(1, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 42}})
			b.do(0, history.Request{Put: &history.PutRequest{Key: "b", Lease: 42}})
			b.put(0, "b", "later")
			compact := b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 3}})
			h := b.done()
			h.Ops[expiry].Call, h.Ops[expiry].Return = h.Ops[grant].Call, h.Ops[grant].Call
			for _, i := range []int{put, expiry, compact} {
				h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
			}
			mustPass(t, "Linearizable", Linearizable(h, time.Second))
			for _, mode := range bothModes {
				mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, time.Second))
			}
		})
	}
}
