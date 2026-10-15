//go:build difftest

package difftest

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// TestSweep goes through the request forms and errors of KV, Watch and
// Lease that TestBoundaries leaves out, on a fresh server.
func TestSweep(t *testing.T) {
	runCases(t, "etcd-v3.7.2-sweep.json", []testCase{
		{"range-forms", rangeForms},
		{"write-forms", writeForms},
		{"txn-forms", txnForms},
		{"watch-forms", watchForms},
		{"lease-forms", leaseForms},
		{"unknown-methods", unknownMethods},
	})
}

// rangeForms reads five keys with every sort, filter and option.
func rangeForms(r *run) {
	// Key e is written first and c last, so the orders by key, by
	// revision and by value all differ; b has three versions.
	for _, p := range [][2]string{{"e", "1"}, {"b", "5"}, {"d", "2"}, {"a", "4"}, {"b", "3"}, {"b", "6"}, {"c", "0"}} {
		r.put("put "+p[0]+"="+p[1], &pb.PutRequest{Key: r.key(p[0]), Value: []byte(p[1])})
	}
	all := func(req *pb.RangeRequest) *pb.RangeRequest {
		req.Key, req.RangeEnd = r.key(""), prefixEnd(r.key(""))
		return req
	}
	targets := []pb.RangeRequest_SortTarget{pb.RangeRequest_KEY, pb.RangeRequest_VERSION, pb.RangeRequest_CREATE, pb.RangeRequest_MOD, pb.RangeRequest_VALUE}
	for _, target := range targets {
		for _, order := range []pb.RangeRequest_SortOrder{pb.RangeRequest_NONE, pb.RangeRequest_ASCEND, pb.RangeRequest_DESCEND} {
			r.get("sort by "+target.String()+" "+order.String(), all(&pb.RangeRequest{SortTarget: target, SortOrder: order}))
			r.get("sort by "+target.String()+" "+order.String()+", limit 2", all(&pb.RangeRequest{SortTarget: target, SortOrder: order, Limit: 2}))
		}
	}
	r.get("sort order 3", all(&pb.RangeRequest{SortOrder: 3}))
	r.get("sort target 5 ascending", all(&pb.RangeRequest{SortTarget: 5, SortOrder: pb.RangeRequest_ASCEND}))
	r.get("sort target 5 descending", all(&pb.RangeRequest{SortTarget: 5, SortOrder: pb.RangeRequest_DESCEND}))
	r.get("min mod revision 4", all(&pb.RangeRequest{MinModRevision: r.abs(4)}))
	r.get("max mod revision 4", all(&pb.RangeRequest{MaxModRevision: r.abs(4)}))
	r.get("min create revision 3", all(&pb.RangeRequest{MinCreateRevision: r.abs(3)}))
	r.get("max create revision 3", all(&pb.RangeRequest{MaxCreateRevision: r.abs(3)}))
	r.get("mod revisions 3 to 5", all(&pb.RangeRequest{MinModRevision: r.abs(3), MaxModRevision: r.abs(5)}))
	r.get("min mod revision 4, limit 1", all(&pb.RangeRequest{MinModRevision: r.abs(4), Limit: 1}))
	r.get("min mod revision 4, count only", all(&pb.RangeRequest{MinModRevision: r.abs(4), CountOnly: true}))
	r.get("min mod revision 4, keys only", all(&pb.RangeRequest{MinModRevision: r.abs(4), KeysOnly: true}))
	r.get("min mod revision 4, sorted by value", all(&pb.RangeRequest{MinModRevision: r.abs(4), SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND}))
	r.get("min mod revision 4 at revision 5", all(&pb.RangeRequest{MinModRevision: r.abs(4), Revision: r.abs(5)}))
	r.get("min mod revision -1", all(&pb.RangeRequest{MinModRevision: -1}))
	r.get("max mod revision -1", all(&pb.RangeRequest{MaxModRevision: -1}))
	r.get("keys only, count only", all(&pb.RangeRequest{KeysOnly: true, CountOnly: true}))
	r.get("keys only, limit 3", all(&pb.RangeRequest{KeysOnly: true, Limit: 3}))
	r.get("limit 5 of 5", all(&pb.RangeRequest{Limit: 5}))
	r.get("limit 6 of 5", all(&pb.RangeRequest{Limit: 6}))
	r.get("limit 4 of 5", all(&pb.RangeRequest{Limit: 4}))
	r.get("limit 1 at revision 3", all(&pb.RangeRequest{Limit: 1, Revision: r.abs(3)}))
	r.get("serializable at revision 4", all(&pb.RangeRequest{Serializable: true, Revision: r.abs(4)}))
	r.get("serializable count only", all(&pb.RangeRequest{Serializable: true, CountOnly: true}))
	r.get("from c to the end", &pb.RangeRequest{Key: r.key("c"), RangeEnd: []byte{0}, CountOnly: true})
	r.get("from c to d", &pb.RangeRequest{Key: r.key("c"), RangeEnd: r.key("d")})
	r.get("from c to c", &pb.RangeRequest{Key: r.key("c"), RangeEnd: r.key("c")})
	r.get("key c with an empty end", &pb.RangeRequest{Key: r.key("c"), RangeEnd: []byte{}})
	r.get("every key, count only", &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, CountOnly: true})
	r.get("key \\x00 alone", &pb.RangeRequest{Key: []byte{0}})
	r.get("a missing key", &pb.RangeRequest{Key: r.key("z")})
	r.get("a missing key, count only", &pb.RangeRequest{Key: r.key("z"), CountOnly: true})
	r.get("b at revision 2", &pb.RangeRequest{Key: r.key("b"), Revision: r.abs(2)})
	r.get("b at revision 1, before it was written", &pb.RangeRequest{Key: r.key("b"), Revision: r.abs(1)})
	r.get("b at the current revision", &pb.RangeRequest{Key: r.key("b"), Revision: r.abs(7)})
	r.get("b at the next revision", &pb.RangeRequest{Key: r.key("b"), Revision: r.abs(8)})
}

// writeForms are the forms of Put and DeleteRange.
func writeForms(r *run) {
	k := r.key("k")
	r.put("put k with prev_kv, new", &pb.PutRequest{Key: k, Value: []byte("1"), PrevKv: true})
	r.put("put k with an empty value", &pb.PutRequest{Key: k, PrevKv: true})
	r.put("put k ignore_value with prev_kv", &pb.PutRequest{Key: k, IgnoreValue: true, PrevKv: true})
	r.put("put k ignore_lease with a lease", &pb.PutRequest{Key: k, Value: []byte("2"), IgnoreLease: true, Lease: 1})
	r.put("put k ignore_value and ignore_lease", &pb.PutRequest{Key: k, IgnoreValue: true, IgnoreLease: true, PrevKv: true})
	r.put("put key \\x00", &pb.PutRequest{Key: []byte{0}, Value: []byte("zero")})
	r.get("get key \\x00", &pb.RangeRequest{Key: []byte{0}})
	r.del("delete key \\x00", &pb.DeleteRangeRequest{Key: []byte{0}, PrevKv: true})
	r.put("put a binary key", &pb.PutRequest{Key: r.key("\xff\x00\xfe"), Value: []byte{0, 1}})
	r.get("get the binary key", &pb.RangeRequest{Key: r.key("\xff\x00\xfe")})
	for _, n := range []string{"a", "b", "c", "d"} {
		r.put("put "+n, &pb.PutRequest{Key: r.key(n), Value: []byte(n)})
	}
	r.del("delete b to d with prev_kv", &pb.DeleteRangeRequest{Key: r.key("b"), RangeEnd: r.key("d"), PrevKv: true})
	r.del("delete from d to c", &pb.DeleteRangeRequest{Key: r.key("d"), RangeEnd: r.key("c")})
	r.del("delete from c to c", &pb.DeleteRangeRequest{Key: r.key("c"), RangeEnd: r.key("c")})
	r.del("delete with an empty end", &pb.DeleteRangeRequest{Key: r.key("a"), RangeEnd: []byte{}})
	r.del("delete a missing key with prev_kv", &pb.DeleteRangeRequest{Key: r.key("a"), PrevKv: true})
	r.del("delete from d to the end", &pb.DeleteRangeRequest{Key: r.key("d"), RangeEnd: []byte{0}, PrevKv: true})
	r.get("what is left", &pb.RangeRequest{Key: r.key(""), RangeEnd: prefixEnd(r.key(""))})
}

// compare compares key's target with v, a string for VALUE and an int64
// for the other targets.
func compare(key []byte, target pb.Compare_CompareTarget, result pb.Compare_CompareResult, v any) *pb.Compare {
	c := &pb.Compare{Key: key, Target: target, Result: result}
	s, _ := v.(string)
	n, _ := v.(int64)
	switch target {
	case pb.Compare_VALUE:
		c.TargetUnion = &pb.Compare_Value{Value: []byte(s)}
	case pb.Compare_VERSION:
		c.TargetUnion = &pb.Compare_Version{Version: n}
	case pb.Compare_CREATE:
		c.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: n}
	case pb.Compare_MOD:
		c.TargetUnion = &pb.Compare_ModRevision{ModRevision: n}
	case pb.Compare_LEASE:
		c.TargetUnion = &pb.Compare_Lease{Lease: n}
	}
	return c
}

// txnForms are the compares and operations of Txn.
func txnForms(r *run) {
	a, b, c := r.key("a"), r.key("b"), r.key("c")
	r.put("put a=5", &pb.PutRequest{Key: a, Value: []byte("5")})
	r.put("put b=5", &pb.PutRequest{Key: b, Value: []byte("5")})
	r.put("put b=7", &pb.PutRequest{Key: b, Value: []byte("7")})
	results := []pb.Compare_CompareResult{pb.Compare_EQUAL, pb.Compare_GREATER, pb.Compare_LESS, pb.Compare_NOT_EQUAL}
	for _, res := range results {
		for _, v := range []string{"4", "5", "6", ""} {
			r.txn("value(a) "+res.String()+" "+v, &pb.TxnRequest{Compare: []*pb.Compare{compare(a, pb.Compare_VALUE, res, v)}})
		}
		r.txn("value(missing) "+res.String()+" empty", &pb.TxnRequest{Compare: []*pb.Compare{compare(c, pb.Compare_VALUE, res, "")}})
		for _, target := range []pb.Compare_CompareTarget{pb.Compare_VERSION, pb.Compare_CREATE, pb.Compare_MOD, pb.Compare_LEASE} {
			for _, v := range []int64{0, 1, 2} {
				want := v
				if target == pb.Compare_CREATE || target == pb.Compare_MOD {
					want = r.abs(v)
				}
				r.txn(target.String()+"(b) "+res.String()+" "+strconv.FormatInt(v, 10), &pb.TxnRequest{Compare: []*pb.Compare{compare(b, target, res, want)}})
			}
			r.txn(target.String()+"(missing) "+res.String()+" 0", &pb.TxnRequest{Compare: []*pb.Compare{compare(c, target, res, int64(0))}})
		}
	}
	all := prefixEnd(r.key(""))
	ranged := func(target pb.Compare_CompareTarget, res pb.Compare_CompareResult, v any) *pb.Compare {
		c := compare(r.key(""), target, res, v)
		c.RangeEnd = all
		return c
	}
	r.txn("value(a..) EQUAL 5", &pb.TxnRequest{Compare: []*pb.Compare{ranged(pb.Compare_VALUE, pb.Compare_EQUAL, "5")}})
	r.txn("value(a..) GREATER 4", &pb.TxnRequest{Compare: []*pb.Compare{ranged(pb.Compare_VALUE, pb.Compare_GREATER, "4")}})
	r.txn("version(a..) LESS 3", &pb.TxnRequest{Compare: []*pb.Compare{ranged(pb.Compare_VERSION, pb.Compare_LESS, int64(3))}})
	r.txn("version(a..) EQUAL 1", &pb.TxnRequest{Compare: []*pb.Compare{ranged(pb.Compare_VERSION, pb.Compare_EQUAL, int64(1))}})
	r.txn("mod(a..) GREATER 0", &pb.TxnRequest{Compare: []*pb.Compare{ranged(pb.Compare_MOD, pb.Compare_GREATER, int64(0))}})
	emptyRange := compare(c, pb.Compare_VERSION, pb.Compare_EQUAL, int64(0))
	emptyRange.RangeEnd = r.key("d")
	r.txn("version(c..d) EQUAL 0, no keys", &pb.TxnRequest{Compare: []*pb.Compare{emptyRange}})
	toEnd := compare(b, pb.Compare_VERSION, pb.Compare_GREATER, int64(0))
	toEnd.RangeEnd = []byte{0}
	r.txn("version(b to the end) GREATER 0", &pb.TxnRequest{Compare: []*pb.Compare{toEnd}})
	r.txn("compare result 4", &pb.TxnRequest{Compare: []*pb.Compare{compare(a, pb.Compare_VERSION, 4, int64(1))}})
	r.txn("compare target 5", &pb.TxnRequest{Compare: []*pb.Compare{{Key: a, Target: 5, Result: pb.Compare_EQUAL}}})
	r.txn("compare VALUE without a value", &pb.TxnRequest{Compare: []*pb.Compare{{Key: a, Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL}}})
	r.txn("compare VERSION with a mod revision", &pb.TxnRequest{Compare: []*pb.Compare{{Key: a, Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: r.abs(1)}}}})
	r.txn("compare an empty key", &pb.TxnRequest{Compare: []*pb.Compare{compare(nil, pb.Compare_VERSION, pb.Compare_EQUAL, int64(0))}})
	r.txn("two compares, both true", &pb.TxnRequest{Compare: []*pb.Compare{compare(a, pb.Compare_VALUE, pb.Compare_EQUAL, "5"), compare(b, pb.Compare_VALUE, pb.Compare_EQUAL, "7")}, Success: []*pb.RequestOp{getOp(a)}, Failure: []*pb.RequestOp{getOp(b)}})
	r.txn("two compares, one false", &pb.TxnRequest{Compare: []*pb.Compare{compare(a, pb.Compare_VALUE, pb.Compare_EQUAL, "5"), compare(b, pb.Compare_VALUE, pb.Compare_EQUAL, "5")}, Success: []*pb.RequestOp{getOp(a)}, Failure: []*pb.RequestOp{getOp(b)}})
	r.txn("empty txn", &pb.TxnRequest{})
	r.txn("txn with a nil operation", &pb.TxnRequest{Success: []*pb.RequestOp{{}}})
	r.txn("txn putting an empty key", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(nil, "x")}})
	r.txn("txn ranging an empty key", &pb.TxnRequest{Success: []*pb.RequestOp{getOp(nil)}})
	r.txn("txn deleting an empty key", &pb.TxnRequest{Success: []*pb.RequestOp{deleteOp(nil)}})
	r.txn("txn putting with a missing lease", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: c, Value: []byte("x"), Lease: 0x7a11}}}}})
	r.txn("txn putting ignore_value on a missing key", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: c, IgnoreValue: true}}}}})
	r.txn("txn ranging at a future revision", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: a, Revision: r.abs(100)}}}}})
	r.txn("txn ranging in the branch not taken at a future revision", &pb.TxnRequest{Compare: []*pb.Compare{compare(a, pb.Compare_VALUE, pb.Compare_EQUAL, "0")}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: a, Revision: r.abs(100)}}}}})
	r.txn("txn putting a key and deleting its range", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(b, "1"), {Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: a, RangeEnd: c}}}}})
	r.txn("txn deleting a range and putting a key outside it", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: c, RangeEnd: r.key("d"), PrevKv: true}}}, putOp(b, "8")}})
	r.txn("txn deleting one key twice", &pb.TxnRequest{Success: []*pb.RequestOp{deleteOp(c), deleteOp(c)}})
	r.txn("txn putting one key in both branches", &pb.TxnRequest{Success: []*pb.RequestOp{putOp(c, "1")}, Failure: []*pb.RequestOp{putOp(c, "2")}})
	r.txn("txn with prev_kv operations", &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: a, Value: []byte("6"), PrevKv: true}}},
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: b, PrevKv: true}}},
		getOp(a),
	}})
	r.txn("txn with ranges of every form", &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key(""), RangeEnd: all, CountOnly: true}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key(""), RangeEnd: all, KeysOnly: true, SortOrder: pb.RangeRequest_DESCEND}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key(""), RangeEnd: all, Limit: 1, Revision: r.abs(2)}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key(""), RangeEnd: all, MinModRevision: r.abs(3)}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: r.key(""), RangeEnd: all, Serializable: true}}},
	}})
	nested := func(t *pb.TxnRequest) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: t}}
	}
	r.txn("nested txn", &pb.TxnRequest{Success: []*pb.RequestOp{
		putOp(r.key("n1"), "1"),
		nested(&pb.TxnRequest{Compare: []*pb.Compare{compare(r.key("n1"), pb.Compare_VERSION, pb.Compare_EQUAL, int64(0))}, Success: []*pb.RequestOp{putOp(r.key("n2"), "2")}, Failure: []*pb.RequestOp{putOp(r.key("n3"), "3"), getOp(r.key("n1"))}}),
		getOp(r.key("n3")),
	}})
	r.txn("nested txn two levels deep", &pb.TxnRequest{Success: []*pb.RequestOp{
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(r.key("n4"), "4")}})}}),
	}})
	r.txn("nested txn putting a key of the outer one", &pb.TxnRequest{Success: []*pb.RequestOp{
		putOp(r.key("n5"), "5"),
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(r.key("n5"), "6")}}),
	}})
	r.txn("nested txns putting one key in different branches", &pb.TxnRequest{Success: []*pb.RequestOp{
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(r.key("n6"), "1")}, Failure: []*pb.RequestOp{putOp(r.key("n6"), "2")}}),
	}})
	r.txn("two nested txns putting one key", &pb.TxnRequest{Success: []*pb.RequestOp{
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(r.key("n7"), "1")}}),
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(r.key("n7"), "2")}}),
	}})
	r.txn("nested txn with an error", &pb.TxnRequest{Success: []*pb.RequestOp{
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{putOp(nil, "x")}}),
	}})
	r.txn("nested read-only txn", &pb.TxnRequest{Success: []*pb.RequestOp{
		nested(&pb.TxnRequest{Success: []*pb.RequestOp{getOp(a)}}),
	}})
	ops := func(n int) []*pb.RequestOp {
		o := make([]*pb.RequestOp, n)
		for i := range o {
			o[i] = getOp(a)
		}
		return o
	}
	r.txn("nested txn with 128 operations", &pb.TxnRequest{Success: []*pb.RequestOp{nested(&pb.TxnRequest{Success: ops(128)})}})
	r.txn("nested txn with 129 operations", &pb.TxnRequest{Success: []*pb.RequestOp{nested(&pb.TxnRequest{Success: ops(129)})}})
	r.txn("txn with 129 compares", &pb.TxnRequest{Compare: slices.Repeat([]*pb.Compare{compare(a, pb.Compare_VERSION, pb.Compare_GREATER, int64(0))}, 129)})
	r.txn("txn with 129 operations in failure", &pb.TxnRequest{Failure: ops(129)})
	r.get("what is left", &pb.RangeRequest{Key: r.key(""), RangeEnd: all})
}

// watchStep is one thing a recorded Watch stream does: send a request, or
// write through KV.
type watchStep struct {
	do  string
	req *pb.WatchRequest
	put *pb.PutRequest
	del *pb.DeleteRangeRequest
}

func create(req *pb.WatchCreateRequest) *pb.WatchRequest {
	return &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: req}}
}

func cancelWatch(id int64) *pb.WatchRequest {
	return &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{CancelRequest: &pb.WatchCancelRequest{WatchId: id}}}
}

func progress() *pb.WatchRequest {
	return &pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}
}

// watchStream records one Watch stream. It does each step in order and
// records, as a step of its own, what the stream answered until 300 ms
// passed without an answer: by watch ID, the creation, the events, the
// cancel, and progress notifications. The events of a watch are one list,
// since how the server batches them into responses may differ.
func (r *run) watchStream(steps []watchStep) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	defer cancel()
	stream, err := r.c.watch.Watch(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	type recv struct {
		resp *pb.WatchResponse
		err  error
	}
	recvs := make(chan recv, 64)
	go func() {
		for {
			resp, err := stream.Recv()
			recvs <- recv{resp, err}
			if err != nil {
				return
			}
		}
	}()
	type event struct {
		Type   string `json:"type"`
		KV     *kv    `json:"kv"`
		PrevKV *kv    `json:"prevKV,omitempty"`
	}
	type answer struct {
		Created         bool    `json:"created,omitempty"`
		CreatedRevision int64   `json:"createdRevision,omitempty"`
		Canceled        bool    `json:"canceled,omitempty"`
		CompactRevision int64   `json:"compactRevision,omitempty"`
		CancelReason    string  `json:"cancelReason,omitempty"`
		Events          []event `json:"events,omitempty"`
		Progress        []int64 `json:"progress,omitempty"`
	}
	for _, s := range steps {
		var err error
		switch {
		case s.req != nil:
			err = stream.Send(s.req)
		case s.put != nil:
			_, err = r.c.kv.Put(r.ctx, s.put)
		case s.del != nil:
			_, err = r.c.kv.DeleteRange(r.ctx, s.del)
		}
		if err != nil {
			r.t.Fatalf("%s: %v", s.do, err)
		}
		answers := map[int64]*answer{}
		var end error
	collect:
		for {
			select {
			case got := <-recvs:
				if got.err != nil {
					end = got.err
					break collect
				}
				resp := got.resp
				a := answers[resp.WatchId]
				if a == nil {
					a = &answer{}
					answers[resp.WatchId] = a
				}
				if !resp.Created && !resp.Canceled && len(resp.Events) == 0 {
					a.Progress = append(a.Progress, r.rel(resp.Header.GetRevision()))
				}
				if resp.Created {
					a.Created, a.CreatedRevision = true, r.rel(resp.Header.GetRevision())
				}
				a.Canceled = a.Canceled || resp.Canceled
				if resp.CompactRevision != 0 {
					a.CompactRevision = r.rel(resp.CompactRevision)
				}
				if resp.CancelReason != "" {
					a.CancelReason = resp.CancelReason
				}
				for _, e := range resp.Events {
					a.Events = append(a.Events, event{Type: e.Type.String(), KV: r.kv(e.Kv), PrevKV: r.kv(e.PrevKv)})
				}
			case <-time.After(300 * time.Millisecond):
				break collect
			}
		}
		if end != nil && !errors.Is(end, io.EOF) {
			r.record(s.do, nil, end)
			return
		}
		r.record(s.do, answers, nil)
		if end != nil {
			r.record("the stream ended", nil, nil)
			return
		}
	}
}

// watchForms are the forms of Watch requests, on a few streams.
func watchForms(r *run) {
	a, b := r.key("a"), r.key("b")
	all := prefixEnd(r.key(""))
	putA := func(do, v string) watchStep {
		return watchStep{do: do, put: &pb.PutRequest{Key: a, Value: []byte(v)}}
	}
	r.watchStream([]watchStep{
		{do: "watch a", req: create(&pb.WatchCreateRequest{Key: a})},
		{do: "watch the case's keys without puts, with prev_kv", req: create(&pb.WatchCreateRequest{Key: r.key(""), RangeEnd: all, Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NOPUT}, PrevKv: true})},
		{do: "watch the case's keys without deletes", req: create(&pb.WatchCreateRequest{Key: r.key(""), RangeEnd: all, Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NODELETE}})},
		{do: "watch the case's keys without puts and deletes", req: create(&pb.WatchCreateRequest{Key: r.key(""), RangeEnd: all, Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NOPUT, pb.WatchCreateRequest_NODELETE}})},
		{do: "watch from b to the end", req: create(&pb.WatchCreateRequest{Key: b, RangeEnd: []byte{0}})},
		{do: "watch every key", req: create(&pb.WatchCreateRequest{RangeEnd: []byte{0}})},
		{do: "watch filter 2", req: create(&pb.WatchCreateRequest{Key: a, Filters: []pb.WatchCreateRequest_FilterType{2}})},
		putA("put a=1", "1"),
		{do: "put b=1", put: &pb.PutRequest{Key: b, Value: []byte("1")}},
		putA("put a=2", "2"),
		{do: "delete a", del: &pb.DeleteRangeRequest{Key: a}},
		{do: "ask for progress", req: progress()},
		{do: "cancel watch 0", req: cancelWatch(0)},
		putA("put a=3 after the cancel", "3"),
		{do: "cancel watch 0 again", req: cancelWatch(0)},
		{do: "cancel watch 99, which does not exist", req: cancelWatch(99)},
		{do: "an empty request", req: &pb.WatchRequest{}},
		{do: "a create request without a body", req: &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{}}},
		{do: "a cancel request without a body", req: &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{}}},
		{do: "ask for progress again", req: progress()},
	})
	r.watchStream([]watchStep{
		{do: "watch a as ID 7", req: create(&pb.WatchCreateRequest{Key: a, WatchId: 7})},
		{do: "watch b as ID 7 too", req: create(&pb.WatchCreateRequest{Key: b, WatchId: 7})},
		{do: "watch b with the next automatic ID", req: create(&pb.WatchCreateRequest{Key: b})},
		{do: "watch a as ID 1", req: create(&pb.WatchCreateRequest{Key: a, WatchId: 1})},
		{do: "watch b with the next automatic ID again", req: create(&pb.WatchCreateRequest{Key: b})},
		{do: "watch a as ID -5", req: create(&pb.WatchCreateRequest{Key: a, WatchId: -5})},
		putA("put a=4", "4"),
		{do: "cancel ID 7", req: cancelWatch(7)},
		{do: "watch b as ID 7 again", req: create(&pb.WatchCreateRequest{Key: b, WatchId: 7})},
		{do: "put b=2", put: &pb.PutRequest{Key: b, Value: []byte("2")}},
	})
	r.watchStream([]watchStep{
		{do: "watch an empty range", req: create(&pb.WatchCreateRequest{Key: b, RangeEnd: a})},
		{do: "watch from a to a", req: create(&pb.WatchCreateRequest{Key: a, RangeEnd: a})},
		{do: "watch a with an empty end", req: create(&pb.WatchCreateRequest{Key: a, RangeEnd: []byte{}})},
		{do: "watch a from revision 1 with prev_kv", req: create(&pb.WatchCreateRequest{Key: a, StartRevision: r.abs(1), PrevKv: true})},
		{do: "watch a from the current revision", req: create(&pb.WatchCreateRequest{Key: a, StartRevision: r.abs(7)})},
		{do: "watch a from 2 ahead", req: create(&pb.WatchCreateRequest{Key: a, StartRevision: r.abs(9)})},
		{do: "watch a with progress_notify and fragment", req: create(&pb.WatchCreateRequest{Key: a, ProgressNotify: true, Fragment: true})},
		putA("put a=5", "5"),
		putA("put a=6", "6"),
		{do: "ask for progress", req: progress()},
	})
}

// leaseForms are the forms of the Lease requests.
func leaseForms(r *run) {
	id := r.base*1000 + 7
	short := r.grant("grant with a negative TTL", 0, -5)
	r.revoke("revoke it", short)
	largest := r.grant("grant with the largest TTL", 0, 9000000000)
	r.revoke("revoke that", largest)
	r.grant("grant", id, 60)
	r.put("put k with the lease", &pb.PutRequest{Key: r.key("k"), Value: []byte("v"), Lease: id})
	r.put("put j with the lease", &pb.PutRequest{Key: r.key("j"), Value: []byte("v"), Lease: id})
	r.leaseTTL("time to live", id, false)
	r.leaseTTL("time to live with keys", id, true)
	r.leaseTTL("time to live of a missing lease", id+1, true)
	r.leaseTTL("time to live of lease 0", 0, false)
	r.keepAlive("keep alive", id, id+1, 0, id)
	r.put("move j to no lease", &pb.PutRequest{Key: r.key("j"), Value: []byte("w")})
	r.leaseTTL("time to live with keys after the move", id, true)
	r.del("delete k", &pb.DeleteRangeRequest{Key: r.key("k")})
	r.leaseTTL("time to live with keys after the delete", id, true)
	r.leaseLeases("leases")
	r.revoke("revoke", id)
	r.leaseTTL("time to live after the revoke", id, true)
	r.keepAlive("keep alive after the revoke", id)
	r.leaseLeases("leases after the revoke")
	r.put("put with the revoked lease", &pb.PutRequest{Key: r.key("k"), Value: []byte("v"), Lease: id})
	r.put("put with a negative lease", &pb.PutRequest{Key: r.key("k"), Value: []byte("v"), Lease: -1})
}

// leaseTTL records LeaseTimeToLive. The TTL left depends on timing, so
// for a lease with a granted TTL only whether it is within 5 s of it
// is recorded. etcd lists the keys in map order, so they are sorted.
func (r *run) leaseTTL(do string, id int64, keys bool) {
	r.t.Helper()
	resp, err := r.c.lease.LeaseTimeToLive(r.ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: keys})
	var res any
	if err == nil {
		var ks []string
		for _, k := range resp.Keys {
			ks = append(ks, r.nsKey(k))
		}
		slices.Sort(ks)
		ttl := "near granted"
		if resp.GrantedTTL == 0 || resp.TTL <= resp.GrantedTTL-5 || resp.TTL > resp.GrantedTTL {
			ttl = time.Duration(resp.TTL * int64(time.Second)).String()
		}
		res = struct {
			Revision   int64    `json:"revision"`
			Lease      int64    `json:"lease"`
			TTL        string   `json:"ttl"`
			GrantedTTL int64    `json:"grantedTTL"`
			Keys       []string `json:"keys,omitempty"`
		}{r.rel(resp.Header.GetRevision()), r.lease(resp.ID), ttl, resp.GrantedTTL, ks}
	}
	r.record(do, res, err)
}

// keepAlive records the answers of one LeaseKeepAlive stream to a request
// for each of ids.
func (r *run) keepAlive(do string, ids ...int64) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	s, err := r.c.lease.LeaseKeepAlive(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	type answer struct {
		Revision int64 `json:"revision"`
		Lease    int64 `json:"lease"`
		TTL      int64 `json:"ttl"`
	}
	var res []answer
	for _, id := range ids {
		if err := s.Send(&pb.LeaseKeepAliveRequest{ID: id}); err != nil {
			r.t.Fatalf("%s: %v", do, err)
		}
		resp, err := s.Recv()
		if err != nil {
			r.record(do, res, err)
			return
		}
		res = append(res, answer{r.rel(resp.Header.GetRevision()), r.lease(resp.ID), resp.TTL})
	}
	r.record(do, res, nil)
}

// leaseLeases records the leases of this case that LeaseLeases lists.
func (r *run) leaseLeases(do string) {
	r.t.Helper()
	resp, err := r.c.lease.LeaseLeases(r.ctx, &pb.LeaseLeasesRequest{})
	var res any
	if err == nil {
		var ns []int64
		for _, l := range resp.Leases {
			if _, ok := r.leases[l.ID]; ok {
				ns = append(ns, r.lease(l.ID))
			}
		}
		slices.Sort(ns)
		res = struct {
			Revision int64   `json:"revision"`
			Leases   []int64 `json:"leases"`
		}{r.rel(resp.Header.GetRevision()), ns}
	}
	r.record(do, res, err)
}

// rawCodec sends the bytes it is given as the request.
type rawCodec struct{}

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.([]byte))}, nil
}

func (rawCodec) Unmarshal(mem.BufferSlice, any) error { return nil }

func (rawCodec) Name() string { return "proto" }

var _ encoding.CodecV2 = rawCodec{}

// unknownMethods are calls to methods and services etcd does not have,
// and requests it cannot decode.
func unknownMethods(r *run) {
	r.invoke("KV.Nope", "/etcdserverpb.KV/Nope")
	r.invoke("Watch.Range", "/etcdserverpb.Watch/Range")
	r.invoke("Lease.Nope", "/etcdserverpb.Lease/Nope")
	r.invoke("Nope.Range", "/etcdserverpb.Nope/Range")
	r.invoke("a method without a service", "/Range")
	r.invoke("KV.Range with an empty request", "/etcdserverpb.KV/Range")
	r.invoke("Lease.LeaseGrant with an empty request", "/etcdserverpb.Lease/LeaseGrant")
	raw := func(do, path string, b []byte) {
		r.t.Helper()
		err := r.c.conn.Invoke(r.ctx, path, b, new(int), grpc.ForceCodecV2(rawCodec{}))
		r.record(do, nil, err)
	}
	raw("KV.Range with a malformed request", "/etcdserverpb.KV/Range", []byte{0x0a, 0x05, 'a'})
	raw("KV.Put with a field of the wrong wire type", "/etcdserverpb.KV/Put", []byte{0x0d, 1, 2, 3, 4})
	raw("KV.Range with an unknown field", "/etcdserverpb.KV/Range", append([]byte{0x0a, 1, 'a'}, 0xf8, 0x3e, 1))
}
