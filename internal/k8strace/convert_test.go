package k8strace

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

// logOf gzips records as cmd/etcdrec writes them, numbering their times.
func logOf(t *testing.T, recs ...Record) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for i := range recs {
		recs[i].T = int64(i+1) * 1000
		if err := enc.Encode(&recs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func unary(id int64, method string, req *Request, resp *Response) []Record {
	return []Record{
		{Event: EventOpen, Call: id, Method: method},
		{Event: EventReq, Call: id, Req: req},
		{Event: EventResp, Call: id, Resp: resp},
		{Event: EventEnd, Call: id},
	}
}

func cas(mod int64, size int) *Request {
	const key = "/a"
	return &Request{Txn: &Txn{
		Compare: []LogCompare{{Target: TargetMod, Key: key, Value: mod}},
		Success: []LogOp{{Put: &Put{Key: key, Size: size}}},
		Failure: []LogOp{{Range: &Range{Key: key}}},
	}}
}

// TestConvert follows revisions from a log into references: a key written
// before the window comes from the preload, a revision a GET returned
// waits on that GET, one a watch event carried waits on the write that
// made it, a lease waits on its grant, and a LIST's next page waits on
// the first page whose revision it reads at.
func TestConvert(t *testing.T) {
	var recs []Record
	add := func(r []Record) { recs = append(recs, r...) }
	// Before the window: /a is created at revision 5; a watch on /
	// opens.
	add(unary(1, MethodTxn, cas(0, 10), &Response{Rev: 5, Succeeded: true, Ops: []Response{{Rev: 5}}}))
	recs = append(recs,
		Record{Event: EventOpen, Call: 2, Method: MethodWatch},
		Record{Event: EventReq, Call: 2, Req: &Request{Watch: &WatchRequest{Create: &WatchCreate{Key: "/", End: "0", PrevKV: true}}}},
		Record{Event: EventResp, Call: 2, Resp: &Response{Rev: 5, Watch: &WatchResponse{ID: 7, Created: true}}},
	)
	start := int64(len(recs)+1) * 1000
	// The window. Call 0: update /a from the preload's revision.
	add(unary(3, MethodTxn, cas(5, 20), &Response{Rev: 6, Succeeded: true, Ops: []Response{{Rev: 6}}}))
	// The watch delivers /a at 6. Call 1: update /a from that event.
	recs = append(recs, Record{Event: EventResp, Call: 2, Resp: &Response{Rev: 6, Watch: &WatchResponse{ID: 7, Events: []Event{{KV: KV{Key: "/a", Mod: 6, Size: 20}}}}}})
	add(unary(4, MethodTxn, cas(6, 30), &Response{Rev: 7, Succeeded: true, Ops: []Response{{Rev: 7}}}))
	// Call 2: a GET of /a. Call 3: an update from what it returned.
	add(unary(5, MethodRange, &Request{Range: &Range{Key: "/a"}}, &Response{Rev: 7, Count: 1, KVs: []KV{{Key: "/a", Mod: 7}}}))
	add(unary(6, MethodTxn, cas(7, 40), &Response{Rev: 8, Succeeded: true, Ops: []Response{{Rev: 8}}}))
	// Call 4: a grant. Call 5: a put with its lease.
	add(unary(7, MethodLeaseGrant, &Request{LeaseGrant: &LeaseGrant{TTL: 3600}}, &Response{Rev: 8, Lease: 99, TTL: 3600}))
	add(unary(8, MethodPut, &Request{Put: &Put{Key: "/e", Size: 5, Lease: 99}}, &Response{Rev: 9}))
	// Call 6: a LIST page. Call 7: the next page at its revision.
	add(unary(9, MethodRange, &Request{Range: &Range{Key: "/", End: "0", Limit: 1}}, &Response{Rev: 9, Count: 2, More: true, KVs: []KV{{Key: "/a", Mod: 8}}}))
	add(unary(10, MethodRange, &Request{Range: &Range{Key: "/a\x00", End: "0", Limit: 1, Rev: 9}}, &Response{Rev: 9, Count: 1, KVs: []KV{{Key: "/e", Mod: 9}}}))
	// Call 8: a stale update, from a revision no key has, which fails.
	add(unary(11, MethodTxn, cas(3, 50), &Response{Rev: 9, Ops: []Response{{Rev: 9, KVs: []KV{{Key: "/a", Mod: 8}}}}}))
	// Call 9: a progress request on the watch.
	recs = append(recs, Record{Event: EventReq, Call: 2, Req: &Request{Watch: &WatchRequest{Progress: true}}})
	end := int64(len(recs)+1) * 1000
	// After the window: nothing of this counts.
	add(unary(12, MethodPut, &Request{Put: &Put{Key: "/late", Size: 1}}, &Response{Rev: 10}))

	tr, dropped, err := Convert(logOf(t, recs...), Window{Start: start, End: end})
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 1 || dropped["revision no key had when the window opened, compared as stale"] != 1 {
		t.Errorf("dropped %v, want only the stale compare", dropped)
	}
	key := func(k string) int32 {
		for i, x := range tr.Keys {
			if x == k {
				return int32(i)
			}
		}
		t.Fatalf("no key %q", k)
		return -1
	}
	if want := []Item{{Key: key("/a"), Size: 10}}; !reflect.DeepEqual(tr.Preload, want) {
		t.Errorf("preload %+v, want %+v", tr.Preload, want)
	}
	if len(tr.Watches) != 1 || len(tr.Watches[0].Creates) != 1 || !tr.Watches[0].Creates[0].PrevKV {
		t.Errorf("watches %+v, want the one on /", tr.Watches)
	}
	type want struct {
		kind  uint8
		waits []int32
		rev   Rev
	}
	rev := func(c *Call) Rev {
		if c.Kind == CallTxn {
			return c.Op.Compare[0].Rev
		}
		return c.Op.Rev
	}
	wants := []want{
		{CallTxn, nil, -1},       // the preload's /a
		{CallTxn, []int32{0}, 1}, // the event of call 0's write
		{CallRange, nil, 0},
		{CallTxn, []int32{1, 2}, 2}, // what the GET returned, written by call 1
		{CallLeaseGrant, nil, 0},
		{CallPut, []int32{4}, 0},
		{CallRange, nil, 0},
		{CallRange, []int32{6}, 7}, // the first page's revision
		{CallTxn, nil, RevStale},
		{CallProgress, nil, 0},
	}
	if len(tr.Calls) != len(wants) {
		t.Fatalf("%d calls, want %d: %+v", len(tr.Calls), len(wants), tr.Calls)
	}
	for i, w := range wants {
		c := &tr.Calls[i]
		if c.Kind != w.kind || !reflect.DeepEqual(c.Waits, w.waits) || c.Kind != CallProgress && rev(c) != w.rev {
			t.Errorf("call %d: kind %d waits %v rev %d, want %+v", i, c.Kind, c.Waits, rev(c), w)
		}
	}
	if tr.Calls[5].Op.Lease != 5 {
		t.Errorf("the put's lease is %d, want the grant of call 4", tr.Calls[5].Op.Lease)
	}
	if !tr.Calls[8].Failed || tr.Calls[0].Failed {
		t.Error("only the stale update failed")
	}

	path := filepath.Join(t.TempDir(), "t.trace.gz")
	if err := tr.Write(path); err != nil {
		t.Fatal(err)
	}
	back, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, tr) {
		t.Errorf("read back\n%+v\nwant\n%+v", back, tr)
	}
}

// TestConvertRestartsAndExpiry keeps a restarted watch as one stream, with
// the progress requests of both pointing at it, and drops a lease whose
// TTL ran out before the window, with its key, while a live one stays.
func TestConvertRestartsAndExpiry(t *testing.T) {
	watch := &Request{Watch: &WatchRequest{Create: &WatchCreate{Key: "/r/", End: "/r0", PrevKV: true}}}
	progress := &Request{Watch: &WatchRequest{Progress: true}}
	recs := []Record{
		// A 1 s lease and a 1 h lease, each with a key.
		{T: 1, Event: EventOpen, Call: 1, Method: MethodLeaseGrant},
		{T: 2, Event: EventReq, Call: 1, Req: &Request{LeaseGrant: &LeaseGrant{TTL: 1}}},
		{T: 3, Event: EventResp, Call: 1, Resp: &Response{Rev: 1, Lease: 11, TTL: 1}},
		{T: 4, Event: EventOpen, Call: 2, Method: MethodLeaseGrant},
		{T: 5, Event: EventReq, Call: 2, Req: &Request{LeaseGrant: &LeaseGrant{TTL: 3600}}},
		{T: 6, Event: EventResp, Call: 2, Resp: &Response{Rev: 1, Lease: 12, TTL: 3600}},
		{T: 7, Event: EventOpen, Call: 3, Method: MethodPut},
		{T: 8, Event: EventReq, Call: 3, Req: &Request{Put: &Put{Key: "/short", Size: 1, Lease: 11}}},
		{T: 9, Event: EventResp, Call: 3, Resp: &Response{Rev: 2}},
		{T: 10, Event: EventOpen, Call: 4, Method: MethodPut},
		{T: 11, Event: EventReq, Call: 4, Req: &Request{Put: &Put{Key: "/long", Size: 1, Lease: 12}}},
		{T: 12, Event: EventResp, Call: 4, Resp: &Response{Rev: 3}},
		// A watch, open when the window opens 2 s later, restarted in it.
		{T: 13, Event: EventOpen, Call: 5, Method: MethodWatch},
		{T: 14, Event: EventReq, Call: 5, Req: watch},
		{T: 2e9, Event: EventReq, Call: 5, Req: progress},
		{T: 2e9 + 1, Event: EventEnd, Call: 5, Err: "canceled"},
		{T: 2e9 + 2, Event: EventOpen, Call: 6, Method: MethodWatch},
		{T: 2e9 + 3, Event: EventReq, Call: 6, Req: watch},
		{T: 2e9 + 4, Event: EventReq, Call: 6, Req: progress},
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	tr, _, err := Convert(&buf, Window{Start: 2e9, End: 3e9})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Watches) != 1 {
		t.Errorf("%d watch streams, want the restarted one once", len(tr.Watches))
	}
	if len(tr.Calls) != 2 || tr.Calls[0].Stream != 0 || tr.Calls[1].Stream != 0 {
		t.Errorf("progress calls %+v, want both on stream 0", tr.Calls)
	}
	if len(tr.Leases) != 1 || tr.Leases[0] != 3600 {
		t.Errorf("leases %v, want only the 1 h one", tr.Leases)
	}
	if len(tr.Preload) != 1 || tr.Keys[tr.Preload[0].Key] != "/long" {
		t.Errorf("preload %+v, want only /long", tr.Preload)
	}
}
