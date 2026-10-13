package main

import (
	"bytes"
	"math"
	"testing"

	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protowire"
)

func testMixed(t *testing.T, keys int) (*mixedLoad, *conn) {
	t.Helper()
	c := &conn{addr: "h:1", streams: make(map[uint32]*stream), nextID: 1, sendWin: 1 << 30, streamWin: 65535, maxStreams: math.MaxUint32}
	wk := newWorker(c, 1)
	ledger := &mixedLedger{expected: make(map[int64]mixedEvent), observed: make(map[int64]mixedEvent)}
	w, err := newMixedLoad(wk, config{keys: keys, conns: 1, streams: 16, mix: defaultMix}, 0, newRevisionSet(), ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range w.live {
		k.value = []byte("random bytes")
		k.create = 2
		k.mod = 2
		k.version = 1
	}
	return w, c
}

func mixedResponse(rev int64, deleted bool) []byte {
	header := newMsg()
	header.varint(headerRevision, uint64(rev))
	m := newMsg()
	m.sub(respHeader, header)
	m.varint(txnSucceeded, 1)
	op := newMsg()
	inner := newMsg()
	if deleted {
		inner.varint(delDeleted, 1)
		op.sub(3, inner)
	} else {
		op.sub(opResponsePut, inner)
	}
	m.sub(txnResponses, op)
	return m.b
}

func TestMixedTransitions(t *testing.T) {
	w, c := testMixed(t, 1)
	k := w.live[0]
	for i, op := range []int{2, 4, 3, 2} {
		w.pending = &mixedCall{op: op, key: k, value: []byte("next random value")}
		st := c.newStream(w)
		st.msgs = 1
		if !w.start(c, st) {
			t.Fatal("call could not start")
		}
		if err := w.message(st, mixedResponse(int64(i+3), op == 4)); err != nil {
			t.Fatal(err)
		}
		w.done(st, 10, nil)
		delete(c.streams, st.id)
		if op == 4 {
			if len(w.live) != 0 || k.mod != 0 || len(w.absent) != 1 {
				t.Fatal("delete did not make key absent")
			}
			w.absent = nil
		} else if len(w.live) != 1 || k.mod != int64(i+3) {
			t.Fatal("create/update did not restore tracked key")
		}
		if op == 3 && (k.create != int64(i+3) || k.version != 1) {
			t.Fatal("recreate did not reset create revision and version")
		}
	}
	if len(w.wk.stats.errs) != 0 || w.wk.stats.acked != 4 {
		t.Fatal("valid transitions counted as failures")
	}
}

func TestMixedLocks(t *testing.T) {
	w, c := testMixed(t, 2)
	k := w.live[0]
	group := &w.groups[k.group]
	update := c.newStream(w)
	w.pending = &mixedCall{op: 2, key: k, value: []byte("new")}
	if !w.start(c, update) {
		t.Fatal("update did not start")
	}
	w.pending = &mixedCall{op: 2, key: k, value: []byte("again")}
	if w.start(c, c.newStream(w)) {
		t.Fatal("key in flight twice")
	}
	w.pending = &mixedCall{op: 1, key: k, group: group}
	if w.start(c, c.newStream(w)) {
		t.Fatal("LIST raced a namespace mutation")
	}
	update.msgs = 1
	w.done(update, 10, nil)
	delete(c.streams, update.id)
	list := c.newStream(w)
	if !w.start(c, list) {
		t.Fatal("LIST did not start after mutation drained")
	}
	w.pending = &mixedCall{op: 2, key: k, value: []byte("again")}
	if w.start(c, c.newStream(w)) {
		t.Fatal("mutation raced LIST")
	}
	list.msgs = 1
	w.done(list, 10, nil)
	delete(c.streams, list.id)
	if !w.start(c, c.newStream(w)) {
		t.Fatal("LIST did not release namespace")
	}
}

func TestMixedRequestShapes(t *testing.T) {
	k := &mixedKey{key: []byte("/registry/pods/ns/name"), mod: 17}
	for _, op := range []int{2, 3, 4} {
		m := mixedTxn(k, op, []byte("value"))
		cmp, err := bytesField(m.b, txnCompare)
		if err != nil {
			t.Fatal(err)
		}
		target, err := varintField(cmp, cmpTarget)
		if err != nil {
			t.Fatal(err)
		}
		field := protowire.Number(cmpModRevision)
		want := uint64(17)
		wantTarget := uint64(cmpTargetMod)
		if op == 3 {
			field = 5
			want = 0
			wantTarget = 1
		}
		rev, err := varintField(cmp, field)
		if err != nil {
			t.Fatal(err)
		}
		key, err := bytesField(cmp, cmpKey)
		if err != nil {
			t.Fatal(err)
		}
		if target != wantTarget || rev != want || !bytes.Equal(key, k.key) {
			t.Fatal("incorrect Kubernetes compare")
		}
		branch, err := bytesField(m.b, txnSuccess)
		if err != nil {
			t.Fatal(err)
		}
		operation := protowire.Number(opPut)
		if op == 4 {
			operation = 3
		}
		inner, err := bytesField(branch, operation)
		if err != nil || inner == nil {
			t.Fatal("wrong success operation")
		}
	}
}

func TestMixedListPage(t *testing.T) {
	var keys []*mixedKey
	m := newMsg()
	for i := range 501 {
		k := &mixedKey{key: mixedName("pods", 0, 0, i), value: []byte("value"), create: 2, mod: 2, version: 1}
		keys = append(keys, k)
		if i < 500 {
			m.sub(rangeKVs, mixedKV(k))
		}
	}
	m.varint(rangeCount, 501)
	m.varint(rangeMore, 1)
	if err := checkMixedRange(m.b, keys, 500); err != nil {
		t.Fatal(err)
	}
	keys[0], keys[1] = keys[1], keys[0]
	if err := checkMixedRange(m.b, keys, 500); err == nil {
		t.Fatal("accepted out-of-order LIST")
	}
}

func TestMixedPreloadBoundsAndFrames(t *testing.T) {
	w, c := testMixed(t, 300)
	p := &mixedPreload{w: w}
	st := c.newStream(p)
	if !p.start(c, st) {
		t.Fatal("preload did not start")
	}
	if n := len(w.calls[st.id].batch); n != 128 {
		t.Fatalf("small values got %d puts, want 128", n)
	}
	// A large value spans DATA frames, without losing bytes or END_STREAM.
	m := newMsg()
	m.bytes(putKey, w.live[0].key)
	m.bytes(putValue, bytes.Repeat([]byte{7}, 33365))
	tpl, err := newTemplate(c.addr, "/etcdserverpb.KV/Put", m, true)
	if err != nil {
		t.Fatal(err)
	}
	c.out = nil
	large := c.newStream(w)
	c.startStream(large, tpl)
	fr := http2.NewFramer(nil, bytes.NewReader(c.out))
	var payload []byte
	ended := false
	frames := 0
	for range len(tpl.frames) + 1 {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if f.Header().StreamID != large.id {
			t.Fatal("unpatched stream ID")
		}
		if data, ok := f.(*http2.DataFrame); ok {
			frames++
			payload = append(payload, data.Data()...)
			ended = data.StreamEnded()
		}
	}
	if frames != 3 || !ended || !bytes.Equal(payload[grpcPrefixLen:], m.b) {
		t.Fatal("multipart framing changed request")
	}
	// Large values force a byte cap before the operation cap.
	for _, k := range w.live {
		k.value = bytes.Repeat([]byte{8}, 33365)
	}
	c.out = nil
	p.next = 0
	st = c.newStream(p)
	if !p.start(c, st) {
		t.Fatal("large preload did not start")
	}
	if n := len(w.calls[st.id].batch); n != 1 {
		t.Fatalf("large preload got %d puts", n)
	}
}

func TestMixedRejectsStaleAndFailedTxn(t *testing.T) {
	w, _ := testMixed(t, 1)
	k := w.live[0]
	st := &stream{id: 1}
	w.calls[1] = mixedCall{op: 2, key: k, value: []byte("new"), floor: 5}
	if err := w.message(st, mixedResponse(5, false)); err == nil {
		t.Fatal("write accepted a stale cluster revision")
	}
	failed := newMsg()
	header := newMsg()
	header.varint(headerRevision, 6)
	failed.sub(respHeader, header)
	if err := w.message(st, failed.b); err == nil {
		t.Fatal("failed compare counted as success")
	}
	if k.mod != 2 || w.wk.stats.acked != 0 {
		t.Fatal("failed response changed model")
	}
}
