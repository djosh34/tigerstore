package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

func TestDirectResidentWatchPreviousValueColdMidMessageKeepsExactWire(t *testing.T) {
	c, _, st, log, l := fillRangeFixture(t)
	n := c.node
	n.connOut = 64 << 10
	l.epfd = -1
	var expected []*mvccpb.Event
	for i := range 3 {
		key := fmt.Sprintf("/watch/%d", i)
		value := bytes.Repeat([]byte{byte('a' + i)}, 900<<10)
		putValue(t, st, log, key, string(value))
		expected = append(expected, &mvccpb.Event{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte(key), ModRevision: 5}, PrevKv: &mvccpb.KeyValue{Key: []byte(key), Value: value, CreateRevision: int64(2 + i), ModRevision: int64(2 + i), Version: 1}})
	}
	st.rev++
	var live []event
	for i := range 3 {
		id, _ := st.find([]byte(fmt.Sprintf("/watch/%d", i)))
		prev, ok, err := st.del(id, st.rev, true)
		if err != nil || !ok {
			t.Fatalf("delete: %v %v", ok, err)
		}
		next := int32(i + 1)
		if i == 2 {
			next = -1
		}
		live = append(live, event{key: id, rev: st.rev, del: true, prev: prev, next: next})
	}
	// Main's apply event still owns these old slab records in this turn.
	s, wi := establishFillWatch(t, n, c, "/watch/", 5)
	x := &n.w.ws[wi]
	x.end = []byte("/watch0")
	wantBody, err := proto.Marshal(&pb.WatchResponse{Header: &pb.ResponseHeader{ClusterId: n.clusterID, MemberId: n.id, Revision: 5, RaftTerm: n.term}, WatchId: x.id, Events: expected})
	if err != nil {
		t.Fatal(err)
	}
	want := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(wantBody)))
	want = append(want, wantBody...)
	if err := n.syncWatchers(); err != nil {
		t.Fatal(err)
	}
	if len(n.bigWatches) != 1 {
		t.Fatal("delete's previous values did not select large watch")
	}
	fd := roomSocket(t, c)
	c.flow.conn = 1024
	s.win = 1024
	b := n.bigWatches[0]
	b.liveEvents = live
	b.liveFirst = 0
	n.pumpBigWatches()
	b.liveEvents = nil
	b.liveFirst = 0 // exactly flushEvents' same-turn borrow lifetime
	if n.fillBook != nil || len(n.fills) != 0 || b.resume.taken != -1 {
		t.Fatal("resident previous-value prefix built a source plan")
	}
	var wire []byte
	l.flush()
	readFillSocket(t, fd, &wire)
	if len(fillData(t, wire)) != 1024 {
		t.Fatal("did not publish a partial resident previous value")
	}
	st.endApply() // now the unsent previous values genuinely become cold
	putValue(t, st, log, "/watch/0", "resurrected")
	c.flow.conn = 1 << 30
	s.win = 1 << 30
	wire = append(wire, driveLargeFills(t, n, l, c, fd, nil)...)
	data := fillData(t, wire)
	got := decodeWatchMessages(t, data)
	if len(got) != 1 || len(got[0].Events) != 3 || !bytes.Equal(data, want) {
		t.Fatal("cold previous-value continuation replayed/discarded bytes or changed the all-resident decoded response")
	}
	if !proto.Equal(got[0], &pb.WatchResponse{Header: &pb.ResponseHeader{ClusterId: n.clusterID, MemberId: n.id, Revision: 5, RaftTerm: n.term}, WatchId: x.id, Events: expected}) {
		t.Fatal("decoded watch differs from its original snapshot")
	}
	if n.stageCredits.used != 0 || st.heldReads() != 0 || x.syncRev != 6 {
		t.Fatal("watch continuation leaked P/history or failed to advance once")
	}
}
