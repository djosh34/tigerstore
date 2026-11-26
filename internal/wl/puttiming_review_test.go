package wl

import (
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

type putTimingShell struct {
	shell
	logger *log.Logger
}

func (s *putTimingShell) logf(format string, args ...any) { s.logger.Printf(format, args...) }

func TestPutTimingDoesNotChangeDefaultBuffers(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("the baseline is the production 64-bit stream layout")
	}
	const mainSlotBytes = 229376
	if slotBytes != mainSlotBytes {
		t.Errorf("stream slots take %d bytes, main takes %d", slotBytes, mainSlotBytes)
	}
	n := &node{}
	c := &client{node: n}
	if got, want := c.bufNeed(), int64(clientInBuf+clientPtBuf+mainSlotBytes); got != want {
		t.Errorf("default bufNeed = %d, main = %d", got, want)
	}
	c.takeBufs()
	if got := len(c.slots) * int(unsafe.Sizeof(stream{})); got != mainSlotBytes {
		t.Errorf("actual slot allocation = %d, main = %d", got, mainSlotBytes)
	}
}

func TestPutTimingStartsBeforeParkingDATA(t *testing.T) {
	c, s := flowClient(t)
	n := c.node
	var out putTimingLog
	n.shell = &putTimingShell{logger: log.New(&out, "", 0)}
	n.putTimings = &putTimings{pending: map[uint64][]*putTiming{}}
	n.sizes = sizeFor(1 << 30)
	n.connIn, n.connOut = 1<<25, 1<<25
	n.cpool = n.sizes.clientInBufs()
	n.now = time.Now()
	s.kind, c.preface, c.lastSid = kPut, true, s.id
	// Only the gRPC length is here. The full client pool parks this
	// frame before data() takes it, and its timeout later refuses it.
	frame := make([]byte, 14)
	putFrameHeader(frame, 5, frData, 0, s.id)
	binary.BigEndian.PutUint32(frame[10:], putTimingBytes)
	copy(c.pt, frame)
	c.pw = len(frame)
	if err := c.frames(n); err != nil {
		t.Fatal(err)
	}
	if c.roomNeed == 0 || c.pr != 0 {
		t.Fatal("the DATA frame was not parked")
	}
	groups := timingGroups(t, out.text())
	if len(groups) != 1 {
		t.Fatalf("parked known-size put has no request ID: %s", out.text())
	}
	for _, events := range groups {
		if len(events) != 2 || events[0]["stage"] != "receive" || events[1]["stage"] != "receive_wait" {
			t.Fatalf("wait not visible before parking: %v", events)
		}
	}
	// Advance the diagnostic clock without a real seven-second sleep.
	n.putTimingOf(s).began = n.putTimingOf(s).began.Add(-requestTimeout)
	n.now = n.now.Add(requestTimeout)
	l := &loop{n: n}
	l.serveRoom(c)
	groups = timingGroups(t, out.text())
	if len(groups) != 1 {
		t.Fatal("refusal created another request ID")
	}
	for _, events := range groups {
		last := events[len(events)-1]
		elapsed, err := strconv.ParseInt(last["elapsed_ns"], 10, 64)
		if err != nil || elapsed < int64(requestTimeout) || last["stage"] != "reply_queued" || last["code"] != "8" {
			t.Fatalf("parked put's wait/refusal disappeared: %v", events)
		}
	}
}

func timingBatchFixture(t *testing.T) (*node, *client, *stream, []byte) {
	t.Helper()
	c, s := flowClient(t)
	n := c.node
	s.kind = kPut
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.putTimings = &putTimings{pending: map[uint64][]*putTiming{}}
	n.rn = newTestRawNode(t) // follower, no leader: Propose is dropped
	req, err := proto.Marshal(&pb.PutRequest{Key: []byte("/timed"), Value: make([]byte, putTimingBytes)})
	if err != nil {
		t.Fatal(err)
	}
	n.tracePutStart(s, len(req))
	return n, c, s, req
}

func TestPutTimingDefinitiveRejectionsReleaseMetadata(t *testing.T) {
	for _, exit := range []string{"no-leader", "follower-refusal"} {
		t.Run(exit, func(t *testing.T) {
			n, c, s, req := timingBatchFixture(t)
			n.addOp(mkHandle(c, s.id), kPut, req)
			if len(n.putTimings.pending) != 1 {
				t.Fatal("request was not tracked in its batch")
			}
			if exit == "no-leader" {
				n.proposeBatch(time.Now())
				n.now = time.Now().Add(requestTimeout)
				n.expireProposals()
			} else {
				seq := n.fwdSeq
				n.fwd[seq&(fwdSlots-1)] = fwdBatch{seq: seq, handles: n.batchOwn}
				f := make([]byte, refusedFrameLn)
				f[0] = frameRefused
				binary.LittleEndian.PutUint64(f[1:], seq)
				n.refused(f)
			}
			if s.open || len(n.putTimings.pending) != 0 || len(n.putTimings.active) != 0 {
				t.Fatalf("definitive rejection kept metadata: open=%v pending=%d active=%d", s.open, len(n.putTimings.pending), len(n.putTimings.active))
			}
		})
	}
}

func TestPutTimingAmbiguousRejectionAndSlotReuse(t *testing.T) {
	n, c, s, req := timingBatchFixture(t)
	n.lead = 2 // Propose drops, but its response is ambiguous leader-changed
	n.fwdSeq = fwdSlots
	old := &putTiming{id: 99, began: time.Now()}
	n.fwd[1].seq = 1
	n.putTimings.pending[1] = []*putTiming{old}
	n.addOp(mkHandle(c, s.id), kPut, req)
	seq := n.fwdSeq
	n.proposeBatch(time.Now())
	if _, ok := n.putTimings.pending[1]; ok {
		t.Fatal("real forward-slot reuse did not discard the old timing")
	}
	if s.open || len(n.putTimings.pending[seq]) != 1 {
		t.Fatal("leader-changed reply lost the timing needed for a late apply")
	}
}

func TestPutTimingApplyBatchReleasesCancelledMetadata(t *testing.T) {
	n, c, s, req := timingBatchFixture(t)
	st, l := newTestStore(t)
	defer st.close() //nolint:errcheck // a test's store
	n.st = st
	n.addOp(mkHandle(c, s.id), kPut, req)
	data, seq := n.batch, n.fwdSeq
	binary.LittleEndian.PutUint32(data[12:], uint32(n.batchOps))
	n.fwd[seq&(fwdSlots-1)] = fwdBatch{seq: seq, handles: n.batchOwn}
	c.endStream(s) // cancel before apply; no client remains to hold metadata
	if err := n.applyBatch(data, 8, l.add(string(data))); err != nil {
		t.Fatal(err)
	}
	if len(n.putTimings.pending) != 0 || len(n.putTimings.active) != 0 || len(n.fwd[seq&(fwdSlots-1)].handles) != 0 {
		t.Fatal("applyBatch's real deferred cleanup kept diagnostics or handles")
	}
	if _, ok := st.find([]byte("/timed")); !ok {
		t.Fatal("the cancelled request did not actually apply")
	}
}

func TestPutTimingOnReceivingFollower(t *testing.T) {
	var out putTimingLog
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			c.TraceLargePuts = true
			c.Logger = log.New(&out, "", 0)
		}
	})
	all := tc.client(t)
	lead := tc.leaderEndpoint(t, all)
	for i, ep := range tc.endpoints {
		if ep == lead {
			continue
		}
		c := tc.clientOf(t, ep)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		if _, err := c.Put(ctx, "/follower", strings.Repeat("v", putTimingBytes)); err != nil {
			t.Fatal(err)
		}
		groups := timingGroups(t, out.text())
		if len(groups) != 1 {
			t.Fatalf("want one receiving-follower timeline, got %d", len(groups))
		}
		for id, events := range groups {
			if !strings.HasPrefix(id, strconv.FormatUint(tc.ids[i], 10)+"/") || len(events) != 9 {
				t.Fatalf("wrong receiver or missing stages: %s %v", id, events)
			}
		}
		return
	}
	t.Fatal("no follower available")
}
