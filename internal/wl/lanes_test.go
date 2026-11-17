package wl

import (
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Heartbeats go with the appends unless an append to the peer went
// unanswered for a tick (#715); their answers go back on the connection
// they came on; votes always go on the connection of their own. The
// ReadIndex rounds are heartbeats, and a connection of their own cost a
// write and a read each per follower (#689, "0 ms: the bisect").
func TestHeartbeatsTakeTheirOwnConnectionOnlyBehindASlowAppend(t *testing.T) {
	n, _ := testNode(t)
	n.now = time.Now()
	n.peersOut = map[uint64]*peerOut{}
	p := &peerOut{peer: 2}
	n.peersOut[2] = p
	msg := func(typ pb.MessageType) *pb.Message { return &pb.Message{Type: typ.Enum(), To: new(uint64(2))} }
	lane := func(m *pb.Message) string {
		if n.onBeats(m, p) {
			return "its own"
		}
		return "the appends'"
	}
	check := func(what string, m *pb.Message, want string) {
		t.Helper()
		if got := lane(m); got != want {
			t.Fatalf("%s goes on %s connection, want %s", what, got, want)
		}
	}
	check("a heartbeat with no append waiting", msg(pb.MsgHeartbeat), "the appends'")
	for _, typ := range []pb.MessageType{pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgVote, pb.MsgVoteResp} {
		check(typ.String(), msg(typ), "its own")
	}
	check("an append", msg(pb.MsgApp), "the appends'")

	// sendPeer notes when an append went out unanswered.
	p.appSince = n.now.Add(-tickEvery / 2)
	check("a heartbeat behind an append half a tick old", msg(pb.MsgHeartbeat), "the appends'")
	p.appSince = n.now.Add(-tickEvery)
	check("a heartbeat behind an append a tick old", msg(pb.MsgHeartbeat), "its own")
	n.noteLanes(&peerIn{from: 2}, &pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2))})
	if !p.appSince.IsZero() {
		t.Fatal("the peer's answer to its appends left them waiting")
	}
	check("a heartbeat once the appends were answered", msg(pb.MsgHeartbeat), "the appends'")

	check("the answer to a heartbeat that came with the appends", msg(pb.MsgHeartbeatResp), "the appends'")
	n.noteLanes(&peerIn{from: 2, beat: true}, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2))})
	check("the answer to a heartbeat that came on its own connection", msg(pb.MsgHeartbeatResp), "its own")
	n.noteLanes(&peerIn{from: 2}, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2))})
	check("the answer to the next, with the appends", msg(pb.MsgHeartbeatResp), "the appends'")
}

// Behind a message held for a batch it names, a heartbeat is stepped at
// once, as on its own connection, and so is a batch; any other message
// keeps its order and waits. A follower whose disk had stalled held its
// leader's heartbeats behind such appends past an election timeout
// (TestWLStalledLeaderIsReplaced, seed 1, on 3489fdd1).
func TestAHeartbeatPassesMessagesHeldBeforeIt(t *testing.T) {
	frame := func(typ byte, n int) []byte {
		f := make([]byte, n)
		f[0] = typ
		return f
	}
	for _, c := range []struct {
		what  string
		f     []byte
		waits bool
	}{
		{"a heartbeat", frame(byte(pb.MsgHeartbeat), msgHdr+4), false},
		{"a batch", frame(frameBatch, batchFrameHdr+8), false},
		{"an append", frame(byte(pb.MsgApp), msgHdr+4), true},
		{"a heartbeat's answer", frame(byte(pb.MsgHeartbeatResp), msgHdr+4), true},
		{"an append's answer", frame(byte(pb.MsgAppResp), msgHdr+4), true},
		{"a request to carry batches", []byte{frameCarry}, true},
	} {
		if got := waitsBehindHeld(c.f); got != c.waits {
			t.Errorf("%s waits behind a held message: %v, want %v", c.what, got, c.waits)
		}
	}
}
