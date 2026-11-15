package peer

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// wireMessages are messages of every shape the codec writes itself, and
// some it leaves to protobuf.
func wireMessages() []*raftpb.Message {
	et := raftpb.EntryType_EntryConfChangeV2
	return []*raftpb.Message{
		{},
		{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Commit: new(uint64(9)), Context: []byte{}},
		{Type: raftpb.MessageType_MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Index: new(uint64(1 << 40)), Reject: new(false), RejectHint: new(uint64(4)), LogTerm: new(uint64(2))},
		{Type: raftpb.MessageType_MsgVoteResp.Enum(), Reject: new(true), Vote: new(uint64(3))},
		{
			Type: raftpb.MessageType_MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Index: new(uint64(7)), LogTerm: new(uint64(3)), Commit: new(uint64(6)),
			Entries: []*raftpb.Entry{
				{Term: new(uint64(3)), Index: new(uint64(8)), Data: []byte("data")},
				{Term: new(uint64(3)), Index: new(uint64(9))},
				{Term: new(uint64(3)), Index: new(uint64(10)), Data: []byte{}},
				{Type: &et, Term: new(uint64(3)), Index: new(uint64(11)), Data: bytes.Repeat([]byte("x"), 300)},
			},
			Context: []byte("held"),
		},
		{Type: raftpb.MessageType_MsgSnap.Enum(), Snapshot: &raftpb.Snapshot{Data: []byte("s"), Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(5))}}},
		{Type: raftpb.MessageType_MsgStorageAppend.Enum(), Responses: []*raftpb.Message{{Type: raftpb.MessageType_MsgAppResp.Enum()}}},
	}
}

// TestWireIsProtobuf checks that the codec writes what protobuf's Marshal
// writes, and reads back what protobuf's Unmarshal reads.
func TestWireIsProtobuf(t *testing.T) {
	for i, m := range wireMessages() {
		want, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := appendMessage([]byte("x"), m)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[1:], want) {
			t.Fatalf("message %d:\n got %x\nwant %x", i, got[1:], want)
		}
		checkDecode(t, want)
	}
	// An enum value raftpb does not name and a field it does not have
	// go through protobuf, which keeps them.
	odd := protowire.AppendVarint(protowire.AppendTag(nil, fType, protowire.VarintType), 1000)
	odd = protowire.AppendVarint(protowire.AppendTag(odd, 99, protowire.VarintType), 1)
	checkDecode(t, odd)
	m, err := decodeMessage(odd)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := appendMessage(nil, m); err != nil || !bytes.Equal(got, odd) {
		t.Fatalf("written again: %x %v, want %x", got, err, odd)
	}
}

// checkDecode checks that decodeMessage reads b as protobuf does.
func checkDecode(t *testing.T, b []byte) {
	t.Helper()
	want := &raftpb.Message{}
	werr := proto.Unmarshal(b, want)
	got, err := decodeMessage(b)
	if (err != nil) != (werr != nil) {
		t.Fatalf("decode %x: error %v, protobuf's %v", b, err, werr)
	}
	if err == nil && !proto.Equal(got, want) {
		t.Fatalf("decode %x:\n got %v\nwant %v", b, got, want)
	}
}

// FuzzDecodeMessage checks that the codec reads any bytes as protobuf
// does, and writes what it read as protobuf would.
func FuzzDecodeMessage(f *testing.F) {
	for _, m := range wireMessages() {
		b, err := proto.Marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkDecode(t, b)
		m, err := decodeMessage(b)
		if err != nil {
			return
		}
		want, err := proto.Marshal(m)
		if err != nil {
			return
		}
		got, err := appendMessage(nil, m)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("written again: %x %v, want %x", got, err, want)
		}
	})
}
