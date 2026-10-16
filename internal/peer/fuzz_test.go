package peer

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// FuzzFrames reads any bytes as the frames of an inbound connection from
// peer 2. The reader must stop with an error at bad input, never crash,
// and deliver only messages from peer 2.
func FuzzFrames(f *testing.F) {
	var seed []byte
	for _, m := range []*raftpb.Message{
		{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3))},
		{
			Type: raftpb.MessageType_MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Index: new(uint64(7)),
			Entries: []*raftpb.Entry{{Term: new(uint64(3)), Index: new(uint64(8)), Data: []byte("data")}},
		},
	} {
		b, err := proto.Marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		seed = binary.BigEndian.AppendUint32(seed, uint32(len(b)))
		seed = append(seed, b...)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		tr := &TCP{cfg: Config{Deliver: func(m *raftpb.Message) {
			if m.GetFrom() != 2 {
				t.Fatalf("delivered a message from %d", m.GetFrom())
			}
		}}}
		cs := &counters{received: prometheus.NewCounter(prometheus.CounterOpts{Name: "received"})}
		if err := tr.frames(bytes.NewReader(b), 2, cs); err == nil {
			t.Fatal("frames returned without an error")
		}
	})
}
