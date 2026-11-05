package floor

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/net/http2"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"testing"
)

var sink []byte

func BenchmarkFloorGCM(b *testing.B) {
	block, _ := aes.NewCipher(make([]byte, 16))
	a, _ := cipher.NewGCM(block)
	nonce := make([]byte, 12)
	aad := []byte{23, 3, 3, 0, 0}
	for _, n := range []int{80, 1100, 1220, 16384} {
		p := make([]byte, n)
		ct := a.Seal(nil, nonce, p, aad)
		out := make([]byte, 0, n+32)
		for _, open := range []bool{false, true} {
			name := "seal"
			if open {
				name = "open"
			}
			b.Run(fmt.Sprintf("%s/n%d", name, n), func(b *testing.B) {
				nonce := make([]byte, 12)
				var seq uint64
				for b.Loop() {
					if open {
						var err error
						out, err = a.Open(out[:0], nonce, ct, aad)
						if err != nil {
							b.Fatal(err)
						}
					} else {
						seq++
						binary.BigEndian.PutUint64(nonce[4:], seq)
						out = a.Seal(out[:0], nonce, p, aad)
					}
				}
				sink = out
			})
		}
	}
}
func BenchmarkFloorH2(b *testing.B) {
	var wire bytes.Buffer
	f := http2.NewFramer(&wire, nil)
	_ = f.WriteData(1, true, make([]byte, 1100))
	data := bytes.Clone(wire.Bytes())
	r := bytes.NewReader(data)
	fr := http2.NewFramer(io.Discard, r)
	for b.Loop() {
		r.Reset(data)
		if _, err := fr.ReadFrame(); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkFloorPeer(b *testing.B) {
	for _, txn := range []bool{false, true} {
		for _, batch := range []int{64, 256, 1536} {
			name := "put"
			if txn {
				name = "txn"
			}
			data := Batch(txn, batch)
			m := &raftpb.Message{Type: raftpb.MsgApp.Enum(), To: new(uint64(0x223456789abcdef0)), From: new(uint64(0x123456789abcdef0)), Term: new(uint64(2)), LogTerm: new(uint64(2)), Index: new(uint64(100000)), Commit: new(uint64(99999)), Entries: []*raftpb.Entry{{Term: new(uint64(2)), Index: new(uint64(100001)), Type: raftpb.EntryNormal.Enum(), Data: data}}}
			wire, _ := proto.Marshal(m)
			out := make([]byte, 0, len(wire)+4)
			b.Run(fmt.Sprintf("%s/b%d", name, batch), func(b *testing.B) {
				for b.Loop() {
					_ = proto.Size(m)
					var err error
					out, err = proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(out[:0], m)
					if err != nil {
						b.Fatal(err)
					}
				}
				sink = out
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/item")
			})
		}
	}
	ack := &raftpb.Message{Type: raftpb.MsgAppResp.Enum(), To: new(uint64(0x123456789abcdef0)), From: new(uint64(0x223456789abcdef0)), Term: new(uint64(2)), Index: new(uint64(100001))}
	wire, _ := proto.Marshal(ack)
	b.Run("ack_decode", func(b *testing.B) {
		for b.Loop() {
			var m raftpb.Message
			if err := proto.Unmarshal(wire, &m); err != nil {
				b.Fatal(err)
			}
		}
	})
}
func BenchmarkFloorSyscall(b *testing.B) {
	// /dev/null is syscall-only, not a filesystem or durability measurement.
	f, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 256*1120)
	for b.Loop() {
		if _, err := unix.Write(int(f.Fd()), buf); err != nil {
			b.Fatal(err)
		}
	}
}
func TestFloorSizes(t *testing.T) {
	for _, txn := range []bool{false, true} {
		for _, batch := range []int{64, 256, 1536} {
			data := Batch(txn, batch)
			t.Logf("txn=%v key=%d request=%d batch=%d entry=%d WAL=%d", txn, len(Key(0)), len(Wire(txn)), batch, len(data), len(data)+26)
		}
	}
}
