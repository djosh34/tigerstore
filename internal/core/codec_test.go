package core

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestBatchRoundTrip(t *testing.T) {
	wire, err := proto.Marshal(&pb.PutRequest{Key: []byte("w"), Value: []byte("2"), Lease: 5, PrevKv: true})
	if err != nil {
		t.Fatal(err)
	}
	put, err := ParsePut(wire)
	if err != nil {
		t.Fatal(err)
	}
	ops := []*pending{
		{req: Request{ID: 7, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}}},
		{req: Request{ID: 10, Put: put}},
		{req: Request{ID: 8, Op: &pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte("c")}}},
		{req: Request{ID: 9, Op: &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a")}}}}},
	}
	size := map[uint32]int{}
	for format := uint32(1); format <= MaxVersion; format++ {
		data, err := encodeBatch(format, 2, 1<<40, ops, 0)
		if err != nil {
			t.Fatal(err)
		}
		if (data[0] == 0) != (format > 1) {
			t.Fatalf("format %d starts with %d", format, data[0])
		}
		decodeAll(t, data, ops)
		size[format] = len(data)
	}
	// From originVersion, the origin and boot come once per entry.
	once := protowire.SizeVarint(2) + protowire.SizeVarint(1<<40)
	if got, want := size[originVersion], size[originVersion-1]-(len(ops)-1)*once; got != want {
		t.Errorf("format %d took %d bytes, want %d", originVersion, got, want)
	}
	if err := DecodeBatch([]byte{0, MaxVersion + 1, 0}, func(_, _, _ uint64, _ proto.Message) error { return nil }); err == nil {
		t.Fatal("an unknown format decoded")
	}
	if _, err := encodeBatch(1, 1, 1, []*pending{{req: Request{Op: &pb.RangeRequest{}}}}, 0); err == nil {
		t.Fatal("a range was encoded as a write")
	}
}

// decodeAll decodes a batch of ops and checks every write and every cut.
func decodeAll(t *testing.T, data []byte, ops []*pending) {
	t.Helper()
	i := 0
	err := DecodeBatch(data, func(origin, boot, id uint64, op proto.Message) error {
		want := ops[i].req.Op
		if want == nil {
			want = &pb.PutRequest{Key: []byte("w"), Value: []byte("2"), Lease: 5, PrevKv: true}
		}
		if origin != 2 || boot != 1<<40 || id != ops[i].req.ID || !proto.Equal(op, want) {
			t.Errorf("write %d decoded as %d/%d/%d %v", i, origin, boot, id, op)
		}
		i++
		return nil
	})
	if err != nil || i != len(ops) {
		t.Fatalf("decoded %d writes, err %v", i, err)
	}
	for cut := range len(data) {
		if err := DecodeBatch(data[:cut], func(_, _, _ uint64, _ proto.Message) error { return nil }); err == nil {
			t.Fatalf("batch cut at %d of %d bytes decoded without error", cut, len(data))
		}
	}
}

// TestTxnValuesPlaced decodes a transaction that starts inside an entry,
// with puts in both branches, nested, and repeated, and requires the place
// of every put's value to be where its bytes are in the entry.
func TestTxnValuesPlaced(t *testing.T) {
	put := func(v string) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k" + v), Value: []byte(v)}}}
	}
	nested := &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: []*pb.RequestOp{put("inner")}, Failure: []*pb.RequestOp{put("other")}}}}
	b, err := proto.Marshal(&pb.TxnRequest{Success: []*pb.RequestOp{put("first"), nested}, Failure: []*pb.RequestOp{put("failure")}})
	if err != nil {
		t.Fatal(err)
	}
	// A put whose value comes twice: the last one wins.
	twice := append([]byte{0x12, 4, 'o', 'n', 'c', 'e', 0x12, 5}, "twice"...)
	op := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), twice)
	b = protowire.AppendBytes(protowire.AppendTag(b, 2, protowire.BytesType), op)
	entry := append([]byte("prefix"), b...)
	var m pb.TxnRequest
	var pl placer
	if err := decodeTxnPlaced(entry[6:], &m, 6, &pl); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	var walk func(ops []*pb.RequestOp)
	walk = func(ops []*pb.RequestOp) {
		for _, op := range ops {
			if p := op.GetRequestPut(); p != nil {
				got = append(got, p.Value)
			}
			if sub := op.GetRequestTxn(); sub != nil {
				walk(sub.Success)
				walk(sub.Failure)
			}
		}
	}
	walk(m.Success)
	walk(m.Failure)
	if len(got) != 5 || string(got[3]) != "twice" {
		t.Fatalf("decoded values %q", got)
	}
	vals := &values{placer: pl}
	for _, v := range got {
		r, err := vals.Place(v)
		if err != nil {
			t.Fatalf("value %q: %v", v, err)
		}
		if &entry[r.Off] != &v[0] || int(r.Len) != len(v) {
			t.Fatalf("value %q placed at %d+%d", v, r.Off, r.Len)
		}
	}
	if _, err := vals.Place([]byte("elsewhere")); err == nil {
		t.Fatal("a value from no entry was placed")
	}
}

// TestParsePutMatchesProtobuf compares ParsePut with protobuf's decoder on
// encodings with unknown, repeated and mistyped fields, and on every
// truncation of them.
func TestParsePutMatchesProtobuf(t *testing.T) {
	base, err := proto.Marshal(&pb.PutRequest{Key: []byte("k"), Value: []byte("value"), Lease: -3, PrevKv: true, IgnoreValue: true, IgnoreLease: true})
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		base,
		{},
		append(append([]byte{}, base...), 0x0a, 1, 'z'),                                                     // key again: the last wins
		append(append([]byte{}, base...), 0x20, 0),                                                          // prev_kv false again
		append(append([]byte{}, base...), 0x3a, 2, 'u', 'v'),                                                // unknown field 7
		append(append([]byte{}, base...), 0x1a, 1, 'x'),                                                     // lease with the wrong wire type
		append(append([]byte{}, base...), 0x0d, 1, 2, 3, 4),                                                 // key with the wrong wire type
		append(append([]byte{}, base...), 0x98, 0x06, 0x01),                                                 // unknown varint field 99
		append(append([]byte{}, base...), 0x82, 0xb0, 0x00, 0x00),                                           // unknown field 768, tag in more bytes than needed
		append(append([]byte{}, base...), 0x18, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01), // lease -1
		{0x0a, 0x01, 'k', 0x80, 0x80, 0x80, 0x80, 0x10, 0x01},                                               // field 2^29, above the largest
	}
	for i, b := range cases {
		for cut := 0; cut <= len(b); cut++ {
			in := b[:cut]
			var want pb.PutRequest
			wantErr := proto.Unmarshal(in, &want)
			got, err := ParsePut(in)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("case %d cut %d: ParsePut error %v, protobuf error %v", i, cut, err, wantErr)
			}
			if err != nil {
				continue
			}
			if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Lease != want.Lease ||
				got.PrevKv != want.PrevKv || got.IgnoreValue != want.IgnoreValue || got.IgnoreLease != want.IgnoreLease {
				t.Fatalf("case %d cut %d: got %+v, want %v", i, cut, got.PutOp, &want)
			}
			if len(got.Value) > 0 && &in[got.valueAt] != &got.Value[0] {
				t.Fatalf("case %d cut %d: the value is not at %d", i, cut, got.valueAt)
			}
		}
	}
}
