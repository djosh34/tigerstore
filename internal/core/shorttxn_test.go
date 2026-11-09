package core

import (
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// randomShortish returns a transaction near kube-apiserver's shape: often
// of it, sometimes one field off.
func randomShortish(r *rand.Rand) *pb.TxnRequest {
	key := []byte{'k', byte('a' + r.IntN(2))}
	other := []byte("o")
	pick := func(k []byte) []byte {
		if r.IntN(10) == 0 {
			return other
		}
		return k
	}
	c := &pb.Compare{Key: key, Result: pb.Compare_CompareResult(r.IntN(4)), Target: pb.Compare_CompareTarget(r.IntN(5))}
	switch r.IntN(7) {
	case 0:
	case 1:
		c.TargetUnion = &pb.Compare_Version{Version: r.Int64N(10)}
	case 2:
		c.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: r.Int64N(10)}
	case 3:
		c.TargetUnion = &pb.Compare_ModRevision{ModRevision: r.Int64N(1 << 40)}
	case 4:
		c.TargetUnion = &pb.Compare_Lease{Lease: -r.Int64N(10)}
	case 5:
		c.TargetUnion = &pb.Compare_Value{Value: []byte("v")}
	default:
		c.TargetUnion = &pb.Compare_ModRevision{ModRevision: 0}
	}
	if r.IntN(20) == 0 {
		c.RangeEnd = []byte("z")
	}
	t := &pb.TxnRequest{Compare: []*pb.Compare{c}}
	if r.IntN(2) == 0 {
		p := &pb.PutRequest{Key: pick(key), Lease: r.Int64N(3), PrevKv: r.IntN(4) == 0, IgnoreValue: r.IntN(8) == 0, IgnoreLease: r.IntN(8) == 0}
		if r.IntN(5) > 0 {
			p.Value = make([]byte, r.IntN(40))
			for i := range p.Value {
				p.Value[i] = byte(r.IntN(256))
			}
		}
		t.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: p}}}
	} else {
		d := &pb.DeleteRangeRequest{Key: pick(key), PrevKv: r.IntN(4) == 0}
		if r.IntN(20) == 0 {
			d.RangeEnd = []byte("z")
		}
		t.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: d}}}
	}
	switch r.IntN(6) {
	case 0:
	case 1:
		t.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: pick(key), Limit: 1}}}}
	case 2:
		t.Success = append(t.Success, t.Success[0])
	default:
		t.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: pick(key)}}}}
	}
	return t
}

// TestShortTxnRoundTrip checks that a transaction of kube-apiserver's
// shape decodes from its short form to the same request, with the value
// placed where it is, and that one of another shape has no short form.
func TestShortTxnRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	shorts := 0
	for range 20000 {
		want := randomShortish(r)
		// As a node gets it: decoded from the client's encoding.
		wire, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var took pb.TxnRequest
		if err := decodeTxnAlias(wire, &took); err != nil {
			t.Fatal(err)
		}
		if !isShort(&took) {
			continue
		}
		shorts++
		entry := appendShort([]byte("prefix"), &took)
		var got pb.TxnRequest
		var pl placer
		if err := decodeShort(entry[6:], &got, 6, &pl); err != nil {
			t.Fatalf("%v: %v", want, err)
		}
		if !proto.Equal(&got, want) {
			t.Fatalf("%v came back as %v", want, &got)
		}
		if put := got.Success[0].GetRequestPut(); put != nil && len(put.Value) > 0 {
			r, err := (&values{placer: pl}).Place(put.Value)
			if err != nil || &entry[r.Off] != &put.Value[0] {
				t.Fatalf("%v: value placed at %d, %v", want, r.Off, err)
			}
		}
		for cut := range len(entry) - 6 {
			// A short form cut anywhere, but at the end of a put's
			// value, does not decode.
			var m pb.TxnRequest
			if err := decodeShort(entry[6:6+cut], &m, 6, nil); err == nil && proto.Equal(&m, want) {
				t.Fatalf("%v decoded from %d of %d bytes", want, cut, len(entry)-6)
			}
		}
	}
	if shorts < 2000 {
		t.Fatalf("only %d transactions had the short shape", shorts)
	}
}

// TestShortTxnInBatch checks that a format-4 entry holds a transaction of
// kube-apiserver's shape in its short form, which the checks decode to the
// request, and that an older format keeps the client's encoding.
func TestShortTxnInBatch(t *testing.T) {
	update := &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("/registry/pods/a"), Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: 7}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("/registry/pods/a"), Value: []byte("pod")}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("/registry/pods/a")}}}},
	}
	wire := mustMarshal(t, update)
	req := Request{ID: 3, Op: &pb.TxnRequest{}, Wire: wire}
	if err := DecodeTxn(&req); err != nil {
		t.Fatal(err)
	}
	for format := uint32(originVersion - 1); format <= MaxVersion; format++ {
		data, err := encodeBatch(format, 2, 1<<40, []*pending{{req: req}}, 0)
		if err != nil {
			t.Fatal(err)
		}
		var kind byte
		var size int
		if err := walkBatch(data, func(_, _, _ uint64, k byte, b []byte, _ int) error {
			kind, size = k, len(b)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if short := format >= originVersion; short != (kind == kindShortTxn) || !short && size != len(wire) || short && size >= len(wire)-2*len("/registry/pods/a") {
			t.Fatalf("format %d: kind %d, %d bytes for %d in the client's encoding", format, kind, size, len(wire))
		}
		decodeAll(t, data, []*pending{{req: Request{ID: 3, Op: update}}})
	}
}

// TestShortSize checks shortSize against appendShort.
func TestShortSize(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 10))
	for range 5000 {
		txn := randomShortish(r)
		if isShort(txn) {
			if got, want := shortSize(txn), len(appendShort(nil, txn)); got != want {
				t.Fatalf("%v: size %d, short form %d bytes", txn, got, want)
			}
		}
	}
}

// TestDecodeTxnAllocs checks that kube-apiserver's update decodes with its
// messages in one block: the block and the places of its value.
func TestDecodeTxnAllocs(t *testing.T) {
	wire := mustMarshal(t, &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("/registry/pods/a"), Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: 7}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("/registry/pods/a"), Value: []byte("pod")}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("/registry/pods/a")}}}},
	})
	var txn pb.TxnRequest
	if n := testing.AllocsPerRun(100, func() {
		txn = pb.TxnRequest{}
		req := Request{Op: &txn, Wire: wire}
		if err := DecodeTxn(&req); err != nil {
			t.Fatal(err)
		}
	}); n > 2 {
		t.Fatalf("%v allocations to decode an update", n)
	}
}
