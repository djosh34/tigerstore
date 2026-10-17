package history

import "testing"

func TestWithoutLostWrites(t *testing.T) {
	put := func(v string) Request {
		return Request{Put: &PutRequest{Key: "k", Value: v}}
	}
	del := Request{Delete: &DeleteRequest{Key: "k"}}
	amb := &Error{Code: "Unknown", Ambiguous: true}
	h := &History{
		Ops: []Op{
			{Client: 0, Req: put("lost"), Err: amb},
			{Client: 0, Req: put("kept"), Err: amb},
			{Client: 1, Req: del, Resp: &Response{}},
			{Client: 1, Req: del, Err: amb},
			{Client: 2, Req: Request{Range: &RangeRequest{Key: "k"}}, Err: amb},
		},
		Log: []Request{put("kept"), del},
	}
	got, pruned, err := h.WithoutLostWrites()
	if err != nil {
		t.Fatal(err)
	}
	// The lost put and the delete whose only log copy belongs to the
	// successful call go; the committed put and the read stay.
	if pruned != 2 || len(got.Ops) != 3 || got.Ops[0].Req.Put.Value != "kept" || got.Ops[1].Resp == nil || got.Ops[2].Req.Range == nil {
		t.Fatalf("pruned %d, kept %+v", pruned, got.Ops)
	}
	h.Log = append(h.Log, del)
	if _, pruned, err := h.WithoutLostWrites(); err != nil || pruned != 1 {
		t.Fatalf("with a second delete in the log, pruned %d, want 1", pruned)
	}
	if len(h.Ops) != 5 {
		t.Fatal("the input history changed")
	}
}
