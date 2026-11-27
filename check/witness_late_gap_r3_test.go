package check

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestLateGapR3Observer(t *testing.T) {
	q := history.Request{Delete: &history.DeleteRequest{Key: "a"}}
	h := &history.History{
		Start:  &history.Start{Rev: 602474, KVs: []history.KeyValue{{Key: "a", Value: "x", CreateRevision: 602474, ModRevision: 602474, Version: 1}}},
		Target: 602475, Log: []history.Request{q},
		Ops: []history.Op{
			{Client: 1, Call: 1, Return: 2, Req: q, Err: &history.Error{Code: "Unavailable", Message: "lost reply", Ambiguous: true}},
			{Client: 1, Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 602475, Range: &history.RangeResponse{}}},
		},
	}
	var base, got int64
	if err := Witnessed(h, FromRevisions, time.Minute, StepsUsed(&base)); err != nil {
		t.Fatal(err)
	}
	LateGapR3Start(h.Target)
	defer func() { r3.Enabled = false; r3.Log = nil; r3.JSON = nil; r3.ActualNear = nil }()
	if err := Witnessed(h, FromLog, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Witnessed(h, FromRevisions, time.Minute, StepsUsed(&got)); err != nil {
		t.Fatal(err)
	}
	if got != base {
		t.Fatalf("observer changed weighted steps: %d versus %d", got, base)
	}
	g := r3.Gaps[0]
	if g.LastPool == nil || g.LastPool.Certified || g.LastPool.Individual["unknown: delete: present keys, none covered unchanged"] != 1 || g.Candidates[0].Fits["accepted"] != 1 {
		t.Fatalf("unexpected certificate/fit counters: %+v", g)
	}
	var out struct {
		Frames                    []struct{ SameKeyEffect bool }
		FirstDifferentEffectIndex int
	}
	if err := json.Unmarshal(LateGapR3JSON(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Frames) != 1 || !out.Frames[0].SameKeyEffect || out.FirstDifferentEffectIndex != -1 {
		t.Fatalf("log key effects do not match: %+v", out)
	}
}
