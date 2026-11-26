package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func dueGapHistory(t testing.TB, leases, gaps int) *history.History {
	t.Helper()
	h := &history.History{Start: &history.Start{Rev: 1}}
	for i := range leases {
		id := int64(i + 1)
		h.Start.Leases = append(h.Start.Leases, id)
		h.Ops = append(h.Ops,
			history.Op{Client: len(h.Ops), Call: 1, Return: 2, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: id}}, Err: errUnavailable},
			history.Op{Client: len(h.Ops) + 1, Call: 3, Return: 4, Req: history.Request{Put: &history.PutRequest{Key: "k", Lease: id}}, Err: errLeaseNotFound},
		)
	}
	for i := range gaps {
		h.Ops = append(h.Ops, history.Op{Client: len(h.Ops), Call: int64(5 + i*2), Return: int64(6 + i*2), Req: history.Request{Put: &history.PutRequest{Key: "k", Value: "v"}}, Err: errUnavailable})
	}
	h.Ops = append(h.Ops, history.Op{
		Client: len(h.Ops), Call: 1000, Return: 1001,
		Req:  history.Request{Range: &history.RangeRequest{Key: "k"}},
		Resp: &history.Response{Revision: int64(gaps + 1), Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "k", Value: "v", CreateRevision: 2, ModRevision: int64(gaps + 1), Version: int64(gaps)}}}},
	})
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	return h
}

// sameRevisionGrants exercises the fast path of slotOrder: every grant
// fits in call order, and every earlier call is within its return time.
func sameRevisionGrants(n int) *history.History {
	h := &history.History{}
	for i := range n {
		id := int64(i + 1)
		h.Ops = append(h.Ops, history.Op{
			Client: i, Call: 1, Return: 2,
			Req:  history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: id, TTL: 10}},
			Resp: &history.Response{Revision: 1, LeaseGrant: &history.LeaseGrantResponse{ID: id, TTL: 10}},
		})
	}
	return h
}
