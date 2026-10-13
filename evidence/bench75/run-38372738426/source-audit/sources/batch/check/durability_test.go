package check

import (
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// scan appends a full scan through client 7 at the end of the history, as
// the harness does after recovery.
func scan(h *history.History) *history.Op {
	r, _ := newReplay(h.Log)
	q := &history.RangeRequest{Key: "/", End: history.AllKeys}
	end := h.Ops[len(h.Ops)-1].Return
	h.Ops = append(h.Ops, history.Op{Client: 7, Call: end + 1, Return: end + 2, Req: history.Request{Range: q}, Resp: &history.Response{Revision: r.rev, Range: r.read(q, r.rev)}})
	return &h.Ops[len(h.Ops)-1]
}

func TestDurableGood(t *testing.T) {
	t.Run("workload with overwrites, deletes, a revoke and compaction", func(t *testing.T) {
		h := workload(t).done()
		scan(h)
		mustPass(t, "Durable", Durable(h))
	})
	t.Run("lease expiry in the log", func(t *testing.T) {
		b := newBuilder(t, "/a")
		b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 3, TTL: 1}})
		b.do(0, history.Request{Put: &history.PutRequest{Key: "/a", Value: "x", Lease: 3}})
		expiry := b.do(0, history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 3}})
		h := b.done()
		h.Ops = slices.Delete(h.Ops, expiry, expiry+1)
		scan(h)
		mustPass(t, "Durable", Durable(h))
	})
	t.Run("timed out writes, one committed and one not", func(t *testing.T) {
		b := newBuilder(t, "/a", "/b")
		committed := b.put(0, "/a", "x")
		h := b.done()
		h.Ops[committed].Resp, h.Ops[committed].Err = nil, errUnavailable
		h.Ops = append(h.Ops, history.Op{Client: 1, Call: 100, Return: 200, Req: history.Request{Put: &history.PutRequest{Key: "/b", Value: "y"}}, Err: errUnavailable})
		scan(h)
		mustPass(t, "Durable", Durable(h))
	})
	t.Run("grant whose ID the client never learned", func(t *testing.T) {
		h := &history.History{
			Ops: []history.Op{{Client: 0, Call: 1, Return: 2, Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{TTL: 30}}, Err: errUnavailable}},
			Log: []history.Request{{LeaseGrant: &history.LeaseGrantRequest{ID: 77, TTL: 30}}},
		}
		mustPass(t, "Durable", Durable(h))
	})
}

func TestDurableBroken(t *testing.T) {
	cases := []struct {
		name  string
		want  string
		build func(t *testing.T) *history.History
	}{
		{"acked write lost from the log", "is not in the log", func(t *testing.T) *history.History {
			b := newBuilder(t, "/a", "/b")
			b.put(0, "/a", "1")
			b.put(1, "/b", "2")
			h := b.done()
			h.Log = h.Log[:1]
			return h
		}},
		{"log in another order than the acks", "is not in the log with the response", func(t *testing.T) *history.History {
			b := newBuilder(t, "/a", "/b")
			b.put(0, "/a", "1")
			b.put(1, "/b", "2")
			h := b.done()
			h.Log[0], h.Log[1] = h.Log[1], h.Log[0]
			return h
		}},
		{"value bytes changed in the log", "is not in the log", func(t *testing.T) *history.History {
			b := newBuilder(t, "/a")
			b.put(0, "/a", "1")
			h := b.done()
			h.Log[0] = history.Request{Put: &history.PutRequest{Key: "/a", Value: "2"}}
			return h
		}},
		{"final scan misses a key", "but the log holds", func(t *testing.T) *history.History {
			h := workload(t).done()
			s := scan(h)
			s.Resp.Range.KVs = s.Resp.Range.KVs[1:]
			s.Resp.Range.Count--
			return h
		}},
		{"read above the recovered log", "above the last revision", func(t *testing.T) *history.History {
			h := workload(t).done()
			s := scan(h)
			s.Resp.Revision++
			return h
		}},
		{"phantom write in the log", "no client sent it", func(t *testing.T) *history.History {
			h := workload(t).done()
			h.Log = append(h.Log, history.Request{Put: &history.PutRequest{Key: "/r/a", Value: "phantom"}})
			return h
		}},
		{"rejected write in the log", "its client got an error", func(t *testing.T) *history.History {
			b := newBuilder(t, "/a")
			put := b.put(0, "/a", "1")
			h := b.done()
			h.Ops[put].Resp, h.Ops[put].Err = nil, &history.Error{Code: "InvalidArgument", Message: "etcdserver: request is too large"}
			return h
		}},
		{"write applied twice", "no client sent it", func(t *testing.T) *history.History {
			b := newBuilder(t, "/a")
			b.put(0, "/a", "1")
			h := b.done()
			h.Log = append(h.Log, h.Log[0])
			return h
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustFail(t, "Durable", Durable(c.build(t)), c.want)
		})
	}
}
