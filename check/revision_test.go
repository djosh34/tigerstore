package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func TestRevisionsGood(t *testing.T) {
	t.Run("workload", func(t *testing.T) {
		mustPass(t, "Revisions", Revisions(workload(t).done()))
	})
	t.Run("delete and recreate", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.put(0, "a", "1")
		b.put(0, "a", "2")
		b.del(1, "a")
		b.put(2, "a", "3")
		b.get(1, "a")
		mustPass(t, "Revisions", Revisions(b.done()))
	})
	t.Run("compact at the current revision and read there", func(t *testing.T) {
		b := newBuilder(t, "a")
		b.put(0, "a", "1")
		b.put(0, "a", "2")
		b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 3}})
		b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 3}})
		b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
		b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 4}})
		mustPass(t, "Revisions", Revisions(b.done()))
	})
}

func TestRevisionsBroken(t *testing.T) {
	cases := []struct {
		name  string
		want  string
		build func(t *testing.T) *history.History
	}{
		{"two writes share a revision", "both wrote at revision", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "1")
			put := b.put(1, "b", "1")
			h := b.done()
			h.Ops[put].Resp.Revision = 2
			return h
		}},
		{"write not above a revision already returned", "called at", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.put(0, "a", "1")
			b.get(0, "a")
			put := b.put(1, "b", "1")
			h := b.done()
			h.Ops[put].Resp.Revision = 2
			return h
		}},
		{"revision goes backwards", "called at", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			get := b.get(1, "a")
			h := b.done()
			h.Ops[get].Resp.Revision = 1
			h.Ops[get].Resp.Range = &history.RangeResponse{}
			return h
		}},
		{"txn gets one revision per key", "did not put it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b", "c")
			b.do(0, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
				{Put: &history.PutRequest{Key: "a", Value: "1"}},
				{Put: &history.PutRequest{Key: "b", Value: "1"}},
			}}})
			b.put(1, "c", "1")
			get := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}})
			h := b.done()
			kv := &h.Ops[get].Resp.Range.KVs[1]
			kv.CreateRevision, kv.ModRevision = 3, 3
			return h
		}},
		{"watch misses one key of a multi-key delete", "has no delete event of key", func(t *testing.T) *history.History {
			b := newBuilder(t, "a", "b")
			b.watch(0, "a", history.AllKeys, false)
			b.put(1, "a", "1")
			b.put(1, "b", "1")
			b.do(1, history.Request{Delete: &history.DeleteRequest{Key: "a", End: history.AllKeys, PrevKV: true}})
			h := b.done()
			w := &h.Watches[0]
			last := &w.Responses[len(w.Responses)-1]
			last.Events = last.Events[1:]
			return h
		}},
		{"version kept after delete and recreate", "was deleted at revision", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.del(0, "a")
			b.put(0, "a", "2")
			get := b.get(0, "a")
			h := b.done()
			kv := &h.Ops[get].Resp.Range.KVs[0]
			kv.CreateRevision, kv.Version = 2, 2
			return h
		}},
		{"version 1 with a mod revision above the create revision", "has version 1", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			get := b.get(0, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0].Version = 1
			return h
		}},
		{"versions go down", "has version 2 at revision 3 and version 2", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.get(0, "a")
			b.put(0, "a", "3")
			get := b.get(0, "a")
			h := b.done()
			h.Ops[get].Resp.Range.KVs[0].Version = 2
			return h
		}},
		{"mod revision above the read revision", "above the revision", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			read := b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp.Range.KVs[0] = history.KeyValue{Key: "a", Value: "2", CreateRevision: 2, ModRevision: 3, Version: 2}
			return h
		}},
		{"future revision error for a returned revision", "future revision error", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			read := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp, h.Ops[read].Err = nil, errFutureRev
			return h
		}},
		{"compacted error without a compaction", "no compaction that could cause it", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			read := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			h.Ops[read].Resp, h.Ops[read].Err = nil, errCompacted
			return h
		}},
		{"read below an acked compaction succeeds", "after op", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.put(0, "a", "2")
			b.do(0, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
			h := b.done()
			// Move the read after a compaction at 3.
			read := h.Ops[2]
			read.Call, read.Return = 1000, 1010
			h.Ops[2] = history.Op{Client: 0, Call: 100, Return: 110, Req: history.Request{Compact: &history.CompactRequest{Revision: 3}}, Resp: &history.Response{Revision: 3, Compact: &history.CompactResponse{}}}
			h.Ops = append(h.Ops, read)
			return h
		}},
		{"compact twice at one revision", "after op", func(t *testing.T) *history.History {
			b := newBuilder(t, "a")
			b.put(0, "a", "1")
			b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			again := b.do(0, history.Request{Compact: &history.CompactRequest{Revision: 2}})
			h := b.done()
			h.Ops[again].Resp, h.Ops[again].Err = &history.Response{Revision: 2, Compact: &history.CompactResponse{}}, nil
			return h
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustFail(t, "Revisions", Revisions(c.build(t)), c.want)
		})
	}
}
