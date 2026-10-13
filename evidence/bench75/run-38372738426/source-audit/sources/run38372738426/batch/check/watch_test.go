package check

import (
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// The watches of workload: 0 is the prefix with previous values, 1 the key
// /r/a without.
const (
	prefixWatch = 0
	keyWatch    = 1
)

// findEvent returns the response and event index of the first event in w
// that matches.
func findEvent(t *testing.T, w *history.Watch, match func(history.Event) bool) (int, int) {
	t.Helper()
	for i, r := range w.Responses {
		for j, e := range r.Events {
			if match(e) {
				return i, j
			}
		}
	}
	t.Fatal("no such event")
	return 0, 0
}

func isUpdate(e history.Event) bool { return e.Type == history.EventPut && e.KV.Version > 1 }
func isDelete(e history.Event) bool { return e.Type == history.EventDelete }
func isCreate(e history.Event) bool { return e.IsCreate() }

func TestWatchesGood(t *testing.T) {
	t.Run("workload", func(t *testing.T) {
		h := workload(t).done()
		mustPass(t, "Watches", Watches(h))
		// The test must see each kind of event with a previous value.
		w := &h.Watches[prefixWatch]
		for _, match := range []func(history.Event) bool{isCreate, isUpdate, isDelete} {
			findEvent(t, w, match)
		}
	})
	t.Run("start revision and compaction cancel", func(t *testing.T) {
		h := workload(t).done()
		h.Watches = append(h.Watches,
			history.Watch{Client: 3, Req: history.WatchRequest{Key: "/r/a", StartRevision: 4, PrevKV: true}},
			history.Watch{Client: 4, Req: history.WatchRequest{Key: "/r/a", StartRevision: 2}, Responses: []history.WatchResponse{
				{Revision: h.Target, Canceled: true, CompactRevision: 4},
			}},
		)
		// The resumed watch gets the update at 4 and nothing after.
		r, _ := newReplay(h.Log)
		for _, e := range r.events {
			if e.KV.Key == "/r/a" && e.Revision() >= 4 {
				h.Watches[len(h.Watches)-2].Responses = append(h.Watches[len(h.Watches)-2].Responses, history.WatchResponse{Revision: e.Revision(), Events: []history.Event{e}})
			}
		}
		mustPass(t, "Watches", Watches(h))
	})
	t.Run("compaction cancel at the next revision when the previous value is gone", func(t *testing.T) {
		h := workload(t).done()
		h.Watches = append(h.Watches, history.Watch{Client: 4, Req: history.WatchRequest{Key: "/r/a", StartRevision: 4, PrevKV: true}, Responses: []history.WatchResponse{
			{Revision: h.Target, Canceled: true, CompactRevision: 4},
		}})
		mustPass(t, "Watches", Watches(h))
	})
	t.Run("one txn over two keys in one response", func(t *testing.T) {
		mustPass(t, "Watches", Watches(twoKeyTxn(t)))
	})
}

// twoKeyTxn is a history with one txn that writes two watched keys.
func twoKeyTxn(t *testing.T) *history.History {
	t.Helper()
	b := newBuilder(t, "a", "b")
	b.watch(0, "a", history.AllKeys, true)
	b.put(1, "a", "0")
	b.do(1, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{
		{Put: &history.PutRequest{Key: "a", Value: "1"}},
		{Put: &history.PutRequest{Key: "b", Value: "1"}},
	}}})
	return b.done()
}

func TestWatchesBroken(t *testing.T) {
	cases := []struct {
		name  string
		want  string
		build func(t *testing.T) *history.History
	}{
		{"update without previous value", "has no previous value", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, j := findEvent(t, w, isUpdate)
			w.Responses[i].Events[j].PrevKV = nil
			return h
		}},
		{"delete without previous value", "has no previous value", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, j := findEvent(t, w, isDelete)
			w.Responses[i].Events[j].PrevKV = nil
			return h
		}},
		{"create with a previous value", "creates its key but has a previous value", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, j := findEvent(t, w, isCreate)
			w.Responses[i].Events[j].PrevKV = &history.KeyValue{Key: "/r/a", Value: "ghost", CreateRevision: 1, ModRevision: 1, Version: 1}
			return h
		}},
		{"wrong previous value", "has previous value", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, j := findEvent(t, w, isUpdate)
			prev := *w.Responses[i].Events[j].PrevKV
			prev.Value = "other"
			w.Responses[i].Events[j].PrevKV = &prev
			return h
		}},
		{"previous value nobody asked for", "did not ask for it", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[keyWatch]
			i, j := findEvent(t, w, isUpdate)
			w.Responses[i].Events[j].PrevKV = &history.KeyValue{Key: "/r/a", Value: "a1", CreateRevision: 2, ModRevision: 2, Version: 1}
			return h
		}},
		{"dropped event", "misses", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, _ := findEvent(t, w, isUpdate)
			w.Responses = slices.Delete(w.Responses, i, i+1)
			return h
		}},
		{"watch that emits nothing", "misses", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[keyWatch]
			w.Responses = w.Responses[:1]
			return h
		}},
		{"duplicate event", "delivered twice", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, _ := findEvent(t, w, isUpdate)
			w.Responses = slices.Insert(w.Responses, i+1, w.Responses[i])
			return h
		}},
		{"events out of order", "comes after revision", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			w.Responses[1], w.Responses[2] = w.Responses[2], w.Responses[1]
			return h
		}},
		{"revision split across responses", "split across responses", func(t *testing.T) *history.History {
			h := twoKeyTxn(t)
			w := &h.Watches[0]
			last := w.Responses[len(w.Responses)-1]
			w.Responses = w.Responses[:len(w.Responses)-1]
			for _, e := range last.Events {
				w.Responses = append(w.Responses, history.WatchResponse{Revision: last.Revision, Events: []history.Event{e}})
			}
			return h
		}},
		{"event above the committed log", "above the last committed revision", func(t *testing.T) *history.History {
			h := workload(t).done()
			h.Log = h.Log[:len(h.Log)-1]
			h.Target = 0
			return h
		}},
		{"progress notification before an earlier event", "comes after a progress notification", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			p := slices.IndexFunc(w.Responses, func(r history.WatchResponse) bool { return r.Progress })
			progress := w.Responses[p]
			w.Responses = slices.Delete(w.Responses, p, p+1)
			w.Responses = slices.Insert(w.Responses, 1, progress)
			return h
		}},
		{"event outside the watched range", "does not match the watch", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[keyWatch]
			i, j := findEvent(t, w, isUpdate)
			w.Responses[i].Events[j].KV.Key = "/r/b"
			return h
		}},
		{"compaction cancel below the needed revision", "only needed revision", func(t *testing.T) *history.History {
			h := workload(t).done()
			h.Watches = append(h.Watches, history.Watch{Client: 4, Req: history.WatchRequest{Key: "/r/a", StartRevision: 4}, Responses: []history.WatchResponse{
				{Revision: h.Target, Canceled: true, CompactRevision: 4},
			}})
			return h
		}},
		{"compaction cancel at a revision never compacted", "never compacted", func(t *testing.T) *history.History {
			h := workload(t).done()
			h.Watches = append(h.Watches, history.Watch{Client: 4, Req: history.WatchRequest{Key: "/r/a", StartRevision: 2}, Responses: []history.WatchResponse{
				{Revision: h.Target, Canceled: true, CompactRevision: 3},
			}})
			return h
		}},
		{"watch with unknown start", "no start revision", func(t *testing.T) *history.History {
			h := workload(t).done()
			h.Watches = append(h.Watches, history.Watch{Client: 4, Req: history.WatchRequest{Key: "/r/a"}})
			return h
		}},
		{"wrong value in an event", "got put", func(t *testing.T) *history.History {
			h := workload(t).done()
			w := &h.Watches[prefixWatch]
			i, j := findEvent(t, w, isUpdate)
			w.Responses[i].Events[j].KV.Value = "x"
			return h
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustFail(t, "Watches", Watches(c.build(t)), c.want)
		})
	}
}
