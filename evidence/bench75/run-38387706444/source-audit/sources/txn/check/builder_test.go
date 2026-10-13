package check

import (
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// builder runs requests one at a time against the model and records the
// history a correct server would produce. Tests then break it on purpose.
type builder struct {
	t       *testing.T
	s       *state
	now     int64
	h       history.History
	watches []*history.Watch
}

func newBuilder(t *testing.T, keys ...string) *builder {
	t.Helper()
	slices.Sort(keys)
	return &builder{t: t, s: newState(keys)}
}

// do runs req for client and returns the index of its op.
func (b *builder) do(client int, req history.Request) int {
	b.t.Helper()
	b.now += 10
	call := b.now
	o := b.s.step(req, true)
	if o.historical {
		// The model keeps no old versions; read them from the log.
		r, err := newReplay(b.h.Log)
		if err != nil {
			b.t.Fatal(err)
		}
		o.resp.Range = r.read(req.Range, req.Range.Revision)
	}
	if o.err == nil && changes(req, o) {
		b.h.Log = append(b.h.Log, req)
	}
	b.now += 10
	op := history.Op{Client: client, Call: call, Return: b.now, Req: req, Resp: o.resp, Err: o.err}
	b.h.Ops = append(b.h.Ops, op)
	b.deliver(o.events)
	return len(b.h.Ops) - 1
}

// deliver sends the events of one revision to every open watch.
func (b *builder) deliver(events []history.Event) {
	if len(events) == 0 {
		return
	}
	for _, w := range b.watches {
		var resp history.WatchResponse
		for _, e := range events {
			if !history.InRange(e.KV.Key, w.Req.Key, w.Req.End) {
				continue
			}
			if !w.Req.PrevKV {
				e.PrevKV = nil
			}
			resp.Events = append(resp.Events, e)
		}
		if len(resp.Events) > 0 {
			resp.Time, resp.Revision = b.now, b.s.rev
			w.Responses = append(w.Responses, resp)
		}
	}
}

func (b *builder) put(client int, key, value string) int {
	b.t.Helper()
	return b.do(client, history.Request{Put: &history.PutRequest{Key: key, Value: value}})
}

func (b *builder) get(client int, key string) int {
	b.t.Helper()
	return b.do(client, history.Request{Range: &history.RangeRequest{Key: key}})
}

func (b *builder) del(client int, key string) int {
	b.t.Helper()
	return b.do(client, history.Request{Delete: &history.DeleteRequest{Key: key}})
}

// cas puts value if key has mod revision mod, else reads key, the way
// kube-apiserver updates objects.
func (b *builder) cas(client int, key string, mod int64, value string) int {
	b.t.Helper()
	return b.do(client, history.Request{Txn: &history.TxnRequest{
		Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: mod}},
		Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: value}}},
		Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}},
	}})
}

// watch opens a watch from the next revision with a created response.
func (b *builder) watch(client int, key, end string, prevKV bool) {
	w := &history.Watch{Client: client, Req: history.WatchRequest{Key: key, End: end, PrevKV: prevKV}}
	w.Responses = append(w.Responses, history.WatchResponse{Time: b.now, Revision: b.s.rev, Created: true})
	b.watches = append(b.watches, w)
}

// progress sends a progress notification to every open watch.
func (b *builder) progress() {
	for _, w := range b.watches {
		w.Responses = append(w.Responses, history.WatchResponse{Time: b.now, Revision: b.s.rev, Progress: true})
	}
}

// done returns the history with every watch and the current revision as
// target.
func (b *builder) done() *history.History {
	h := b.h
	h.Watches = nil
	for _, w := range b.watches {
		h.Watches = append(h.Watches, *w)
	}
	h.Target = b.s.rev
	return &h
}

// mustPass and mustFail run a checker and compare its verdict.
func mustPass(t *testing.T, name string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s convicted a good history: %v", name, err)
	}
}

func mustFail(t *testing.T, name string, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s passed a broken history", name)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%s failed for another reason: got %q, want it to mention %q", name, err, want)
	}
}
