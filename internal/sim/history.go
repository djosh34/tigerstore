package sim

import (
	"errors"
	"fmt"
	"math"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/planted"
)

// Checkable converts a run into the format the checkers read. Watch
// clients get client numbers after the request clients.
//
// Lease keepalives are left out: the format has no keepalive, and a
// keepalive changes no key. Instead, every granted lease gets an expiry op:
// an ambiguous LeaseRevoke from its own client, called when the grant was,
// which the checkers' models may apply at any time after it.
//
// An ambiguous put, delete or txn that is not in the committed log never
// took effect, so it is left out. One that is took effect. If it is the
// only call with its request and the log holds it once, its Effect says at
// which revision, and that it was before any write after it in the log was
// acknowledged. Both keep Porcupine's search small, as etcd's robustness
// tests do, and neither can hide a bug: a history with fewer ops, or with
// narrower ones, is never easier to linearize.
func (r *Result) Checkable() (*history.History, error) {
	h := &history.History{Target: r.FinalRev}
	committed, err := r.committed()
	if err != nil {
		return nil, err
	}
	unique, err := r.uniqueInLog(committed)
	if err != nil {
		return nil, err
	}
	effectBy := r.effectBy(unique)
	expiryClient := r.Clients + len(r.Watches)
	for _, rec := range r.History {
		if _, ok := rec.req.(*pb.LeaseKeepAliveRequest); ok {
			continue
		}
		if rec.Ambiguous && neverCommitted(rec.req, committed) {
			continue
		}
		if g, ok := rec.Resp.(*pb.LeaseGrantResponse); ok && rec.Err == nil {
			h.Ops = append(h.Ops, history.Op{
				Client: expiryClient, Call: rec.Call, Return: rec.Call,
				Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: g.ID}},
				Err: &history.Error{Code: "Unknown", Message: "lease expiry", Ambiguous: true},
			})
			expiryClient++
		}
		req, err := frometcd.Request(rec.req)
		if err != nil {
			return nil, err
		}
		op := history.Op{Client: rec.Client, Call: rec.Call, Return: rec.Return, Req: req}
		if rec.Err != nil {
			// Planted: a call that may have taken effect counts as one
			// that certainly did not.
			op.Err = historyError(rec.Err, rec.Ambiguous && !planted.On("B15"))
			if pos, ok := unique[rec]; ok && op.Err.Ambiguous {
				op.Effect = &history.Effect{By: effectBy[pos], Revision: r.revs[pos]}
			}
		} else if op.Resp, err = frometcd.Response(rec.Resp); err != nil {
			return nil, err
		}
		h.Ops = append(h.Ops, op)
	}
	for _, m := range r.Log {
		if _, ok := m.(*pb.LeaseKeepAliveRequest); ok {
			continue
		}
		req, err := frometcd.Request(m)
		if err != nil {
			return nil, err
		}
		h.Log = append(h.Log, req)
	}
	for i, w := range r.Watches {
		hw := history.Watch{
			Client: r.Clients + i,
			Req: history.WatchRequest{
				Key: string(w.Key), End: string(w.End), StartRevision: w.Start,
				PrevKV: w.PrevKV, ProgressNotify: w.ProgressNotify,
			},
		}
		for _, wr := range w.Responses {
			hr, err := watchResponse(wr)
			if err != nil {
				return nil, err
			}
			hw.Responses = append(hw.Responses, hr)
		}
		h.Watches = append(h.Watches, hw)
	}
	return h, nil
}

// committed returns the positions in the committed log of each encoded
// put, delete and txn. The cluster logs them as the client sent them.
func (r *Result) committed() (map[string][]int, error) {
	if len(r.revs) != len(r.Log) {
		return nil, fmt.Errorf("sim: %d revisions for %d log entries", len(r.revs), len(r.Log))
	}
	out := map[string][]int{}
	for i, m := range r.Log {
		if !writeOp(m) {
			continue
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			return nil, err
		}
		out[string(b)] = append(out[string(b)], i)
	}
	return out, nil
}

// uniqueInLog returns the position in the committed log of the write of
// each call that is the only call with its request and is in the log once.
// Only then is that entry surely the call's.
func (r *Result) uniqueInLog(committed map[string][]int) (map[*Record]int, error) {
	calls := map[string][]*Record{}
	for _, rec := range r.History {
		if !writeOp(rec.req) {
			continue
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(rec.req)
		if err != nil {
			return nil, err
		}
		calls[string(b)] = append(calls[string(b)], rec)
	}
	out := map[*Record]int{}
	for b, recs := range calls {
		if pos := committed[b]; len(recs) == 1 && len(pos) == 1 {
			out[recs[0]] = pos[0]
		}
	}
	return out, nil
}

// effectBy returns, for each position in the committed log, the earliest
// time a unique write at or after it was acknowledged, or math.MaxInt64 if
// none was.
func (r *Result) effectBy(unique map[*Record]int) []int64 {
	by := make([]int64, len(r.Log))
	for i := range by {
		by[i] = math.MaxInt64
	}
	for rec, pos := range unique {
		if rec.Err == nil {
			by[pos] = rec.Return
		}
	}
	for i := len(by) - 2; i >= 0; i-- {
		by[i] = min(by[i], by[i+1])
	}
	return by
}

func writeOp(m proto.Message) bool {
	switch m.(type) {
	case *pb.PutRequest, *pb.DeleteRangeRequest, *pb.TxnRequest:
		return true
	}
	return false
}

func neverCommitted(m proto.Message, committed map[string][]int) bool {
	if !writeOp(m) {
		return false
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	return err == nil && len(committed[string(b)]) == 0
}

func historyError(err error, ambiguous bool) *history.Error {
	if st, ok := status.FromError(err); ok {
		return &history.Error{Code: st.Code().String(), Message: st.Message(), Ambiguous: ambiguous}
	}
	code := codes.Unavailable
	if errors.Is(err, errClientTimeout) {
		code = codes.DeadlineExceeded
	}
	return &history.Error{Code: code.String(), Message: err.Error(), Ambiguous: ambiguous}
}

func watchResponse(wr WatchRecord) (history.WatchResponse, error) {
	r := wr.Resp
	out := history.WatchResponse{
		Time: wr.Time, Revision: r.Header.Revision, Created: r.Created, Canceled: r.Canceled,
		CompactRevision: r.CompactRevision, CancelReason: r.CancelReason,
		Progress: !r.Created && !r.Canceled && len(r.Events) == 0,
	}
	for _, ev := range r.Events {
		he := history.Event{KV: frometcd.KeyValue(ev.Kv), PrevKV: frometcd.PrevKV(ev.PrevKv)}
		switch ev.Type {
		case mvccpb.Event_PUT:
			he.Type = history.EventPut
		case mvccpb.Event_DELETE:
			he.Type = history.EventDelete
		default:
			return out, fmt.Errorf("watch event of unknown type %d", ev.Type)
		}
		out.Events = append(out.Events, he)
	}
	return out, nil
}
