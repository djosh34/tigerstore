package sim

import (
	"bytes"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
)

// values keeps one copy of each value the clients of the Kubernetes-shaped
// load put, so that the run's records of it share that copy: the answers
// that return it, read from a disk or decoded by clientv3, its watch
// events, the committed log and the checkers' history would otherwise
// each hold their own, and a long run holds hundreds of thousands. A
// value is shared only where it is equal byte for byte to the one put,
// so the records hold exactly what the nodes answered. Every value starts
// with a part of its own, up to its first space (kubePut), by which it is
// found.
type values struct {
	bytes   map[string][]byte
	strings map[string]string
}

func newValues() *values {
	return &values{bytes: map[string][]byte{}, strings: map[string]string{}}
}

// head is the part of v by which it is found.
func head(v []byte) []byte {
	h, _, _ := bytes.Cut(v, []byte{' '})
	return h
}

// headString is head of a string.
func headString(v string) string {
	h, _, _ := strings.Cut(v, " ")
	return h
}

// add keeps v, a value a client puts.
func (vs *values) add(v []byte) { vs.bytes[string(head(v))] = v }

// share returns the kept copy of v, if it is equal to v, or else v.
func (vs *values) share(v []byte) []byte {
	if kept, ok := vs.bytes[string(head(v))]; ok && bytes.Equal(kept, v) {
		return kept
	}
	return v
}

// shareString is share for the checkers' strings: the first string equal
// to a kept value is the one the others share.
func (vs *values) shareString(v string) string {
	h := headString(v)
	if kept, ok := vs.strings[h]; ok && kept == v {
		return kept
	}
	if kept, ok := vs.bytes[h]; ok && string(kept) == v {
		vs.strings[h] = v
	}
	return v
}

// shareKVs shares the values of kvs.
func (vs *values) shareKVs(kvs ...*mvccpb.KeyValue) {
	for _, kv := range kvs {
		if kv != nil {
			kv.Value = vs.share(kv.Value)
		}
	}
}

// shareMessage shares the values a request or an answer holds.
func (vs *values) shareMessage(m proto.Message) {
	switch m := m.(type) {
	case *pb.PutRequest:
		m.Value = vs.share(m.Value)
	case *pb.TxnRequest:
		for _, ops := range [][]*pb.RequestOp{m.Success, m.Failure} {
			for _, op := range ops {
				if p := op.GetRequestPut(); p != nil {
					p.Value = vs.share(p.Value)
				}
			}
		}
	case *pb.RangeResponse:
		vs.shareKVs(m.Kvs...)
	case *pb.PutResponse:
		vs.shareKVs(m.PrevKv)
	case *pb.DeleteRangeResponse:
		vs.shareKVs(m.PrevKvs...)
	case *pb.TxnResponse:
		for _, op := range m.Responses {
			switch {
			case op.GetResponseRange() != nil:
				vs.shareMessage(op.GetResponseRange())
			case op.GetResponsePut() != nil:
				vs.shareMessage(op.GetResponsePut())
			case op.GetResponseDeleteRange() != nil:
				vs.shareMessage(op.GetResponseDeleteRange())
			}
		}
	case *pb.WatchResponse:
		for _, ev := range m.Events {
			vs.shareKVs(ev.Kv, ev.PrevKv)
		}
	}
}

// shareHistoryKVs shares the values of the checkers' kvs.
func (vs *values) shareHistoryKVs(kvs []history.KeyValue) {
	for i := range kvs {
		kvs[i].Value = vs.shareString(kvs[i].Value)
	}
}

// shareRequest shares the values of the checkers' request.
func (vs *values) shareRequest(r *history.Request) {
	if r.Put != nil {
		r.Put.Value = vs.shareString(r.Put.Value)
	}
	if r.Txn != nil {
		for _, ops := range [][]history.TxnOp{r.Txn.Success, r.Txn.Failure} {
			for _, op := range ops {
				if op.Put != nil {
					op.Put.Value = vs.shareString(op.Put.Value)
				}
			}
		}
	}
}

// shareResponse shares the values of the checkers' answer.
func (vs *values) shareResponse(r *history.Response) {
	if r == nil {
		return
	}
	share := func(rr *history.RangeResponse, pr *history.PutResponse, dr *history.DeleteResponse) {
		if rr != nil {
			vs.shareHistoryKVs(rr.KVs)
		}
		if pr != nil && pr.PrevKV != nil {
			pr.PrevKV.Value = vs.shareString(pr.PrevKV.Value)
		}
		if dr != nil {
			vs.shareHistoryKVs(dr.PrevKVs)
		}
	}
	share(r.Range, r.Put, r.Delete)
	if r.Txn != nil {
		for _, res := range r.Txn.Results {
			share(res.Range, res.Put, res.Delete)
		}
	}
}

// shareEvents shares the values of the checkers' watch events.
func (vs *values) shareEvents(evs []history.Event) {
	for i := range evs {
		evs[i].KV.Value = vs.shareString(evs[i].KV.Value)
		if p := evs[i].PrevKV; p != nil {
			p.Value = vs.shareString(p.Value)
		}
	}
}
