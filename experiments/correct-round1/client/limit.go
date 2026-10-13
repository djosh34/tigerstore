package main

import (
	"errors"
	"fmt"
	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"strings"
)

const capBytes = 1536 * 1024

func framedSize(q history.Request) int {
	r := &pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: ^uint64(0)}}
	if q.Put != nil {
		r.Put = &pb.PutRequest{Key: []byte(q.Put.Key), Value: []byte(q.Put.Value), PrevKv: q.Put.PrevKV}
	}
	if q.Txn != nil {
		r.Txn = &pb.TxnRequest{}
		for _, c := range q.Txn.Compare {
			p := &pb.Compare{Key: []byte(c.Key), Result: pb.Compare_EQUAL}
			if c.Target == history.TargetVersion {
				p.Target = pb.Compare_VERSION
				p.TargetUnion = &pb.Compare_Version{Version: c.Number}
			} else {
				p.Target = pb.Compare_MOD
				p.TargetUnion = &pb.Compare_ModRevision{ModRevision: c.Number}
			}
			r.Txn.Compare = append(r.Txn.Compare, p)
		}
		for _, o := range q.Txn.Success {
			r.Txn.Success = append(r.Txn.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(o.Put.Key), Value: []byte(o.Put.Value), PrevKv: o.Put.PrevKV}}})
		}
		for _, o := range q.Txn.Failure {
			r.Txn.Failure = append(r.Txn.Failure, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(o.Range.Key)}}})
		}
	}
	return proto.Size(r)
}
func fitLimit(q *history.Request, identity string, extra int) {
	var p *history.PutRequest
	if q.Put != nil {
		p = q.Put
	} else if q.Txn != nil {
		p = q.Txn.Success[0].Put
	} else {
		return
	}
	// At this size all affected protobuf lengths stay in the same varint width.
	p.Value = identity + strings.Repeat("x", capBytes-len(identity))
	n := capBytes + extra - framedSize(*q)
	p.Value = p.Value[:len(p.Value)+n]
}
func probeLimit(r *recorder, cs []*clientv3.Client, c config, prefix string) (extras []int, err error) {
	var evidence []map[string]any
	record := func(label string, cl *clientv3.Client, id int, q history.Request) history.Op {
		op := r.call(cl, id, q, c.requestTimeout)
		evidence = append(evidence, map[string]any{"label": label, "client": id, "value_bytes": len(q.Put.Value), "strict_framed_bytes": framedSize(q), "response": op.Resp, "error": op.Err})
		return op
	}
	defer func() { err = errors.Join(err, save(c.out+"/limit.json", evidence)) }()
	for i, cl := range cs {
		q := history.Request{Put: &history.PutRequest{Key: prefix + "k0", Value: strings.Repeat("z", capBytes), PrevKV: true}}
		op := record("exact-1.5-MiB-rejection", cl, 30+i, q)
		if op.Err == nil || op.Err.Ambiguous || !strings.Contains(op.Err.Message, "request is too large") {
			return nil, fmt.Errorf("exact limit did not return definite request-too-large: %+v", op.Err)
		}
	}
	// A stock etcd member can add TLS-derived username bytes to the header,
	// even with auth disabled. Measure each pinned member instead of assuming
	// that the strict-ID sizing used by tigerstore is the complete header.
	for i, cl := range cs {
		low, high := -64, 10
		probe := func(extra int) (bool, error) {
			q := history.Request{Put: &history.PutRequest{Key: prefix + "k0", Value: "probe", PrevKV: true}}
			fitLimit(&q, fmt.Sprintf("probe-c%d-e%d:", i, extra), extra)
			op := record("boundary-search", cl, 33+i, q)
			if op.Resp != nil {
				return true, nil
			}
			if op.Err == nil || op.Err.Ambiguous || !strings.Contains(op.Err.Message, "request is too large") {
				return false, fmt.Errorf("limit probe ambiguous or unexpected: %+v", op.Err)
			}
			return false, nil
		}
		ok, e := probe(low)
		if e != nil {
			return nil, e
		}
		if !ok {
			return nil, errors.New("lower limit bracket rejected")
		}
		ok, e = probe(high)
		if e != nil {
			return nil, e
		}
		if ok {
			return nil, errors.New("upper limit bracket admitted")
		}
		for high-low > 1 {
			mid := (low + high) / 2
			ok, e = probe(mid)
			if e != nil {
				return nil, e
			}
			if ok {
				low = mid
			} else {
				high = mid
			}
		}
		extras = append(extras, low)
		evidence = append(evidence, map[string]any{"label": "largest-admitted-allowance", "client": i, "extra_bytes": low, "rejected_extra_bytes": high})
	}
	return extras, nil
}
