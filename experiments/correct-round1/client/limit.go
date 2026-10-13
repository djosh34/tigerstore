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
func probeLimit(r *recorder, cs []*clientv3.Client, c config, prefix string) (extra int, err error) {
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
			return 0, fmt.Errorf("exact limit did not return definite request-too-large: %+v", op.Err)
		}
	}
	largest := -1
	for extra := 0; extra <= 10; extra++ {
		q := history.Request{Put: &history.PutRequest{Key: prefix + "k0", Value: "probe", PrevKV: true}}
		fitLimit(&q, fmt.Sprintf("probe-%d:", extra), extra)
		op := record("boundary-scan", cs[0], 33, q)
		if op.Resp != nil {
			largest = extra
			continue
		}
		if op.Err == nil || op.Err.Ambiguous || !strings.Contains(op.Err.Message, "request is too large") {
			return 0, fmt.Errorf("limit probe ambiguous or unexpected: %+v", op.Err)
		}
		if largest < 0 {
			return 0, errors.New("strict near-limit payload rejected")
		}
		return largest, nil
	}
	return 0, errors.New("no near-limit rejection boundary within ten bytes")
}
