package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func casRequest(key, value []byte, revision int64) *pb.TxnRequest {
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: key, Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: revision}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: value}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}},
	}
}

func seedRevisions(client pb.KVClient, keys [][]byte) ([]atomic.Int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := client.Range(ctx, &pb.RangeRequest{Key: []byte("/registry/pods/"), RangeEnd: []byte("/registry/pods0"), KeysOnly: true})
	if err != nil {
		return nil, err
	}
	if r == nil || r.Header == nil || r.More || len(r.Kvs) != len(keys) || r.Count != int64(len(keys)) {
		return nil, errors.New("CAS requires every seeded key to exist")
	}
	byKey := make(map[string]int64, len(keys))
	for _, kv := range r.Kvs {
		if kv.ModRevision <= 0 {
			return nil, errors.New("invalid seeded mod revision")
		}
		byKey[string(kv.Key)] = kv.ModRevision
	}
	revisions := make([]atomic.Int64, len(keys))
	for i, key := range keys {
		revision, ok := byKey[string(key)]
		if !ok {
			return nil, fmt.Errorf("missing CAS seed key %s", key)
		}
		revisions[i].Store(revision)
	}
	return revisions, nil
}

func checkCAS(r *pb.TxnResponse, key, value []byte) (int64, error) {
	if r.Header == nil || r.Header.Revision <= 0 || len(r.Responses) != 1 {
		return 0, errors.New("invalid CAS response shape")
	}
	if r.Succeeded {
		p := r.Responses[0].GetResponsePut()
		if p == nil || p.Header == nil || p.Header.Revision != r.Header.Revision {
			return 0, errors.New("invalid CAS Put response")
		}
		return r.Header.Revision, nil
	}
	q := r.Responses[0].GetResponseRange()
	if q == nil || q.Count != 1 || len(q.Kvs) != 1 || !bytes.Equal(q.Kvs[0].Key, key) || !bytes.Equal(q.Kvs[0].Value, value) || q.Kvs[0].ModRevision <= 0 || q.Kvs[0].ModRevision > r.Header.Revision {
		return 0, errors.New("invalid CAS fallback Range response")
	}
	return q.Kvs[0].ModRevision, nil
}

// A late response must never replace a newer cached revision with an older one.
func storeRevision(slot *atomic.Int64, revision int64) {
	for old := slot.Load(); revision > old; old = slot.Load() {
		if slot.CompareAndSwap(old, revision) {
			return
		}
	}
}
