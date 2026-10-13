package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type normalizationDiagnostic struct {
	Type     string          `json:"type"`
	Raw      json.RawMessage `json:"raw"`
	Decision string          `json:"decision"`
	Stamp    int64           `json:"stamp"`
}
type normalizer struct {
	stamp       int64
	leases      map[int64]int64
	diagnostics []normalizationDiagnostic
}

func newNormalizer() *normalizer { return &normalizer{leases: map[int64]int64{}} }

// This adapter does not call core.State or use watch events to choose expiries.
// It interprets every committed lease command and keeps raw renewals/checkpoints.
func (n *normalizer) normalize(m proto.Message, tiger bool) ([]history.Request, error) {
	raw, e := protojson.Marshal(m)
	if e != nil {
		return nil, e
	}
	d := normalizationDiagnostic{Type: string(m.ProtoReflect().Descriptor().FullName()), Raw: raw, Decision: "history request"}
	var out []history.Request
	switch m := m.(type) {
	case *pb.LeaseKeepAliveRequest:
		if !tiger {
			return nil, errors.New("unexpected replicated etcd keepalive")
		}
		if _, ok := n.leases[m.ID]; ok {
			n.stamp++
			n.leases[m.ID] = n.stamp
		}
		d.Decision = "renewal changes stamp, no key mutation in history format"
	case *pb.LeaseCheckpoint:
		if !tiger {
			return nil, errors.New("unexpected etcd single checkpoint")
		}
		stamp, ok := n.leases[m.ID]
		if ok && stamp == m.Remaining_TTL {
			out = []history.Request{{LeaseRevoke: &history.LeaseRevokeRequest{ID: m.ID}}}
			delete(n.leases, m.ID)
			d.Decision = "current stamped expiry normalized as revoke"
		} else {
			d.Decision = "stale stamped expiry is a no-op"
		}
	case *pb.LeaseCheckpointRequest:
		if tiger {
			return nil, errors.New("unexpected tigerstore checkpoint list")
		}
		for _, cp := range m.Checkpoints {
			if cp.ID == 0 || cp.Remaining_TTL < 0 {
				return nil, fmt.Errorf("invalid etcd checkpoint %v", cp)
			}
		}
		d.Decision = "etcd remaining TTL checkpoint, no key mutation"
	default:
		q, e := request(m)
		if e != nil {
			return nil, e
		}
		if e = campaignRequest(q); e != nil {
			return nil, e
		}
		if g := q.LeaseGrant; g != nil {
			if g.ID == 0 || g.TTL > 9000000000 {
				return nil, errors.New("unsupported invalid committed grant")
			}
			if _, ok := n.leases[g.ID]; !ok {
				n.stamp++
				n.leases[g.ID] = n.stamp
			}
		}
		if v := q.LeaseRevoke; v != nil {
			delete(n.leases, v.ID)
		}
		out = []history.Request{q}
	}
	d.Stamp = n.stamp
	n.diagnostics = append(n.diagnostics, d)
	return out, nil
}
