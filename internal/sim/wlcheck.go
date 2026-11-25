package sim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/wl"
)

// checkWL is check for a wl run (Config.wl): every live node applied the
// same log to the same state, the record of committed entries replayed
// ends in that state, and the clients' calls agree with it, as check
// says. The nodes are frozen.
func (s *Sim) checkWL(res *Result) error {
	live, err := s.convergedWL()
	if err != nil {
		return err
	}
	if s.cfg.Membership {
		if err := s.checkMembership(); err != nil {
			return err
		}
	}
	applied := s.applied
	if s.quorumLostAt == 0 {
		applied = live[0].proc.wl.turn.Applied
	}
	ents, err := s.committed(applied)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.GetType() != raftpb.EntryNormal || len(e.GetData()) == 0 {
			continue
		}
		ops, err := wlOps(e.GetData())
		if err != nil {
			return fmt.Errorf("entry %d: %w", e.GetIndex(), err)
		}
		for _, op := range ops {
			if s.values != nil {
				s.values.shareMessage(op)
			}
			res.Log = append(res.Log, op)
		}
	}
	model, log, revs, err := replay(s.synth, res.Log)
	if err != nil {
		return err
	}
	res.Log, res.revs, res.logged = log, revs, true
	res.FinalRev = model.Rev()
	all := &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}
	want, err := model.Range(all)
	if err != nil {
		return err
	}
	if s.quorumLostAt > 0 {
		// The survivors stop where they stopped, each checked against
		// the log up to its own last entry.
		if err := s.survivorReads(live); err != nil {
			return err
		}
	} else if err := s.finalReads(live, model.Rev(), want.Kvs); err != nil {
		return err
	}
	for _, r := range res.History {
		if r.Err != nil {
			continue
		}
		if err := checkRecord(model, r); err != nil {
			return fmt.Errorf("client %d call at %d ms on node %d: %w\nrequest: %v\nresponse: %v",
				r.Client, r.Call/Millisecond, r.Node+1, err, r.req, r.Resp)
		}
	}
	if err := checkRealTime(res.History); err != nil {
		return err
	}
	if err := checkLog(res); err != nil {
		return err
	}
	if err := s.checkWatches(model, res); err != nil {
		return err
	}
	// Without a quorum no lease expires, so none must be gone by the end;
	// none may expire early either way.
	alive := aliveLeases(res.Log)
	if s.quorumLostAt > 0 {
		alive = nil
	}
	if err := s.checkLeasesExpire(res, alive); err != nil {
		return err
	}
	if err := s.checkWatchersFreed(); err != nil {
		return err
	}
	if err := s.checkColdRestarts(res); err != nil {
		return err
	}
	if err := s.checkRecoverable(); err != nil {
		return err
	}
	if s.certsExpiredAt > 0 {
		if err := s.checkExpired(res); err != nil {
			return err
		}
	}
	if s.quorumLostAt > 0 {
		return s.checkUnavailable(res)
	}
	return s.checkLive(res)
}

// wlOps are the writes of a wl batch as etcd's requests, but the
// members' announcements and the callers of writes, which change no key.
func wlOps(data []byte) ([]proto.Message, error) {
	b, err := wl.ParseBatch(data)
	if errors.Is(err, wl.ErrNotBatch) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []proto.Message
	for _, op := range b.Ops {
		m, err := op.Message()
		if err != nil {
			return nil, err
		}
		if pm, ok := m.(proto.Message); ok {
			if _, auth := pm.(*pb.InternalRaftRequest); !auth {
				out = append(out, pm)
			}
		}
	}
	return out, nil
}

// convergedWL checks that every live node is up and applied the same
// entries, which record checked entry by entry, and returns them. A
// cluster that lost its quorum cannot bring a node that is behind up to
// date: it returns every live node, whatever it applied.
func (s *Sim) convergedWL() ([]*node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.live()
	for _, n := range live {
		if n.proc == nil {
			return nil, fmt.Errorf("node %d is down at the end", n.id)
		}
	}
	if s.quorumLostAt > 0 {
		return live, nil
	}
	first := live[0].proc.wl.turn
	for _, n := range live[1:] {
		if t := n.proc.wl.turn; t.Applied != first.Applied || t.Rev != first.Rev {
			return nil, fmt.Errorf("node %d applied %d entries to revision %d, node %d applied %d to revision %d",
				n.id, t.Applied, t.Rev, live[0].id, first.Applied, first.Rev)
		}
	}
	if r, ok := s.log.get(first.Applied); first.Applied > 0 && !ok {
		return nil, fmt.Errorf("the record of committed entries has no entry %d", first.Applied)
	} else if ok && !r.known {
		return nil, fmt.Errorf("the record of committed entries has only the hash of entry %d", first.Applied)
	}
	return live, nil
}

// finalReads reads every key of each live node at revision rev, which
// the replayed record ends at, and its hash there, and checks that they
// are want and the same on every node. A frozen node answers no call:
// the nodes run while they are read, and the reads name the revision.
func (s *Sim) finalReads(live []*node, rev int64, want []*mvccpb.KeyValue) error {
	s.thaw()
	defer func() { _ = s.freeze() }()
	var hash uint32
	for i, n := range live {
		s.mu.Lock()
		p := n.proc
		s.mu.Unlock()
		if p == nil {
			return fmt.Errorf("node %d went down at the end", n.id)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		// Every key in one answer, which in a long Kubernetes run is
		// larger than gRPC's default 4 MiB a message (seeds 1 to 3 of
		// TestKubernetesLoadLong: 8 MB); etcd's server sends it whole.
		got, err := pb.NewKVClient(p.wl.conn).Range(ctx, &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, Revision: rev, Serializable: true}, grpc.MaxCallRecvMsgSize(math.MaxInt32))
		var h *pb.HashKVResponse
		if err == nil {
			h, err = pb.NewMaintenanceClient(p.wl.conn).HashKV(ctx, &pb.HashKVRequest{Revision: rev})
		}
		cancel()
		if err != nil {
			return fmt.Errorf("node %d: the final reads at revision %d: %w", n.id, rev, err)
		}
		if !proto.Equal(&pb.RangeResponse{Kvs: got.Kvs}, &pb.RangeResponse{Kvs: want}) {
			return fmt.Errorf("replaying the committed log gives %d keys at revision %d, node %d has %d", len(want), rev, n.id, len(got.Kvs))
		}
		if i > 0 && h.Hash != hash {
			return fmt.Errorf("node %d and node %d hash revision %d differently", n.id, live[0].id, rev)
		}
		hash = h.Hash
	}
	return nil
}

// aliveLeases are the leases the committed log leaves alive: granted and
// never revoked, in ID order. Main's check asks a live node's core; wl's
// asks the log every node applied.
func aliveLeases(log []proto.Message) []int64 {
	alive := map[int64]bool{}
	for _, op := range log {
		switch r := op.(type) {
		case *pb.LeaseGrantRequest:
			alive[r.ID] = true
		case *pb.LeaseRevokeRequest:
			delete(alive, r.ID)
		}
	}
	return slices.Sorted(maps.Keys(alive))
}

// survivorReads reads every key of each live node of a run that lost its
// quorum at the revision it applied to, and checks them against the log
// up to there.
func (s *Sim) survivorReads(live []*node) error {
	for _, n := range live {
		s.mu.Lock()
		applied := n.proc.wl.turn.Applied
		ents, err := s.committed(applied)
		s.mu.Unlock()
		if err != nil {
			return err
		}
		var ops []proto.Message
		for _, e := range ents {
			o, err := wlOps(e.GetData())
			if err != nil {
				return fmt.Errorf("entry %d: %w", e.GetIndex(), err)
			}
			ops = append(ops, o...)
		}
		model, _, _, err := replay(s.synth, ops)
		if err != nil {
			return err
		}
		want, err := model.Range(&pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
		if err != nil {
			return err
		}
		if err := s.finalReads([]*node{n}, model.Rev(), want.Kvs); err != nil {
			return err
		}
	}
	return nil
}

// checkUnavailable checks a run that lost its quorum (Config.LoseQuorum):
// no write that started after the loss succeeded, as none can commit.
// An entry committed before the loss may still apply after it: a
// survivor can learn its commit late.
func (s *Sim) checkUnavailable(res *Result) error {
	writes := 0
	for _, r := range res.History {
		if r.Call <= s.quorumLostAt || !isWrite(r.req) {
			continue
		}
		if r.Err == nil {
			return fmt.Errorf("unavailable: client %d's write at %d ms, after the cluster lost its quorum at %d ms, succeeded: %v",
				r.Client, r.Call/Millisecond, s.quorumLostAt/Millisecond, r.req)
		}
		writes++
	}
	s.mu.Lock()
	s.tracef("unavailable: %d writes after the loss of the quorum at %d ms, none succeeded", writes, s.quorumLostAt/Millisecond)
	s.mu.Unlock()
	return nil
}

// isWrite reports whether req changes the store, which needs a quorum.
func isWrite(req proto.Message) bool {
	switch r := req.(type) {
	case *pb.PutRequest, *pb.DeleteRangeRequest, *pb.LeaseGrantRequest, *pb.LeaseRevokeRequest, *pb.CompactionRequest:
		return true
	case *pb.TxnRequest:
		for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
			for _, op := range ops {
				if op.GetRequestRange() == nil {
					return true
				}
			}
		}
	}
	return false
}
