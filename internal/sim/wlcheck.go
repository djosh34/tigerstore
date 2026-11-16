package sim

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
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
	applied := live[0].proc.wl.turn.Applied
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
	model, log, revs, err := replay(res.Log)
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
	if err := s.finalReads(live, model.Rev(), want.Kvs); err != nil {
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
	if err := s.checkWatchersFreed(); err != nil {
		return err
	}
	if err := s.checkColdRestarts(res); err != nil {
		return err
	}
	if err := s.checkRecoverable(); err != nil {
		return err
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
// entries, which record checked entry by entry, and returns them.
func (s *Sim) convergedWL() ([]*node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.live()
	for _, n := range live {
		if n.proc == nil {
			return nil, fmt.Errorf("node %d is down at the end", n.id)
		}
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
		got, err := pb.NewKVClient(p.wl.conn).Range(ctx, &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, Revision: rev, Serializable: true})
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
