package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/peer"
)

// The corruption checks follow etcd v3.7.2's (server/etcdserver/corrupt.go).
// Members compare HashKV answers (mvcc.Store.HashKV), which a member asks
// its peers for with peer.Transport.Call:
//
//   - The initial check (feature gate InitialCorruptCheck) runs once at
//     start: a member whose hash differs from a peer's at the same
//     revision and compaction revision does not start.
//   - The periodic check (--corrupt-check-time) runs on the leader: a
//     follower ahead of it, or with the same compaction revision and
//     another hash, gets a CORRUPT alarm, and so does the leader if its
//     own hash of one revision changed.
//   - The compaction hash check (feature gate CompactHashCheck,
//     --compact-hash-check-time) runs on the leader over the hashes its
//     compactions took: members whose hash differs from a majority's get
//     a CORRUPT alarm, or member 0 when no majority agrees.

// requestTimeout is how long a node's own request may take, etcd's
// ReqTimeout.
func (n *Node) requestTimeout() time.Duration {
	return time.Duration(n.cfg.Core.RequestTicks) * n.cfg.Tick
}

// peerHash is one peer's HashKV answer, or the error that came instead.
type peerHash struct {
	id   uint64
	resp *pb.HashKVResponse
	err  error
}

// peerHashes asks every other member for its HashKV of rev.
func (n *Node) peerHashes(ctx context.Context, t peer.Transport, rev int64) []peerHash {
	var out []peerHash
	for _, id := range *n.members.Load() {
		if id == n.ID() {
			continue
		}
		ctx, cancel := context.WithTimeout(ctx, n.requestTimeout())
		p := peerHash{id: id, resp: &pb.HashKVResponse{}}
		p.err = call(ctx, t, id, callHashKV, &pb.HashKVRequest{Revision: rev}, p.resp)
		cancel()
		if p.err != nil {
			p.resp = nil
			n.logf("corruption check: the hash of %d from peer %x: %v", rev, id, p.err)
		}
		out = append(out, p)
	}
	return out
}

// hashKV is this node's HashKV answer.
func (n *Node) hashKV(ctx context.Context, rev int64) (*pb.HashKVResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, n.requestTimeout())
	defer cancel()
	resp, err := n.DoAs(ctx, core.RootCaller(), &pb.HashKVRequest{Revision: rev})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.HashKVResponse), nil
}

// InitialCheck compares this node's hash with its peers' before it
// serves clients, and fails if one with the same compaction revision
// differs. Peers that cannot answer are skipped.
func (n *Node) InitialCheck(ctx context.Context, t peer.Transport) error {
	h, err := n.hashKV(ctx, 0)
	if err != nil {
		return fmt.Errorf("node: initial corruption check: %w", err)
	}
	mismatch := 0
	for _, p := range n.peerHashes(ctx, t, h.HashRevision) {
		if p.resp != nil && p.resp.Hash != h.Hash && p.resp.CompactRevision == h.CompactRevision {
			n.logf("corruption check: peer %x has hash %x of revision %d, this node %x", p.id, p.resp.Hash, h.HashRevision, h.Hash)
			mismatch++
		}
	}
	if mismatch > 0 {
		return fmt.Errorf("node: %x found data inconsistency with peers", n.ID())
	}
	return nil
}

// periodicCheck is etcd's PeriodicCheck, run by the leader.
func (n *Node) periodicCheck(ctx context.Context, t peer.Transport) error {
	h, err := n.hashKV(ctx, 0)
	if err != nil {
		return err
	}
	peers := n.peerHashes(ctx, t, h.HashRevision)
	rctx, cancel := context.WithTimeout(ctx, n.requestTimeout())
	_, err = n.DoAs(rctx, core.RootCaller(), &pb.RangeRequest{Key: []byte{0}, CountOnly: true})
	cancel()
	if err != nil {
		return err
	}
	h2, err := n.hashKV(ctx, 0)
	if err != nil {
		return err
	}
	alarmed := false
	mismatch := func(id uint64, why string) {
		n.logf("corruption check: %s; raising CORRUPT for %x", why, id)
		if !alarmed {
			alarmed = true
			n.corruptAlarm(ctx, id)
		}
	}
	if h2.Hash != h.Hash && h2.HashRevision == h.HashRevision && h2.CompactRevision == h.CompactRevision {
		mismatch(n.ID(), fmt.Sprintf("the hash of revision %d changed from %x to %x", h.HashRevision, h.Hash, h2.Hash))
	}
	for _, p := range peers {
		switch {
		case p.resp == nil:
		case p.resp.Header.GetRevision() > h2.Header.GetRevision():
			mismatch(p.id, fmt.Sprintf("peer %x is at revision %d, ahead of the leader's %d", p.id, p.resp.Header.GetRevision(), h2.Header.GetRevision()))
		case p.resp.CompactRevision > h2.CompactRevision:
			mismatch(p.id, fmt.Sprintf("peer %x compacted at %d, after the leader's %d", p.id, p.resp.CompactRevision, h2.CompactRevision))
		case p.resp.CompactRevision == h.CompactRevision && p.resp.Hash != h.Hash:
			mismatch(p.id, fmt.Sprintf("peer %x has hash %x of revision %d, the leader %x", p.id, p.resp.Hash, h.HashRevision, h.Hash))
		}
	}
	return nil
}

// compactHashCheck is etcd's CompactHashCheck, run by the leader over the
// hashes its compactions took that are newer than checked. It returns the
// newest revision it checked on every peer.
func (n *Node) compactHashCheck(ctx context.Context, t peer.Transport, checked int64) int64 {
	ch := make(chan []mvcc.KVHash, 1)
	if err := n.put(ctx, input{hashes: ch}); err != nil {
		return checked
	}
	var hashes []mvcc.KVHash
	select {
	case hashes = <-ch:
	case <-ctx.Done():
		return checked
	case <-n.done:
		return checked
	}
	slices.SortFunc(hashes, func(a, b mvcc.KVHash) int { return cmp.Compare(b.Revision, a.Revision) })
	for _, h := range hashes {
		if h.Revision <= checked {
			break
		}
		peers := n.peerHashes(ctx, t, h.Revision)
		if len(peers) == 0 {
			continue
		}
		byHash := map[uint32][]uint64{h.Hash: {n.ID()}}
		answered := 0
		for _, p := range peers {
			if p.resp == nil || p.resp.CompactRevision != h.CompactRevision {
				continue
			}
			answered++
			byHash[p.resp.Hash] = append(byHash[p.resp.Hash], p.id)
		}
		if len(byHash) == 1 {
			if answered == len(peers) {
				return h.Revision
			}
			continue
		}
		quorum := (len(peers)+1)/2 + 1
		agreed := false
		for hash, ids := range byHash {
			if len(ids) >= quorum {
				agreed = true
				delete(byHash, hash)
				break
			}
		}
		n.logf("corruption check: the hashes of compaction revision %d differ: %v", h.Revision, byHash)
		if !agreed {
			n.corruptAlarm(ctx, 0)
			return checked
		}
		for _, ids := range byHash {
			for _, id := range ids {
				n.corruptAlarm(ctx, id)
			}
		}
		return checked
	}
	return checked
}

// corruptAlarm raises a CORRUPT alarm for member id.
func (n *Node) corruptAlarm(ctx context.Context, id uint64) {
	ctx, cancel := context.WithTimeout(ctx, n.requestTimeout())
	defer cancel()
	_, err := n.DoAs(ctx, core.RootCaller(), &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_CORRUPT, MemberID: id})
	if err != nil && !errors.Is(err, rpctypes.ErrGRPCStopped) {
		n.logf("corruption check: raising CORRUPT for %x: %v", id, err)
	}
}

// checkHashes runs the periodic and compaction hash checks on the leader
// until ctx ends.
func (n *Node) checkHashes(ctx context.Context, t peer.Transport) {
	var periodic, compact <-chan time.Time
	if d := n.cfg.CorruptCheckTime; d > 0 {
		tk := time.NewTicker(d)
		defer tk.Stop()
		periodic = tk.C
	}
	if d := n.cfg.CompactHashCheckTime; d > 0 {
		tk := time.NewTicker(d)
		defer tk.Stop()
		compact = tk.C
	}
	var checked int64
	for {
		select {
		case <-periodic:
			if n.Leader() == n.ID() {
				if err := n.periodicCheck(ctx, t); err != nil && ctx.Err() == nil {
					n.logf("corruption check: %v", err)
				}
			}
		case <-compact:
			if n.Leader() == n.ID() {
				checked = n.compactHashCheck(ctx, t, checked)
			}
		case <-ctx.Done():
			return
		case <-n.done:
			return
		}
	}
}
