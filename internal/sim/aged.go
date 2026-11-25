package sim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// An aged cluster (#465, #474) is built once by an aging run, a long run
// of real work, and kept as an image: each node's durable disk and the
// run's record of what it committed, compacted at its last revision. A
// fork is a short run whose nodes start from copies of the image's
// disks, as after the whole cluster lost power, with its own seed,
// faults and clients. Its record goes on from the image's, so every
// entry its nodes replay or load from a checkpoint is checked against the
// aging run; its checkers start from the image's store (history.Start),
// not from the aging run's whole history.

// image is what an aging run leaves for its forks.
type image struct {
	// src holds the aging run's certificate and kept pages, which its
	// forks share: the image's disks keep their pages there.
	src   *Sim
	disks []*simfs.Disk
	ids   []uint64
	// log, ckpts, maxLogged and applied are the aging run's record of
	// what its nodes committed, up to applied.
	log       *entryLog
	ckpts     map[uint64]ckptRec
	maxLogged uint64
	applied   uint64
	// expiredAt and revokedAt are when each lease first expired and was
	// revoked: the forks' nodes apply those again as they replay their
	// logs.
	expiredAt, revokedAt map[int64]int64
	// ops is the number of writes in the committed log, and start the
	// store after them, compacted at its revision.
	ops   int
	start *history.Start
	// synth, for a synthetic image (syntheticImage), is the store its
	// snapshot holds, which no committed log leads to: the model starts
	// from it (replay).
	synth *synthStore
}

// capture compacts the aging run s at its last revision, once its nodes
// agree, and takes its image. Every node must be up and a member: a fork
// starts the whole cluster. The nodes are frozen, as after the checks.
func (s *Sim) capture() (*image, error) {
	s.mu.Lock()
	for _, n := range s.nodes {
		if n.proc == nil || n.out() {
			s.mu.Unlock()
			return nil, fmt.Errorf("aging: node %d is down or out of the cluster at the end; a fork needs every node", n.id)
		}
	}
	p := s.nodes[0].proc
	rev := p.wl.turn.Rev
	s.mu.Unlock()
	// The image is compacted at the revision its store has: while the
	// nodes run, a lease may expire and its keys go, a revision more, and
	// the compaction goes again at that one, until the revision holds.
	held := false
	for try := 0; try < 10 && !held; try++ {
		s.thaw()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := pb.NewKVClient(p.wl.conn).Compact(ctx, &pb.CompactionRequest{Revision: rev, Physical: true})
		cancel()
		// The load may have compacted at rev already, and nothing came
		// after: the checks below hold as for a compaction just made.
		if err != nil && !errors.Is(err, rpctypes.ErrGRPCCompacted) {
			return nil, fmt.Errorf("aging: compacting at revision %d: %w", rev, err)
		}
		// The nodes agree again once each applied the compaction.
		agreed := false
		for range 100 {
			if err := s.freeze(); err != nil {
				return nil, err
			}
			if a, _ := disagree(s.nodes); a == nil {
				agreed = true
				break
			}
			s.thaw()
			time.Sleep(100 * time.Millisecond)
		}
		if !agreed {
			return nil, fmt.Errorf("aging: the nodes disagree 10 s after the compaction at revision %d:\n%s", rev, s.report())
		}
		s.mu.Lock()
		now := p.wl.turn.Rev
		s.mu.Unlock()
		held = now == rev
		rev = now
	}
	if !held {
		return nil, fmt.Errorf("aging: the revision moved past every compaction in 10 tries, now %d", rev)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	applied := s.nodes[0].proc.wl.turn.Applied
	ents, err := s.committed(applied)
	if err != nil {
		return nil, err
	}
	var ops []proto.Message
	for _, e := range ents {
		o, err := wlOps(e.GetData())
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", e.GetIndex(), err)
		}
		ops = append(ops, o...)
	}
	model, log, _, err := replay(nil, ops)
	if err != nil {
		return nil, err
	}
	all, err := model.Range(&pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		return nil, err
	}
	if model.Rev() != rev {
		return nil, fmt.Errorf("aging: the committed log ends at revision %d, the store was compacted at %d", model.Rev(), rev)
	}
	start := &history.Start{Rev: model.Rev(), Leases: aliveLeases(log)}
	for _, kv := range all.Kvs {
		start.KVs = append(start.KVs, frometcd.KeyValue(kv))
	}
	// The forks take the aging run's certificate and kept pages, and
	// nothing else of it, so that the run itself can go.
	src := &Sim{pages: s.pages, chunks: s.chunks}
	src.net.ca, src.net.cert, src.net.signer, src.net.hosts, src.net.expires, src.net.valid = s.net.ca, s.net.cert, s.net.signer, s.net.hosts, s.net.expires, s.net.valid
	img := &image{
		src: src, log: s.log.clone(), ckpts: maps.Clone(s.ckpts), maxLogged: s.maxLogged, applied: applied,
		expiredAt: beforeFork(s.expiredAt), revokedAt: beforeFork(s.revokedAt), ops: len(log), start: start,
	}
	for _, n := range s.nodes {
		img.disks = append(img.disks, n.disk.Durable())
		img.ids = append(img.ids, n.id)
	}
	return img, nil
}

// beforeFork is at, the times the aging run saw leases expire or go, at a
// time before any of its forks': the times are on the aging run's clock,
// and a fork's runs on its own, which under the race detector starts over
// (synctest's bubble).
func beforeFork(at map[int64]int64) map[int64]int64 {
	out := make(map[int64]int64, len(at))
	for id := range at {
		out[id] = math.MinInt64
	}
	return out
}

// fork makes s start from img: its nodes from copies of the image's
// disks, and its record from the image's. Nothing runs yet.
func (s *Sim) fork(img *image) {
	for i, n := range s.nodes {
		n.disk = img.disks[i].Durable()
		n.disk.Capacity = s.diskCapacity()
		n.id = img.ids[i]
		s.traceDisk(n)
	}
	s.log, s.ckpts = img.log.clone(), maps.Clone(img.ckpts)
	s.maxLogged, s.applied = img.maxLogged, img.applied
	s.expiredAt, s.revokedAt = maps.Clone(img.expiredAt), maps.Clone(img.revokedAt)
	s.startRev, s.synth = img.start.Rev, img.synth
}

// clone is a copy of l that its owner can add to without changing l: the
// records are copied, and the bytes l holds stay where they are.
func (l *entryLog) clone() *entryLog {
	return &entryLog{from: l.from, recs: slices.Clone(l.recs), set: slices.Clone(l.set), data: l.data.clone()}
}

// agedRequest is one of the calls a fork makes on the image's store
// besides the small keys' load: a read of every key, a delete of all the
// keys of one of the image's prefixes, or a revoke of one of its leases.
// The lock is held.
func (s *Sim) agedRequest() proto.Message {
	st := s.cfg.from.start
	switch p := s.rng.IntN(10); {
	case p < 4 || len(st.KVs) == 0:
		return &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}
	case p < 7 || len(st.Leases) == 0:
		key := st.KVs[s.rng.IntN(len(st.KVs))].Key
		// The key's parent: an object's prefix of its resource and owner.
		prefix := key[:strings.LastIndexByte(key, '/')+1]
		return &pb.DeleteRangeRequest{Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)), PrevKv: s.rng.IntN(2) == 0}
	default:
		return &pb.LeaseRevokeRequest{ID: st.Leases[s.rng.IntN(len(st.Leases))]}
	}
}
