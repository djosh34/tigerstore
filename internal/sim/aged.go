package sim

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
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
	// src is the aging run, whose certificate and kept pages its forks
	// share: the image's disks keep their pages there.
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
	s.thaw()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := pb.NewKVClient(p.wl.conn).Compact(ctx, &pb.CompactionRequest{Revision: rev, Physical: true})
	cancel()
	if err != nil {
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
	model, log, _, err := replay(ops)
	if err != nil {
		return nil, err
	}
	all, err := model.Range(&pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		return nil, err
	}
	start := &history.Start{Rev: model.Rev(), Leases: aliveLeases(log)}
	for _, kv := range all.Kvs {
		start.KVs = append(start.KVs, frometcd.KeyValue(kv))
	}
	img := &image{
		src: s, log: s.log.clone(), ckpts: maps.Clone(s.ckpts), maxLogged: s.maxLogged, applied: applied,
		expiredAt: maps.Clone(s.expiredAt), revokedAt: maps.Clone(s.revokedAt), ops: len(log), start: start,
	}
	for _, n := range s.nodes {
		img.disks = append(img.disks, n.disk.Durable())
		img.ids = append(img.ids, n.id)
	}
	return img, nil
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
	s.startRev = img.start.Rev
}

// clone is a copy of l that its owner can add to without changing l: the
// records are copied, and the bytes l holds stay where they are.
func (l *entryLog) clone() *entryLog {
	return &entryLog{recs: slices.Clone(l.recs), set: slices.Clone(l.set), data: l.data.clone()}
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
