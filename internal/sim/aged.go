package sim

// Research #449, exp branch only: age a cluster once, then start many
// short runs (forks) from its disks.
//
// An aging run is a long run of the Kubernetes-shaped load. When it ends,
// capture keeps an image: each node's durable disk, the simulator's record
// of the committed log, the load's objects as their owners last saw them,
// and the committed log turned into a prefix of acknowledged calls (the
// base). A fork is a new run whose nodes start from copies of the image's
// disks, as after the whole cluster lost power, with its own seed, faults
// and clients. Its record starts as the image's, so every entry its nodes
// replay or load from a checkpoint is checked against the aging run. Its
// history starts with the base, so every checker sees a history that
// starts from the empty store as it needs, and the fork's calls follow.

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// image is what an aging run leaves for its forks.
type image struct {
	disks []*simfs.Disk
	ids   []uint64
	// damage is what the aging run knows of the damage it left on each
	// disk, which the fork carries on.
	damage    []damage
	log       map[uint64]logRec
	ckpts     map[uint64]ckptRec
	maxLogged uint64
	applied   uint64
	// base is the committed log as acknowledged calls of one client, in
	// commit order, before time 0.
	base []*Record
	// expiredAt is when each lease the aging run saw expire first did:
	// a fork's nodes apply those expiries again as they replay their logs.
	expiredAt map[int64]int64
	// rev is the revision at the end, and compacted the last compaction.
	rev, compacted int64
	// clients are the load's clients as the aging run left them.
	clients []*kubeClient
	// entries and bytes describe the size of the image.
	entries int
	bytes   int
	// files counts each node's segments, checkpoints and packs.
	files []string
}

// damage is a node's knowledge of the damage on its disk.
type damage struct {
	corrupted, rotted bool
	rotFile           string
	rotEntry, rotSeg  uint64
}

// baseClient is the client of the base's calls.
const baseClient = 1 << 20

// capture takes the image of a run that ended with its nodes frozen and
// agreeing.
func (s *Sim) capture() (*image, error) {
	last, err := s.converged()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if n.out() || n.proc == nil {
			return nil, fmt.Errorf("node %d is not fit to fork from", n.id)
		}
	}
	applied := last.proc.core.Applied()
	ents, err := s.committed(applied)
	if err != nil {
		return nil, err
	}
	var ops []proto.Message
	for _, e := range ents {
		if len(e.GetData()) == 0 || e.GetType() != raftpb.EntryNormal {
			continue
		}
		if err := core.DecodeBatch(e.GetData(), func(_, _, _ uint64, op proto.Message) error {
			if _, ok := op.(*pb.Member); !ok {
				ops = append(ops, op)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	img := &image{
		log: s.log, ckpts: s.ckpts, maxLogged: s.maxLogged, applied: applied,
		rev: last.proc.core.Store().Rev(), entries: len(ops), expiredAt: maps.Clone(s.expiredAt),
	}
	img.base, img.compacted, err = baseRecords(ops)
	if err != nil {
		return nil, err
	}
	for _, n := range s.nodes {
		d := n.disk.Durable()
		img.disks = append(img.disks, d)
		img.ids = append(img.ids, n.id)
		img.damage = append(img.damage, damage{n.corrupted, n.rotted, n.rotFile, n.rotEntry, n.rotSeg})
		img.bytes += d.Bytes()
		var segs, ckpts, packs int
		for _, name := range d.DurableNames() {
			switch {
			case isSegment(name):
				segs++
			case isCheckpoint(name):
				ckpts++
			case strings.HasPrefix(name, "pack-"):
				packs++
			}
		}
		img.files = append(img.files, fmt.Sprintf("node %d: %d bytes, %d segments, %d checkpoints, %d packs", n.id, d.Bytes(), segs, ckpts, packs))
	}
	for _, c := range s.clients {
		img.clients = append(img.clients, c.kube)
	}
	return img, nil
}

// baseRecords turns a committed log into acknowledged calls, one after
// the other, each with the answer the state gives when it applies, as
// replay reads the log: an expiry that applied is the LeaseRevoke it
// amounts to. Keepalives and lease checkpoints, which no client of the
// load sends, are left out; so are alarms.
func baseRecords(log []proto.Message) ([]*Record, int64, error) {
	st := core.NewState(core.MinLeaseTTL(core.DefaultConfig(1, nil)), &mvcc.Heap{})
	var out []*Record
	var compacted int64
	for i, op := range log {
		switch o := op.(type) {
		case *pb.CompactionRequest:
			// The state compacts nothing, as in replay; the answer follows
			// the compactions before.
			r := &Record{req: op}
			switch {
			case o.Revision <= compacted:
				r.Err = rpctypes.ErrGRPCCompacted
			case o.Revision > st.Store().Rev():
				r.Err = rpctypes.ErrGRPCFutureRev
			default:
				compacted = o.Revision
				r.Resp = &pb.CompactionResponse{Header: &pb.ResponseHeader{Revision: st.Store().Rev()}}
			}
			out = append(out, r)
			continue
		case *pb.LeaseKeepAliveRequest, *pb.LeaseCheckpointRequest, *pb.AlarmRequest:
			if _, err := st.Apply(op); err != nil && !isRejection(err) {
				return nil, 0, fmt.Errorf("base write %d: %w", i, err)
			}
			continue
		}
		e, expiry := op.(*pb.LeaseCheckpoint)
		resp, err := st.Apply(op)
		switch {
		case err != nil && !isRejection(err):
			return nil, 0, fmt.Errorf("base write %d: %w", i, err)
		case errors.Is(err, rpctypes.ErrGRPCNoSpace):
			continue
		case expiry && resp == nil:
			continue
		case expiry:
			op = &pb.LeaseRevokeRequest{ID: e.ID}
		}
		r := &Record{req: op, Resp: resp, Err: err}
		if err != nil {
			r.Resp = nil
		}
		out = append(out, r)
	}
	// One client, one call after the other, all before time 0.
	for i, r := range out {
		r.Client = baseClient
		r.Call = -2*int64(len(out)-i) - 1
		r.Return = r.Call + 1
	}
	return out, compacted, nil
}

// fork makes s start from img: its nodes from copies of the image's
// disks, its record from the image's, its history from the base, and its
// load from the image's objects. The lock is not needed: nothing runs yet.
func (s *Sim) fork(img *image) {
	for i, n := range s.nodes {
		n.disk = img.disks[i].Durable()
		n.disk.Capacity = s.cfg.Faults.DiskCapacity
		n.id = img.ids[i]
		dm := img.damage[i]
		n.corrupted, n.rotted, n.rotFile, n.rotEntry, n.rotSeg = dm.corrupted, dm.rotted, dm.rotFile, dm.rotEntry, dm.rotSeg
		s.traceDisk(n)
	}
	s.log = maps.Clone(img.log)
	s.ckpts = maps.Clone(img.ckpts)
	s.maxLogged, s.applied = img.maxLogged, img.applied
	s.history = append([]*Record(nil), img.base...)
	s.expiredAt = maps.Clone(img.expiredAt)
}

// forkLoad is the image's load, copied so that a fork changes only its
// own: every object as its owner last saw it, every first create tried,
// each client's count of values kept so that values stay unique, and no
// lease, which the fork's clients grant again.
func (s *Sim) forkLoad(img *image) []*kubeClient {
	s.kube = &kubeLoad{byKey: map[string]*kubeObject{}, compactAt: s.cfg.CompactEvery, compactTo: img.compacted}
	s.values = newValues()
	var out []*kubeClient
	for _, k := range img.clients {
		kc := &kubeClient{prefixes: k.prefixes, created: k.created, values: k.values}
		copies := map[*kubeObject]*kubeObject{}
		for _, o := range k.all {
			c := *o
			copies[o] = &c
			kc.all = append(kc.all, &c)
			s.kube.byKey[string(c.key)] = &c
		}
		for _, g := range k.groups {
			var cg []*kubeObject
			for _, o := range g {
				cg = append(cg, copies[o])
			}
			kc.groups = append(kc.groups, cg)
		}
		out = append(out, kc)
	}
	return out
}
