package sim

import (
	"bytes"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

// The Kubernetes-shaped load (Config.Objects) is cmd/soak's, in the
// simulator. Each client owns Objects/clients objects under
// /registry/<resource>/sim-c<client>/, with values of the sizes measured
// on a Kubernetes cluster (internal/k8ssize), and makes the calls the
// apiserver makes on them. It first creates its objects, a few in each
// Txn, then picks from the soak's mix: 25% gets, 10% paginated lists of
// one of its groups, 40% updates, 15% creates and 10% deletes; a create
// of an object it saw present is an update, and an update or delete of
// one it saw absent is a create. Creates, updates and deletes are Txns
// that compare the create or mod revision the client last saw, with a
// read of the object as the else branch, from which it learns the
// object's state when the compare fails, as the apiserver does after a
// conflict. Events go on a lease of kubeLeaseTTL, granted again after a
// minute or 1,000 uses, and vanish when it expires. Client 0 compacts
// every CompactEvery to the newest revision any client had seen when the
// interval before ended, as the apiserver's compactor does, and tries a
// compaction that failed again at its next call. One watch
// client watches each resource prefix, with progress notifications and
// progress requests (watch.go).

// kubeLeaseTTL is the TTL of the leases events go on, as cmd/soak's.
const kubeLeaseTTL = 120

const (
	// kubeBatch is how many objects a create of the first ones puts in
	// one Txn: with values up to 7 KB, a Txn stays well below the
	// simulator's tiny limit of entries waiting to apply (tinyLimits).
	kubeBatch = 8
	// kubePage is the limit of one page of a list.
	kubePage = 25
	// kubeLeaseEvery and kubeLeaseUses are when a client grants a new
	// lease for its events, as the apiserver's lease reuse does.
	kubeLeaseEvery = 60 * Second
	kubeLeaseUses  = 1000
	// kubeThink is how long a client waits after an answer, so that 16
	// clients make up to 400 calls a second, as cmd/soak's default rate;
	// after a failure it waits up to kubeBackoff, as a client backs off.
	kubeThink   = 40 * Millisecond
	kubeBackoff = Second
)

// The soak's mix, in percent: get, list, update, create, delete.
const (
	kubeGets    = 25
	kubeLists   = kubeGets + 10
	kubeUpdates = kubeLists + 40
	kubeCreates = kubeUpdates + 15
)

// kubeLoad is the state of the Kubernetes-shaped load shared by the
// clients.
type kubeLoad struct {
	byKey map[string]*kubeObject
	// compactAt is when client 0 compacts next, to compactTo, and
	// compacting the revision of a compaction it has yet to make.
	compactAt, compactTo, compacting int64
}

// kubeObject is one object. Only its owner writes it.
type kubeObject struct {
	key []byte
	// bin is the size row of k8ssize its values are drawn from.
	bin int
	// leased is set for events, which go on the owner's lease.
	leased bool
	// mod is the object's mod revision as its owner last saw it, or 0
	// if it last saw it absent or never saw it.
	mod int64
}

// kubeClient is one client's part of the load.
type kubeClient struct {
	// groups are the client's objects of each resource, under prefixes.
	groups   [][]*kubeObject
	prefixes [][]byte
	// all are its objects in the order it first creates them, of which it
	// has tried to create the first created.
	all     []*kubeObject
	created int
	// values counts the values it made, which makes each unique.
	values int
	// lease is the lease its events go on, granted at leaseAt, with
	// leaseUses writes on it; 0 when it has none.
	lease     int64
	leaseAt   int64
	leaseUses int
	// list is the list it pages through, if any.
	list *kubeList
}

// kubeList is a paginated list: the next page starts at from, and every
// page after the first reads at the first one's revision.
type kubeList struct {
	from, end []byte
	rev       int64
}

// newKubeLoad makes the objects of cfg.Objects and gives each client its
// share. The lock is held.
func (s *Sim) newKubeLoad() []*kubeClient {
	clients := s.cfg.Clients + s.cfg.V3Clients
	s.kube = &kubeLoad{byKey: map[string]*kubeObject{}, compactAt: s.cfg.CompactEvery}
	s.values = newValues()
	var out []*kubeClient
	for id := range clients {
		kc := &kubeClient{}
		byResource := map[string]int{}
		for i := range s.cfg.Objects / clients {
			bin := k8ssize.Draw(s.rng, "")
			r := k8ssize.Buckets[bin].Resource
			g, ok := byResource[r]
			if !ok {
				g = len(kc.groups)
				byResource[r] = g
				kc.groups = append(kc.groups, nil)
				kc.prefixes = append(kc.prefixes, fmt.Appendf(nil, "/registry/%s/sim-c%02d/", r, id))
			}
			o := &kubeObject{key: fmt.Appendf(nil, "%sobj-%05d", kc.prefixes[g], i), bin: bin, leased: r == "events"}
			kc.groups[g] = append(kc.groups[g], o)
			kc.all = append(kc.all, o)
			s.kube.byKey[string(o.key)] = o
		}
		out = append(out, kc)
	}
	return out
}

// kubeRequest picks the client's next call of the load. The lock is held.
func (c *client) kubeRequest() proto.Message {
	s, k := c.sim, c.kube
	if l := s.kube; c.id == 0 {
		if s.now() >= l.compactAt {
			l.compacting = max(l.compacting, l.compactTo)
			for _, o := range s.clients {
				l.compactTo = max(l.compactTo, o.rev)
			}
			l.compactAt += s.cfg.CompactEvery
			mark("progress virtual=%ds history=%d", s.now()/Second, len(s.history))
		}
		if l.compacting > 0 {
			return &pb.CompactionRequest{Revision: l.compacting}
		}
	}
	if l := k.list; l != nil {
		return &pb.RangeRequest{Key: l.from, RangeEnd: l.end, Limit: kubePage, Revision: l.rev}
	}
	if k.created < len(k.all) {
		batch := k.all[k.created:min(k.created+kubeBatch, len(k.all))]
		if needsLease(batch) && !c.kubeLeased() {
			return &pb.LeaseGrantRequest{TTL: kubeLeaseTTL}
		}
		k.created += len(batch)
		txn := &pb.TxnRequest{}
		for _, o := range batch {
			txn.Compare = append(txn.Compare, createdNever(o))
			txn.Success = append(txn.Success, putOp(c.kubePut(o)))
		}
		return txn
	}
	g := s.rng.IntN(len(k.groups))
	p := s.rng.IntN(100)
	if p >= kubeGets && p < kubeLists {
		end := prefixEnd(k.prefixes[g])
		k.list = &kubeList{from: k.prefixes[g], end: end}
		return &pb.RangeRequest{Key: k.prefixes[g], RangeEnd: end, Limit: kubePage}
	}
	o := k.groups[g][s.rng.IntN(len(k.groups[g]))]
	switch {
	case p < kubeGets:
		return &pb.RangeRequest{Key: o.key}
	case o.leased && !c.kubeLeased() && (o.mod == 0 || p < kubeCreates):
		return &pb.LeaseGrantRequest{TTL: kubeLeaseTTL}
	case o.mod == 0:
		return &pb.TxnRequest{
			Compare: []*pb.Compare{createdNever(o)},
			Success: []*pb.RequestOp{putOp(c.kubePut(o))},
			Failure: []*pb.RequestOp{getOp(o)},
		}
	case p < kubeCreates:
		return &pb.TxnRequest{
			Compare: []*pb.Compare{modIs(o)},
			Success: []*pb.RequestOp{putOp(c.kubePut(o))},
			Failure: []*pb.RequestOp{getOp(o)},
		}
	default:
		return &pb.TxnRequest{
			Compare: []*pb.Compare{modIs(o)},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: o.key}}}},
			Failure: []*pb.RequestOp{getOp(o)},
		}
	}
}

// kubeLeased reports whether the client has a lease its events may still
// go on. The lock is held.
func (c *client) kubeLeased() bool {
	k := c.kube
	return k.lease != 0 && c.sim.now()-k.leaseAt < kubeLeaseEvery && k.leaseUses < kubeLeaseUses
}

func needsLease(objs []*kubeObject) bool {
	for _, o := range objs {
		if o.leased {
			return true
		}
	}
	return false
}

// kubePut is the put of a new value of o, on the client's lease for an
// event. Its value starts with the client and a count of its values, so
// that every value of a run is unique, as checkLog needs, and is filled
// up to a size drawn from o's resource. The lock is held.
func (c *client) kubePut(o *kubeObject) *pb.PutRequest {
	k := c.kube
	k.values++
	o.bin = k8ssize.Draw(c.sim.rng, k8ssize.Buckets[o.bin].Resource)
	size := k8ssize.Buckets[o.bin].Size
	v := fmt.Appendf(make([]byte, 0, max(size, 64)), "c%d-%d %s ", c.id, k.values, o.key)
	for len(v) < size {
		v = append(v, byte('a'+len(v)%26))
	}
	c.sim.values.add(v)
	put := &pb.PutRequest{Key: o.key, Value: v}
	if o.leased {
		put.Lease = k.lease
		k.leaseUses++
	}
	return put
}

func createdNever(o *kubeObject) *pb.Compare {
	return &pb.Compare{Key: o.key, Target: pb.Compare_CREATE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_CreateRevision{CreateRevision: 0}}
}

func modIs(o *kubeObject) *pb.Compare {
	return &pb.Compare{Key: o.key, Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: o.mod}}
}

func putOp(p *pb.PutRequest) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: p}}
}

func getOp(o *kubeObject) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: o.key}}}
}

// prefixEnd is the end of the range of keys with prefix p, which ends in
// a byte below 0xff.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	end[len(end)-1]++
	return end
}

// kubeDone learns what a call of the load shows of the client's objects
// and lease. The lock is held.
func (c *client) kubeDone(r *Record) {
	k := c.kube
	if _, ok := r.req.(*pb.CompactionRequest); ok && (r.Err == nil || isRejection(r.Err)) {
		// Done, or compacted already, as by an earlier try whose answer
		// was lost.
		c.sim.kube.compacting = 0
	}
	if r.Err != nil {
		if errors.Is(r.Err, rpctypes.ErrGRPCLeaseNotFound) {
			k.lease = 0
		}
		if _, ok := r.req.(*pb.RangeRequest); ok {
			// A page that failed ends its list, as a page at a compacted
			// revision must.
			k.list = nil
		}
		return
	}
	switch resp := r.Resp.(type) {
	case *pb.LeaseGrantResponse:
		k.lease, k.leaseAt, k.leaseUses = resp.ID, r.Call, 0
	case *pb.RangeResponse:
		c.kubeSaw(r.req.(*pb.RangeRequest), resp)
		if k.list == nil {
			return
		}
		if !resp.More || len(resp.Kvs) == 0 {
			k.list = nil
			return
		}
		k.list.from = append(bytes.Clone(resp.Kvs[len(resp.Kvs)-1].Key), 0)
		if k.list.rev == 0 {
			// The later pages read at the first one's revision; their
			// headers hold the current one.
			k.list.rev = resp.Header.Revision
		}
	case *pb.TxnResponse:
		req := r.req.(*pb.TxnRequest)
		if !resp.Succeeded {
			for i, op := range resp.Responses {
				if rr := op.GetResponseRange(); rr != nil {
					c.kubeSaw(req.Failure[i].GetRequestRange(), rr)
				}
			}
			return
		}
		for _, op := range req.Success {
			if p := op.GetRequestPut(); p != nil {
				c.sim.kube.byKey[string(p.Key)].mod = resp.Header.Revision
			}
			if d := op.GetRequestDeleteRange(); d != nil {
				c.sim.kube.byKey[string(d.Key)].mod = 0
			}
		}
	}
}

// kubeSaw learns the state of the objects a read returned, and of the one
// a read of a single key found absent.
func (c *client) kubeSaw(req *pb.RangeRequest, resp *pb.RangeResponse) {
	byKey := c.sim.kube.byKey
	if len(req.RangeEnd) == 0 && len(resp.Kvs) == 0 {
		if o, ok := byKey[string(req.Key)]; ok {
			o.mod = 0
		}
	}
	for _, kv := range resp.Kvs {
		if o, ok := byKey[string(kv.Key)]; ok {
			o.mod = kv.ModRevision
		}
	}
}
