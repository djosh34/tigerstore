package sim

import (
	"math/rand/v2"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestKubeShortListsAfterFailedCreates fixes the outcome of the initial
// creates instead of relying on a native synctest schedule (#517). Three
// of each client's eight batches commit; the others fail without writing.
// The load moves past those failed batches, so no resource can have more
// than 24 keys. Its short profile must still read successful later pages.
// Real MVCC Range answers drive the real load's continuation logic. This
// isolates the workload shape, not the cluster's response to the faults.
func TestKubeShortListsAfterFailedCreates(t *testing.T) {
	records, err := newRecordLog()
	if err != nil {
		t.Fatal(err)
	}
	s := &Sim{
		cfg: kubeConfig(1, false), rng: rand.New(rand.NewPCG(1, 2)),
		t0: time.Now(), records: records,
	}
	store := mvcc.New(&mvcc.Heap{})
	// Expiry is not part of this fixture. Accept the load's event lease.
	store.LeaseExists = func(int64) bool { return true }
	later, first, more, largest := 0, 0, 0, int64(0)
	for id, k := range s.newKubeLoad() {
		c := &client{id: id, sim: s, node: -1, kube: k}
		batches := 0
		for k.created < len(k.all) {
			req := c.kubeRequest()
			r := &Record{Client: id, req: req}
			switch req := req.(type) {
			case *pb.LeaseGrantRequest:
				r.Resp = &pb.LeaseGrantResponse{Header: &pb.ResponseHeader{Revision: store.Rev()}, ID: int64(id + 1), TTL: req.TTL}
			case *pb.TxnRequest:
				batches++
				if batches <= 3 {
					r.Resp, r.Err = store.Txn(req)
					if r.Err != nil {
						t.Fatal(r.Err)
					}
				} else {
					r.Err = rpctypes.ErrGRPCNoLeader
				}
			default:
				t.Fatalf("initial create: unexpected request %T", req)
			}
			c.kubeDone(r)
		}
		// Ask once for every group, without a random choice or more writes.
		for _, prefix := range k.prefixes {
			k.list = &kubeList{from: prefix, end: prefixEnd(prefix)}
			var revision int64
			for k.list != nil {
				req := c.kubeRequest().(*pb.RangeRequest)
				resp, err := store.Range(req)
				if err != nil {
					t.Fatal(err)
				}
				if req.Revision == 0 {
					first++
					largest = max(largest, resp.Count)
					revision = resp.Header.Revision
					if resp.More {
						more++
					}
				} else {
					later++
					if req.Revision != revision {
						t.Fatalf("later page at %d, want first revision %d", req.Revision, revision)
					}
				}
				c.kubeDone(&Record{Client: id, req: req, Resp: resp})
			}
		}
	}
	t.Logf("limit %d, %d committed keys, %d first pages, %d with More, largest group %d, %d successful later pages", s.listPage(), store.Counts().Keys, first, more, largest, later)
	if later == 0 {
		t.Fatal("short load did no successful later LIST page after failed creates")
	}
}
