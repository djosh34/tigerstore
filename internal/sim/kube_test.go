package sim

import (
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// kubeConfig is a run of the Kubernetes-shaped load (kube.go) under every
// fault of DefaultConfig, by 16 clients, 3 of them clients of etcd's Go
// client, on segments of 4 MiB, with a disk budget of 8 KiB an object and
// 16 MiB at least, and a disk of twice that. The long profile has 10,000
// objects for 4 virtual hours, with a compaction every 5 minutes, and
// its faults twice as far apart as a default seed's: at the default
// rates, 10,000 objects leave the cluster serving one call in ten, at
// half of them about half the calls, and ten times further apart the
// seed does not fit in a runner's memory. The short one has 1,000
// objects for 2 minutes, with a compaction every 20 s.
func kubeConfig(seed uint64, long bool) Config {
	cfg := DefaultConfig(seed)
	cfg.Clients, cfg.V3Clients = 13, 3
	cfg.Objects, cfg.Duration, cfg.CompactEvery = 1000, 2*60*Second, 20*Second
	if long {
		cfg.Objects, cfg.Duration, cfg.CompactEvery = 10_000, 4*3600*Second, 5*60*Second
		spreadFaults(&cfg.Faults, 2)
	}
	cfg.SegmentBytes = 4 << 20
	cfg.Faults.DiskBudget = max(16<<20, int64(cfg.Objects)*8<<10)
	cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	return cfg
}

// spreadFaults makes the faults that come at random times come k times
// further apart.
func spreadFaults(f *Faults, k int64) {
	for _, every := range []*int64{
		&f.CrashEvery, &f.LinkEvery, &f.DiskErrorEvery, &f.StallEvery, &f.FSDownEvery, &f.DiskFullEvery,
		&f.LinkStallEvery, &f.PauseEvery, &f.HalfOpenEvery, &f.GrayEvery, &f.SlowWatcherEvery, &f.NetemEvery, &f.ResetEvery,
	} {
		*every *= k
	}
}

// TestKubernetesLoad runs the Kubernetes-shaped load of cmd/soak under
// every fault, with every checker: kubeConfig's short profile on 2 seeds
// from seed 1, or, with SIM_LONG=1, as the nightly kube-sim workflow
// sets, its long one. Each seed must have compacted, paged through a
// list, had a lease expire and had progress requests answered; in a long
// one, each fault must have taken effect too. It skips under the race
// detector (#439).
func TestKubernetesLoad(t *testing.T) {
	if simrun.Race {
		// Natively a run can hang when a TLS alert waits for a write lock
		// and the synctest clock stops (#439).
		t.Skip("the Kubernetes-shaped load runs under gosim (#439)")
	}
	long := simrun.Long()
	simrun.Seeds(t, 1, 2, func(t *testing.T, seed uint64) Stats {
		res := run(t, kubeConfig(seed, long))
		checkers(t, res)
		st := res.Stats
		// A compaction whose answer a fault lost is often in the log.
		compactions, pages, answers := 0, 0, 0
		for _, op := range res.Log {
			if _, ok := op.(*pb.CompactionRequest); ok {
				compactions++
			}
		}
		for _, r := range res.History {
			if req, ok := r.req.(*pb.RangeRequest); ok && r.Err == nil && req.Limit > 0 && req.Revision > 0 {
				pages++
			}
		}
		for _, w := range res.Watches {
			for _, r := range w.Responses {
				if r.Resp.WatchId == progressWatchID && len(r.Resp.Events) == 0 {
					answers++
				}
			}
		}
		if compactions == 0 || st.Revokes == 0 || pages == 0 || answers == 0 {
			t.Fatalf("seed %d did too little to test anything: %d compactions in the log, %d revokes, %d later pages of lists, %d answers to progress requests", seed, compactions, st.Revokes, pages, answers)
		}
		if took := faultsTaken(st); long && slices.Contains(slices.Collect(maps.Values(took)), false) {
			var missed []string
			for name, ok := range took {
				if !ok {
					missed = append(missed, name)
				}
			}
			slices.Sort(missed)
			t.Fatalf("seed %d: these faults never took effect: %s", seed, strings.Join(missed, ", "))
		}
		t.Logf("seed %d: %+v, %d committed writes, %d compactions in the log, %d later pages of lists, %d answers to progress requests", seed, st, len(res.Log), compactions, pages, answers)
		return st
	}, nil)
}

// TestKubeValuesUnique makes the calls of the Kubernetes-shaped load,
// first creates included, with every call answered as a success, and
// requires every value they put to be unique, as checkLog needs to tell
// the writes apart, and every object to be created first.
func TestKubeValuesUnique(t *testing.T) {
	s := &Sim{cfg: kubeConfig(1, false), rng: rand.New(rand.NewPCG(1, 2)), t0: time.Now()}
	for i, k := range s.newKubeLoad() {
		s.clients = append(s.clients, &client{id: i, sim: s, node: -1, kube: k})
	}
	seen := map[string]bool{}
	var rev int64
	for range 2000 {
		for _, c := range s.clients {
			r := &Record{Client: c.id, req: c.request()}
			for _, v := range putValues(r.req) {
				if seen[v] {
					t.Fatalf("client %d put %.40q twice", c.id, v)
				}
				seen[v] = true
			}
			rev++
			h := &pb.ResponseHeader{Revision: rev}
			var resp proto.Message
			switch req := r.req.(type) {
			case *pb.LeaseGrantRequest:
				resp = &pb.LeaseGrantResponse{Header: h, ID: rev, TTL: req.TTL}
			case *pb.TxnRequest:
				resp = &pb.TxnResponse{Header: h, Succeeded: true}
			case *pb.RangeRequest:
				resp = &pb.RangeResponse{Header: h}
			default:
				t.Fatalf("unexpected request %T", r.req)
			}
			c.done(r, resp, nil)
		}
	}
	for _, c := range s.clients {
		if c.kube.created != len(c.kube.all) {
			t.Fatalf("client %d created %d of its %d objects first", c.id, c.kube.created, len(c.kube.all))
		}
	}
	if len(seen) < 10_000 {
		t.Fatalf("only %d values", len(seen))
	}
}

// TestLeasesExpireByTTL checks leasesExpire against each lease's own TTL:
// a lease of 120 s granted 30 s before the end may still live then, but
// not one of 4 s granted then, nor one of 120 s granted 300 s before; and
// a lease of 120 s may not expire 100 s after its grant.
func TestLeasesExpireByTTL(t *testing.T) {
	end := 1000 * Second
	grant := func(id, ttl, at int64) *Record {
		return &Record{Call: at, Return: at + Millisecond, req: &pb.LeaseGrantRequest{TTL: ttl}, Resp: &pb.LeaseGrantResponse{ID: id, TTL: ttl}}
	}
	h := []*Record{grant(1, 120, end-30*Second), grant(2, 4, end-30*Second), grant(3, 120, end-300*Second), grant(4, 120, 0)}
	for _, c := range []struct {
		name    string
		alive   []int64
		expired int64
		ok      bool
	}{
		{"120 s lease 30 s old lives", []int64{1}, 0, true},
		{"4 s lease 30 s old lives", []int64{2}, 0, false},
		{"120 s lease 300 s old lives", []int64{3}, 0, false},
		{"120 s lease expires after 121 s", nil, 121 * Second, true},
		{"120 s lease expires after 100 s", nil, 100 * Second, false},
	} {
		at, order := map[int64]int64{}, []int64(nil)
		if c.expired > 0 {
			at[4], order = c.expired, []int64{4}
		}
		err := leasesExpire(h, at, order, c.alive, end, 50)
		if (err == nil) != c.ok {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}
