package sim

import (
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/dataprobe"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// These parameters are compiled into each test so replay needs only its name
// and seed. One native driver process and one gosim run per cell.
func TestDataHeavy(t *testing.T)               { dataHeavy(t, 64, 0, 1) }
func TestDataHeavy_10000_0_0(t *testing.T)     { dataHeavy(t, 10000, 0, 0) }
func TestDataHeavy_30000_0_0(t *testing.T)     { dataHeavy(t, 30000, 0, 0) }
func TestDataHeavy_100000_0_0(t *testing.T)    { dataHeavy(t, 100000, 0, 0) }
func TestDataHeavy_1000_65536_0(t *testing.T)  { dataHeavy(t, 1000, 65536, 0) }
func TestDataHeavy_200_1048576_0(t *testing.T) { dataHeavy(t, 200, 1048576, 0) }
func TestDataHeavy_10000_0_20(t *testing.T)    { dataHeavy(t, 10000, 0, 20) }
func TestDataHeavy_1000_0_200(t *testing.T)    { dataHeavy(t, 1000, 0, 200) }

func dataHeavy(t *testing.T, objects, size, rounds int) {
	if simrun.Drive(t, 1) {
		return
	}
	dataprobe.Mark("load")
	cfg := kubeConfig(1, false)
	cfg.Objects, cfg.Clients, cfg.V3Clients = objects, 16, 0 // #440
	cfg.Duration, cfg.CompactEvery = 120*Second, 20*Second
	// DefaultConfig faults, unspread. Scale the disk budget for bytes as well
	// as objects, with space for ~3 copies of live data; capacity is 2x budget.
	cfg.Faults.DiskBudget = max(32<<20, int64(objects)*int64(max(size, 2048))*int64(rounds+1)*3)
	cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	cfg.dataLimits, cfg.probeValueBytes = true, size
	cfg.probePhase = dataprobe.Mark
	cfg.beforeSchedule = func(s *Sim) error { return heavyPreload(s, rounds) }
	var res *Result
	var err error
	simrun.BubbleFor(t, 24*time.Hour, func(*testing.T) {
		res, err = runInspect(cfg, func(s *Sim) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			n, err := s.converged()
			if err != nil {
				return err
			}
			st := n.proc.core.Store()
			var keys, liveBytes int64
			from := []byte{0}
			for {
				rr, err := st.Range(&pb.RangeRequest{Key: from, RangeEnd: []byte{0}, Limit: 128, KeysOnly: true})
				if err != nil {
					return err
				}
				for _, kv := range rr.Kvs {
					keys++
					refs := st.Refs(kv.Key)
					if len(refs) > 0 {
						liveBytes += int64(refs[len(refs)-1].Len)
					}
				}
				if !rr.More || len(rr.Kvs) == 0 {
					break
				}
				from = append(append([]byte(nil), rr.Kvs[len(rr.Kvs)-1].Key...), 0)
			}
			t.Logf("DATA449 final keys=%d live_value_bytes=%d revision=%d history=%d record_entries=%d", keys, liveBytes, st.Rev(), len(s.history), len(s.log))
			for _, n := range s.nodes {
				t.Logf("DATA449 node=%d disk_bytes=%d crash_image_bytes=%d", n.id, n.disk.Bytes(), imageBytes(n.crashImage))
			}
			var copies int
			for _, c := range s.checkpoints {
				for _, d := range c.disks {
					copies += d.Bytes()
				}
			}
			t.Logf("DATA449 checkpoint_copies=%d bytes=%d stats=%+v", len(s.checkpoints), copies, s.stats)
			dataprobe.Profile("sim-check")
			return nil
		})
	})
	dataprobe.Mark("hashes")
	if res != nil {
		t.Logf("DATA449 hashes %s", hashes(res))
	}
	dataprobe.Mark("independent")
	if err != nil {
		t.Fatal(err)
	}
	if err := independent(res); err != nil {
		t.Fatal(err)
	}
	dataprobe.Mark("done")
	dataprobe.Profile("done")
	t.Logf("DATA449 passed objects=%d size=%d overwrites=%d final_revision=%d log_ops=%d history=%d stats=%+v", objects, size, rounds, res.FinalRev, len(res.Log), len(res.History), res.Stats)
}

func imageBytes(m map[string][]byte) int {
	n := 0
	for _, b := range m {
		n += len(b)
	}
	return n
}

func heavyPreload(s *Sim, rounds int) error {
	s.mu.Lock()
	for id, k := range s.newKubeLoad() {
		s.clients = append(s.clients, &client{id: id, sim: s, node: -1, kube: k})
	}
	s.mu.Unlock()
	time.Sleep(2 * time.Second) // election, without any faults or clients
	var values, bytes int64
	for round := 0; round <= rounds; round++ {
		for _, c := range s.clients {
			for at := 0; at < len(c.kube.all); {
				s.mu.Lock()
				txn := &pb.TxnRequest{}
				start := at
				// Initial load uses the normal 128-op limit and the 1.5 MiB request
				// limit (64 KiB headroom for envelopes). Each overwrite is a separate
				// Txn, deliberately generating 200k actual revisions, not 200k/128.
				limit := 128
				if round > 0 {
					limit = 1
				}
				for at < len(c.kube.all) && at-start < limit {
					put := c.kubePut(c.kube.all[at])
					put.Lease = 0 // stable bootstrap objects, even Events; churn leases them
					op := putOp(put)
					if len(txn.Success) > 0 && proto.Size(txn)+proto.Size(op) > core.MaxRequestBytes-(64<<10) {
						break
					}
					txn.Success = append(txn.Success, op)
					values++
					bytes += int64(len(put.Value))
					at++
				}
				target := s.leaderIdx()
				if target < 0 {
					s.mu.Unlock()
					return fmt.Errorf("preload has no leader")
				}
				r := &Record{Client: c.id, Node: target, Call: s.now(), req: txn}
				s.mu.Unlock()
				resp, err := s.call(s.nodes[target], txn, 10*Second)
				s.mu.Lock()
				c.done(r, resp, err)
				s.mu.Unlock()
				// The independent format requires strictly separated calls.
				time.Sleep(time.Nanosecond)
				if err != nil {
					return fmt.Errorf("preload round %d at %d: %w", round, at, err)
				}
			}
			c.kube.created = len(c.kube.all)
		}
		if round == 0 || round == rounds || round%20 == 0 {
			dataprobe.Mark(fmt.Sprintf("loaded-round-%d", round))
			fmt.Printf("DATA449 loaded round=%d values=%d value_bytes=%d sim_ns=%d\n", round, values, bytes, s.now())
		}
	}
	s.mu.Lock()
	s.kube.compactAt = s.now() + s.cfg.CompactEvery
	s.mu.Unlock()
	dataprobe.Profile("loaded")
	return nil
}
