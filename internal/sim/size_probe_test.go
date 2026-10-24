//go:build sim

package sim

// Throwaway capacity experiment for #401, not a correctness test. It keeps
// the simulator's histories, traces, watches, disks and default fault mix.
import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/k8ssize"
	"github.com/jellevandenhooff/gosim"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func sizeProbe(t *testing.T, objects, seconds int, seed uint64) {
	// simrun.Bubble normally caps virtual time at an hour. Do not use it here.
	gosim.SetSimulationTimeout(24 * time.Hour)
	cfg := DefaultConfig(seed)
	cfg.Duration = int64(seconds) * Second
	cfg.Clients, cfg.V3Clients = 16, 0
	cfg.probeWait = 40 * Millisecond // at most 400 offered calls/s; faults reduce it
	cfg.SegmentBytes = 4 << 20
	// The normal 32 KiB/128 KiB budgets cannot hold even 1,000 objects.
	cfg.Faults.DiskBudget, cfg.Faults.DiskCapacity = 0, 0
	bins := make([]int, objects)
	var loaded int
	var phaseAt int64
	leaseAt := map[*client]int64{}
	cfg.probeRequest = func(c *client) proto.Message {
		s := c.sim // request holds s.mu
		i := s.rng.IntN(objects)
		k := fmt.Appendf(nil, "k%08d", i)
		p := s.rng.IntN(100)
		switch {
		case p < 25:
			return &pb.RangeRequest{Key: k}
		case p < 35:
			return &pb.RangeRequest{Key: k, RangeEnd: []byte("l"), Limit: 100}
		case p < 90: // 40% update, 15% create (same fixed object pool)
			b := k8ssize.Buckets[bins[i]]
			val := make([]byte, b.Size)
			for j := range val {
				val[j] = byte('a' + (j+c.n)%26)
			}
			put := &pb.PutRequest{Key: k, Value: val}
			if b.Resource == "events" && len(c.leases) > 0 {
				put.Lease = c.leases[len(c.leases)-1]
			}
			// Reuse an event lease for a minute, with the soak's TTL of 120 s.
			if b.Resource == "events" && (len(c.leases) == 0 || s.now()-leaseAt[c] >= 60*Second) {
				leaseAt[c] = s.now()
				return &pb.LeaseGrantRequest{TTL: 120}
			}
			return put
		default:
			return &pb.DeleteRangeRequest{Key: k}
		}
	}
	cfg.probeBefore = func(s *Sim) error {
		for i := range bins {
			bins[i] = k8ssize.Draw(s.rng, "")
		}
		// Periodic console logs are streamed by the translated binary itself.
		go func() {
			for {
				time.Sleep(10 * time.Second)
				s.mu.Lock()
				phase := "preload"
				elapsed := int64(0)
				if phaseAt > 0 {
					phase = "churn"
					elapsed = s.now() - phaseAt
				}
				fmt.Printf("SIZE_PROGRESS phase=%s sim_s=%.3f churn_s=%.3f loaded=%d calls=%d ok=%d compact=%d kills=%d history=%d trace=%d\n", phase, float64(s.now())/float64(Second), float64(elapsed)/float64(Second), loaded, len(s.history), s.stats.OK, s.stats.Compactions, s.stats.Kills, len(s.history), len(s.trace))
				s.mu.Unlock()
			}
		}()
		var wg sync.WaitGroup
		for worker := range 16 {
			wg.Go(func() {
				for i := worker; i < objects; i += 16 {
					b := k8ssize.Buckets[bins[i]]
					req := &pb.PutRequest{Key: fmt.Appendf(nil, "k%08d", i), Value: make([]byte, b.Size)}
					for {
						_, err := s.call(s.nodes[worker%3], req, 3*Second)
						if err == nil {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					s.mu.Lock()
					loaded++
					s.mu.Unlock()
				}
			})
		}
		wg.Wait()
		s.mu.Lock()
		phaseAt = s.now()
		s.healAt += phaseAt
		s.stopAt += phaseAt
		s.endAt += phaseAt
		fmt.Printf("SIZE_PRELOADED objects=%d sim_s=%.3f\n", objects, float64(phaseAt)/float64(Second))
		s.mu.Unlock()
		// Compact every five minutes, one interval behind, as the soak does.
		s.wg.Go(func() {
			var prev int64
			for s.sleep(300 * Second) {
				s.mu.Lock()
				if s.now() >= s.stopAt {
					s.mu.Unlock()
					return
				}
				i := s.leaderIdx()
				rev := int64(0)
				for _, c := range s.clients {
					rev = max(rev, c.rev)
				}
				s.mu.Unlock()
				if i >= 0 && prev > 0 {
					_, err := s.call(s.nodes[i], &pb.CompactionRequest{Revision: prev}, 3*Second)
					if err == nil {
						s.mu.Lock()
						s.stats.Compactions++
						s.mu.Unlock()
					}
				}
				prev = rev
			}
		})
		return nil
	}
	s, err := newSim(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("SIZE_START objects=%d seconds=%d seed=%d\n", objects, seconds, seed)
	err = s.run()
	s.mu.Lock()
	fmt.Printf("SIZE_DONE objects=%d target_s=%d sim_s=%.3f churn_s=%.3f loaded=%d calls=%d ok=%d compact=%d kills=%d err=%v\n", objects, seconds, float64(s.now())/float64(Second), float64(s.now()-phaseAt)/float64(Second), loaded, len(s.history), s.stats.OK, s.stats.Compactions, s.stats.Kills, err)
	s.mu.Unlock()
	s.shutdown()
	if err != nil {
		t.Fatal(err)
	}
}
