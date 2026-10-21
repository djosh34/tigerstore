package sim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestGRPCReloadFailsFast puts a Range in flight during a real snapshot
// install, then cuts the follower's peer links at the completed install.
// Its client link stays up. The interrupted call must fail, not wait for
// its three-second deadline. The diagnosis also records every RPC attempt.
func TestGRPCReloadFailsFast(t *testing.T) {
	if simrun.Drive(t, 365) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		var s *Sim
		var target atomic.Int64
		target.Store(-1)
		var armed atomic.Bool
		var once sync.Once
		installing, resume, installed := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
		unblock := func() { once.Do(func() { close(resume) }) }
		defer unblock()
		var cutMask uint64
		cfg := DefaultConfig(365)
		cfg.Faults = Faults{}
		cfg.turnHook = func(p *proc, _ *core.Node, eff core.Effects, err error) {
			if int64(p.n.idx) != target.Load() {
				return
			}
			for _, j := range eff.Persist {
				if j.Install != nil && armed.CompareAndSwap(false, true) {
					close(installing)
					<-resume
				}
			}
			if errors.Is(err, core.ErrInstalled) {
				s.mu.Lock()
				s.setLinks(cutMask)
				s.mu.Unlock()
				installed <- struct{}{}
			}
		}
		attempts := make(chan error, 16)
		cfg.attemptHook = func(err error) { attempts <- err }
		var err error
		s, err = newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			unblock()
			report := s.report()
			s.shutdown()
			res := &Result{Report: report, Trace: s.trace, nodes: s.nodes, Stats: s.stats}
			simrun.Artifact("hashes", []byte(fmt.Sprintf("seed 365: %s\n", hashes(res))))
			save(t, replayCommand(t.Name(), 365), res)
			if t.Failed() {
				t.Logf("%s", report)
			}
		}()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		s.mu.Unlock()
		if lead < 0 {
			t.Fatal("no leader after 3 s")
		}
		follower := (lead + 1) % 3
		c := &client{sim: s, node: follower}
		if err := c.newV3(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := c.closeV3(); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.doV3(ctx, &pb.RangeRequest{Key: []byte("k"), Serializable: true}); err != nil {
			t.Fatal(err)
		}
		<-attempts
		s.mu.Lock()
		all := s.allLinks()
		cutMask = all
		bit := 0
		for i := range s.links {
			for j := range s.links[i] {
				if i == j {
					continue
				}
				if i == follower || j == follower {
					cutMask &^= 1 << bit
				}
				bit++
			}
		}
		s.setLinks(cutMask)
		s.mu.Unlock()
		for i := range 12 {
			if _, err := s.nodes[lead].proc.node.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "value-%d", i)}); err != nil {
				t.Fatal(err)
			}
		}
		target.Store(int64(follower))
		s.mu.Lock()
		s.setLinks(all)
		s.mu.Unlock()
		select {
		case <-installing:
		case <-time.After(2 * time.Second):
			t.Fatal("the behind follower did not start a snapshot install")
		}
		done := make(chan error, 1)
		callCtx, callCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer callCancel()
		started := s.now()
		go func() {
			_, err := c.doV3(callCtx, &pb.RangeRequest{Key: []byte("k")})
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
		unblock()
		select {
		case <-installed:
		case <-time.After(2 * time.Second):
			t.Fatal("the follower did not complete its snapshot install")
		}
		select {
		case err := <-attempts:
			t.Logf("first RPC attempt after install: %v", err)
			if !errors.Is(v3Error(rpctypes.Error(err)), rpctypes.ErrGRPCStopped) {
				t.Fatalf("first attempt did not fail with ErrGRPCStopped: %v", err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatal("first RPC attempt did not fail within 200 ms of the install")
		}
		select {
		case err := <-done:
			t.Logf("clientv3 Range returned after %s: %v", time.Duration(s.now()-started), err)
			if !errors.Is(v3Error(rpctypes.Error(err)), rpctypes.ErrGRPCStopped) {
				t.Fatalf("Range returned %v, want ErrGRPCStopped", err)
			}
		case <-time.After(200 * time.Millisecond):
			err := <-done
			t.Fatalf("first RPC failed promptly, but clientv3 Range returned only after %s: %v", time.Duration(s.now()-started), err)
		}
	})
}
