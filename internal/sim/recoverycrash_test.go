package sim

import (
	"context"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestADownDuringAnArmedStartLeavesTheNextStartClean checks #860: a start
// with a crash in recovery armed (armRecoveryCrash) that a crash at a step
// or a kill takes down before its recovery ends leaves no fault on its
// disk. The next start writes freely, or arms a crash of its own.
func TestADownDuringAnArmedStartLeavesTheNextStartClean(t *testing.T) {
	for _, c := range []struct {
		name string
		down func(s *Sim, p *proc)
	}{
		{"a crash at a step", func(s *Sim, p *proc) {
			// As the start's sync of a dir, until the step crash's draw
			// (one in four) takes it.
			s.stepCrashDue = true
			for s.afterCall(p, simfs.OpSyncDir, "") == nil {
			}
		}},
		{"a kill", func(s *Sim, p *proc) { s.kill(p.n) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig(1)
			cfg.Faults.RecoveryCrash = true
			s, err := newSim(cfg)
			if err != nil {
				t.Fatal(err)
			}
			n := s.nodes[0]
			// A file on the disk, as a restart finds its log.
			f, err := n.disk.Create("log")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte("entries")); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := n.disk.SyncDir(); err != nil {
				t.Fatal(err)
			}

			s.mu.Lock()
			// The start, as start does it: the crash armed, then the
			// process up with armed set.
			if !s.armRecoveryCrash(n) {
				s.mu.Unlock()
				t.Fatal("the start armed no crash in recovery")
			}
			ctx, cancel := context.WithCancel(context.Background())
			p := &proc{n: n, ctx: ctx, cancel: cancel, wl: &wlProc{armed: true}}
			n.proc = p
			c.down(s, p)
			if n.proc != nil {
				s.mu.Unlock()
				t.Fatal("the node is still up")
			}
			left := n.disk.Fault != nil
			// The next start: no recovery crashed yet, so it arms its own.
			rearmed := s.armRecoveryCrash(n)
			n.disk.Fault = nil
			s.stop()
			s.mu.Unlock()
			s.wg.Wait()

			if left {
				t.Error("the armed crash outlived its start: the disk's fault is still set")
			}
			if !rearmed {
				t.Error("the next start armed no crash of its own")
			}
			g, err := n.disk.Create("run.tmp")
			if err != nil {
				t.Fatalf("the next start's first write failed: %v", err)
			}
			if _, err := g.Write([]byte("run")); err != nil {
				t.Fatalf("the next start's first write failed: %v", err)
			}
		})
	}
}
