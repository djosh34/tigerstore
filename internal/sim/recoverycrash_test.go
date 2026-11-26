package sim

import (
	"context"
	"errors"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// armedStart makes a run of one node whose disk holds a file, as a restart
// finds its log, and a start of it with a crash in recovery armed, as
// start does. It returns with the lock held.
func armedStart(t *testing.T) (*Sim, *proc) {
	t.Helper()
	cfg := DefaultConfig(1)
	cfg.Faults.RecoveryCrash = true
	s, err := newSim(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n := s.nodes[0]
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
	if !s.armRecoveryCrash(n) {
		s.mu.Unlock()
		t.Fatal("the start armed no crash in recovery")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &proc{n: n, ctx: ctx, cancel: cancel, wl: &wlProc{armed: true}}
	n.proc = p
	return s, p
}

// endRun ends s, whose lock is held, and waits for its goroutines, as the
// restart a crash scheduled.
func endRun(s *Sim) {
	s.stop()
	s.mu.Unlock()
	s.wg.Wait()
}

// write is a start's first write to n's disk.
func write(n *node) error {
	f, err := n.disk.Create("run.tmp")
	if err != nil {
		return err
	}
	_, err = f.Write([]byte("run"))
	return err
}

// TestADownDuringAnArmedStartLeavesTheNextStartClean checks #860: a start
// with a crash in recovery armed (armRecoveryCrash) that a crash at a step
// or a kill takes down before its recovery ends leaves no fault on its
// disk. The next start writes freely, and arms a crash of its own.
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
			s, p := armedStart(t)
			n := p.n
			c.down(s, p)
			up := n.proc != nil
			left := n.disk.Fault != nil
			// The next start: its first write, then, no recovery crashed
			// yet, a crash of its own armed.
			wrote := write(n)
			rearmed := s.armRecoveryCrash(n)
			n.disk.Fault = nil
			endRun(s)

			if up {
				t.Fatal("the node is still up")
			}
			if left {
				t.Error("the armed crash outlived its start: the disk's fault is still set")
			}
			if wrote != nil {
				t.Errorf("the next start's first write failed: %v", wrote)
			}
			if !rearmed {
				t.Error("the next start armed no crash of its own")
			}
		})
	}
}

// TestADiskFaultArmedDuringAStartOutlivesIt checks that a disk fault armed
// on a starting node (armDiskFault), which replaces the start's armed
// crash in recovery, is not the start's to clear: after a kill, the next
// write still gets its EIO (#861's review).
func TestADiskFaultArmedDuringAStartOutlivesIt(t *testing.T) {
	s, p := armedStart(t)
	n := p.n
	s.armDiskFault(n, 1) // EIO on the next write
	s.kill(n)
	wrote := write(n)
	ioErrors := s.stats.IOErrors
	endRun(s)

	if !errors.Is(wrote, simfs.ErrIO) {
		t.Errorf("the next write gave %v, want the armed EIO", wrote)
	}
	if ioErrors != 1 {
		t.Errorf("IOErrors is %d, want 1", ioErrors)
	}
}
