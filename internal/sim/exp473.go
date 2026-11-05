package sim

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// exp473 counts, for research #473 (exp branch only), the real time spent
// in parts of the run that hold the lock and never yield, and the disk
// reads of nodes that start and of nodes that run.
var exp473 struct {
	startNs, recoveryNs, imageNs, loseNs, ckptNs       int64
	startReads, startReadBytes, runReads, runReadBytes int64
	window                                             int64
}

// exp473Timed adds the real time f takes to acc.
func exp473Timed(acc *int64, f func()) {
	t0 := simrun.RealNanotime()
	f()
	*acc += simrun.RealNanotime() - t0
}

func exp473Now() int64 { return simrun.RealNanotime() }

// exp473Since adds the real time since t0 to acc.
func exp473Since(acc *int64, t0 int64) { *acc += simrun.RealNanotime() - t0 }

// exp473Read counts a read of n bytes by p.
func exp473Read(p *proc, n int) {
	if p.n.proc == p {
		exp473.runReads++
		exp473.runReadBytes += int64(n)
	} else {
		exp473.startReads++
		exp473.startReadBytes += int64(n)
	}
}

// exp473Mark writes what grows with the run's age, and starts a new
// window of the CPU profile every 30 virtual minutes. It reads without the
// lock: gosim runs one goroutine at a time and this does not yield, so
// taking the lock would only change the schedule.
func (s *Sim) exp473Mark() {
	virtual := s.now() / Second
	var b strings.Builder
	fmt.Fprintf(&b, "exp473 virtual=%d history=%d log=%d base=%d checkpoints=%d", virtual, len(s.history), len(s.log), len(s.base), len(s.checkpoints))
	var ckptDisk int
	for _, c := range s.checkpoints {
		for _, d := range c.disks {
			ckptDisk += d.Bytes()
		}
	}
	fmt.Fprintf(&b, " ckpt-disk=%d", ckptDisk)
	st := s.stats
	fmt.Fprintf(&b, " crashes=%d rcrashes=%d kills=%d plosses=%d diskfaults=%d ckpts=%d snapshots=%d reloads=%d packs=%d messages=%d ok=%d failed=%d",
		st.Crashes, st.RecoveryCrashes, st.Kills, st.PowerLosses, st.DiskFaults, st.Checkpoints, st.Snapshots, st.Reloads, st.Packs, st.Messages, st.OK, st.Failed)
	c := core.Exp473
	fmt.Fprintf(&b, " env=%q leaseckpts=%d leaseckptcfg=%d", os.Getenv("EXP473_LEASECKPT"), c.LeaseCkpts, c.LeaseCkptCfg)
	fmt.Fprintf(&b, " starts=%d ckptloads=%d ckptkeys=%d ckptvers=%d replayed=%d tail=%d", c.Starts, c.CkptLoads, c.CkptKeys, c.CkptVersions, c.Replayed, c.Tail)
	f := simfs.Exp473
	fmt.Fprintf(&b, " files=%d filesB=%d durables=%d durableB=%d copies=%d copyB=%d", f.Files, f.FilesBytes, f.Durables, f.DurableBytes, f.Copies, f.CopyBytes)
	e := exp473
	fmt.Fprintf(&b, " startNs=%d recoveryNs=%d imageNs=%d loseNs=%d ckptNs=%d sreads=%d sreadB=%d rreads=%d rreadB=%d",
		e.startNs, e.recoveryNs, e.imageNs, e.loseNs, e.ckptNs, e.startReads, e.startReadBytes, e.runReads, e.runReadBytes)
	for _, n := range s.nodes {
		segs, packs, ckpts := 0, 0, 0
		names := n.disk.DurableNames()
		for _, name := range names {
			switch {
			case isSegment(name):
				segs++
			case strings.HasPrefix(name, "pack-"):
				packs++
			case isCheckpoint(name):
				ckpts++
			}
		}
		keys, vers, leases, empty := -1, -1, -1, -1
		if n.proc != nil && n.proc.core != nil {
			keys, vers = n.proc.core.Exp473Index()
			leases, empty = n.proc.core.Exp473Leases()
			fmt.Fprintf(&b, " s%d=%s", n.id, n.proc.core.Exp473LeaseState())
		}
		fmt.Fprintf(&b, " n%d=disk:%d,files:%d,segs:%d,packs:%d,ckpts:%d,keys:%d,vers:%d,leases:%d,emptyleases:%d", n.id, n.disk.Bytes(), len(names), segs, packs, ckpts, keys, vers, leases, empty)
	}
	mark("%s", b.String())
	if w := virtual / 1800; w > exp473.window {
		exp473.window = w
		simrun.ProfileNext(fmt.Sprintf("v%03d", w*30))
	}
}

// exp473Config applies the switches of the counterfactual runs:
// EXP473_HASH=nosort folds the leases into the state hash without sorting
// them, and EXP473_LEASECKPT sets the lease checkpoint interval in ticks.
func exp473Config(cfg *core.Config) {
	core.Exp473NoSort = os.Getenv("EXP473_HASH") == "nosort"
	if t, err := strconv.Atoi(os.Getenv("EXP473_LEASECKPT")); err == nil && t > 0 {
		cfg.LeaseCheckpointTicks = t
	}
}
