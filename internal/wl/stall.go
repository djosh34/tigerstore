package wl

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

// stallStats tell where a slow turn's time went when it was not the
// loop's own work (#704): the loop thread's CPU, its wait for a CPU while
// runnable, its page faults and context switches, the process's CPU, and
// the kernel's counters of reclaim and compaction that allocations waited
// for, and Go's collector. The loop reads them each tick, and a slow turn
// logs what moved since. Each field is a running total; -1 where the
// system does not tell.
type stallStats struct {
	at                       time.Time
	process, loop, runWait   int64 // ns
	minflt, majflt           int64
	vcsw, ivcsw              int64
	allocStall, compactStall int64
	directScan, thpFallback  int64
	pgmajfault               int64
	gc                       gcStats
}

// gcStats are Go's collector's running totals (sysEnv.goGC): its cycles,
// the CPU its mark assists took, the time goroutines waited on a
// sync.Mutex, and its stop-the-world pauses and the time each took to
// stop the world, as counts and sums.
type gcStats struct {
	cycles, assist, mutexWait int64 // assist and mutexWait in ns
	pauses, stopping          pauseTotal
}

// pauseTotal is a histogram of pauses as a count and a sum in ns, each
// pause taken at its bucket's middle, or at its lower bound for the last.
type pauseTotal struct{ n, ns int64 }

// stallStats reads the totals; it runs on the loop's thread.
func (n *node) stallStats(now time.Time) stallStats {
	s := stallStats{at: now, runWait: -1, allocStall: -1, compactStall: -1, directScan: -1, thpFallback: -1, pgmajfault: -1}
	s.process, s.loop = sysCPU()
	s.minflt, s.majflt, s.vcsw, s.ivcsw = sysThread()
	n.sys.goGC(&s.gc)
	// schedstat: time on a CPU, time runnable waiting for one, timeslices.
	if b, err := procFile("/proc/thread-self/schedstat"); err == nil {
		if f := bytes.Fields(b); len(f) >= 2 {
			if v, err := strconv.ParseInt(string(f[1]), 10, 64); err == nil {
				s.runWait = v
			}
		}
	}
	if b, err := procFile("/proc/vmstat"); err == nil {
		s.allocStall, s.compactStall, s.directScan, s.thpFallback, s.pgmajfault = 0, 0, 0, 0, 0
		for line := range bytes.Lines(b) {
			name, val, ok := bytes.Cut(bytes.TrimSpace(line), []byte(" "))
			if !ok {
				continue
			}
			v, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				continue
			}
			switch {
			case bytes.HasPrefix(name, []byte("allocstall_")):
				s.allocStall += v
			case string(name) == "compact_stall":
				s.compactStall = v
			case bytes.HasPrefix(name, []byte("pgscan_direct")):
				s.directScan += v
			case string(name) == "thp_fault_fallback":
				s.thpFallback = v
			case string(name) == "pgmajfault":
				s.pgmajfault = v
			}
		}
	}
	return s
}

// since says what moved from s0 to s: the loop thread's time on a CPU and
// waiting for one, the rest of the process's CPU, the loop thread's page
// faults and switches (voluntary ones are waits, involuntary ones a CPU
// taken away), and the kernel's reclaim and compaction stalls, direct
// scans, huge-page faults that fell back to small pages, and major faults,
// and Go's collections, mark assists, mutex waits and stop-the-world
// pauses.
func (s stallStats) since(s0 stallStats) string {
	ms := func(ns int64) int64 { return ns / 1e6 }
	d := func(a, b int64) string {
		if a < 0 || b < 0 {
			return "?"
		}
		return strconv.FormatInt(a-b, 10)
	}
	wait := "?"
	if s.runWait >= 0 && s0.runWait >= 0 {
		wait = strconv.FormatInt(ms(s.runWait-s0.runWait), 10)
	}
	g, g0 := s.gc, s0.gc
	return fmt.Sprintf("since %v: loop on a CPU %d ms, runnable and waiting %s ms, others' CPU %d ms; loop faults %d minor %d major, switches %d voluntary %d involuntary; kernel: alloc stalls %s, compact stalls %s, direct scans %s, huge-page fallbacks %s, major faults %s; Go: collections %d, mark assists %d ms, mutex waits %d ms, stop-the-world pauses %d of about %d ms, stopping the world %d of about %d ms",
		s.at.Sub(s0.at).Round(time.Millisecond), ms(s.loop-s0.loop), wait, ms(s.process-s0.process-(s.loop-s0.loop)),
		s.minflt-s0.minflt, s.majflt-s0.majflt, s.vcsw-s0.vcsw, s.ivcsw-s0.ivcsw,
		d(s.allocStall, s0.allocStall), d(s.compactStall, s0.compactStall), d(s.directScan, s0.directScan), d(s.thpFallback, s0.thpFallback), d(s.pgmajfault, s0.pgmajfault),
		g.cycles-g0.cycles, ms(g.assist-g0.assist), ms(g.mutexWait-g0.mutexWait),
		g.pauses.n-g0.pauses.n, ms(g.pauses.ns-g0.pauses.ns), g.stopping.n-g0.stopping.n, ms(g.stopping.ns-g0.stopping.ns))
}
