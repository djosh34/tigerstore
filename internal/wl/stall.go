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
// for. The loop reads them each tick, and a slow turn logs what moved
// since. Each field is a running total; -1 where the system does not tell.
type stallStats struct {
	at                       time.Time
	process, loop, runWait   int64 // ns
	minflt, majflt           int64
	vcsw, ivcsw              int64
	allocStall, compactStall int64
	directScan, thpFallback  int64
	pgmajfault               int64
}

// stallStats reads the totals; it runs on the loop's thread.
func (n *node) stallStats(now time.Time) stallStats {
	s := stallStats{at: now, runWait: -1, allocStall: -1, compactStall: -1, directScan: -1, thpFallback: -1, pgmajfault: -1}
	s.process, s.loop = sysCPU()
	s.minflt, s.majflt, s.vcsw, s.ivcsw = sysThread()
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
// scans, huge-page faults that fell back to small pages, and major faults.
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
	return fmt.Sprintf("since %v: loop on a CPU %d ms, runnable and waiting %s ms, others' CPU %d ms; loop faults %d minor %d major, switches %d voluntary %d involuntary; kernel: alloc stalls %s, compact stalls %s, direct scans %s, huge-page fallbacks %s, major faults %s",
		s.at.Sub(s0.at).Round(time.Millisecond), ms(s.loop-s0.loop), wait, ms(s.process-s0.process-(s.loop-s0.loop)),
		s.minflt-s0.minflt, s.majflt-s0.majflt, s.vcsw-s0.vcsw, s.ivcsw-s0.ivcsw,
		d(s.allocStall, s0.allocStall), d(s.compactStall, s0.compactStall), d(s.directScan, s0.directScan), d(s.thpFallback, s0.thpFallback), d(s.pgmajfault, s0.pgmajfault))
}
