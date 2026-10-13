package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

// userHZ is the unit of the CPU times in /proc/<pid>/stat. Linux fixes it at
// 100 for user space on every architecture this runs on.
const userHZ = 100

// procCPU returns the user plus system CPU time used so far by process pid,
// including its exited threads.
func procCPU(pid int) (time.Duration, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// The command name in field 2 may hold spaces, so count from its ')'.
	_, rest, ok := bytes.CutLast(b, []byte(")"))
	if !ok {
		return 0, fmt.Errorf("pid %d: malformed stat", pid)
	}
	f := bytes.Fields(rest)
	// f[0] is field 3 (state); utime and stime are fields 14 and 15.
	if len(f) < 13 {
		return 0, fmt.Errorf("pid %d: short stat", pid)
	}
	var ticks int64
	for _, s := range f[11:13] {
		n, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("pid %d: %w", pid, err)
		}
		ticks += n
	}
	return time.Duration(ticks) * time.Second / userHZ, nil
}

// selfCPU returns the user plus system CPU time of this process.
func selfCPU() (time.Duration, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}

// cpuSample is the CPU time of the servers and of this process at one moment.
type cpuSample struct {
	at      int64
	servers []time.Duration
	self    time.Duration
	machine machineCPU
}

type machineCPU struct {
	busy, total, iowait, steal int64
}

// machineCPUTicks samples the aggregate CPU line. Guest time is already
// included in user/nice. Busy excludes idle, iowait and stolen time.
func machineCPUTicks() (machineCPU, error) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return machineCPU{}, err
	}
	line, _, _ := bytes.Cut(b, []byte("\n"))
	f := bytes.Fields(line)
	if len(f) < 9 || string(f[0]) != "cpu" {
		return machineCPU{}, fmt.Errorf("malformed /proc/stat")
	}
	var ticks [8]int64
	for i := range ticks {
		ticks[i], err = strconv.ParseInt(string(f[i+1]), 10, 64)
		if err != nil {
			return machineCPU{}, err
		}
	}
	var m machineCPU
	for _, t := range ticks {
		m.total += t
	}
	m.busy = ticks[0] + ticks[1] + ticks[2] + ticks[5] + ticks[6]
	m.iowait, m.steal = ticks[4], ticks[7]
	return m, nil
}

func sampleCPU(pids []int) (cpuSample, error) {
	s := cpuSample{at: now(), servers: make([]time.Duration, len(pids))}
	for i, pid := range pids {
		d, err := procCPU(pid)
		if err != nil {
			return s, err
		}
		s.servers[i] = d
	}
	var err error
	s.self, err = selfCPU()
	if err != nil {
		return s, err
	}
	s.machine, err = machineCPUTicks()
	return s, err
}
