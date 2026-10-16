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

// diskWritten returns the bytes written so far to the block device or
// partition named dev in /proc/diskstats, such as sda1.
func diskWritten(dev string) (int64, error) {
	b, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return 0, err
	}
	return parseDiskstats(b, dev)
}

func parseDiskstats(b []byte, dev string) (int64, error) {
	for line := range bytes.SplitSeq(b, []byte("\n")) {
		f := bytes.Fields(line)
		// Field 3 is the name and field 10 the 512-byte sectors written.
		if len(f) < 10 || string(f[2]) != dev {
			continue
		}
		n, err := strconv.ParseInt(string(f[9]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("diskstats %s: %w", dev, err)
		}
		return n * 512, nil
	}
	return 0, fmt.Errorf("no device %q in /proc/diskstats", dev)
}

// cpuSample is the CPU time of the servers and of this process, and the
// bytes written to the disk under test, at one moment.
type cpuSample struct {
	at        int64
	servers   []time.Duration
	self      time.Duration
	diskBytes int64
}

func sampleCPU(pids []int, disk string) (cpuSample, error) {
	s := cpuSample{at: now(), servers: make([]time.Duration, len(pids))}
	for i, pid := range pids {
		d, err := procCPU(pid)
		if err != nil {
			return s, err
		}
		s.servers[i] = d
	}
	if disk != "" {
		var err error
		if s.diskBytes, err = diskWritten(disk); err != nil {
			return s, err
		}
	}
	var err error
	s.self, err = selfCPU()
	return s, err
}
