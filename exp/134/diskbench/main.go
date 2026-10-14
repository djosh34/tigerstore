// Question (#134): how many bytes per second can the runner's disk take as a
// sequential log, with each kind of durable write, at group sizes from 4 KiB
// to 4 MiB, with one writer and with three writers sharing the disk?
//
// Throwaway. Run on a disposable runner with barriers on:
//
//	go run ./exp/134/diskbench -dir /path/on/disk -dev sda -out cells.jsonl
//
// Every cell prints one JSON line: what the program wrote and what
// /proc/diskstats saw the device do in the same window.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type disk struct {
	writes, sectors, writeMs, ioMs, flushes, flushMs uint64
}

func readDisk(dev string) disk {
	b, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 14 || f[2] != dev {
			continue
		}
		u := func(i int) uint64 {
			if i >= len(f) {
				return 0
			}
			v, _ := strconv.ParseUint(f[i], 10, 64)
			return v
		}
		// fields after the name: 1 reads, ... 5 writes, 7 sectors written,
		// 8 ms writing, 10 io ms, 16 flushes, 17 ms flushing.
		return disk{writes: u(7), sectors: u(9), writeMs: u(10), ioMs: u(12), flushes: u(18), flushMs: u(19)}
	}
	panic("no device " + dev)
}

type cell struct {
	Mode, Layout          string
	Group, Writers, Rep   int
	Seconds               float64
	Ops                   int
	AppBytes              int64
	MiBps, OpsPerS        float64
	OpP50, OpP99, OpMean  float64 // write+sync, us
	SyncP50, SyncP99      float64 // the sync call alone, us
	WriteP50, WriteP99    float64 // the write call alone, us
	DevBytes              int64
	DevWrites, DevFlushes uint64
	DevUtil               float64 // io ticks / wall
	DevAmp                float64 // device bytes / app bytes
	DevWriteMsPerS        float64
	DevFlushMsPerS        float64
}

const fileSize = 1 << 30

func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	slices.Sort(v)
	return v[min(len(v)-1, int(q*float64(len(v))))]
}

func aligned(n int) []byte {
	b := make([]byte, n+4096)
	off := 4096 - int(uintptr(unsafe.Pointer(&b[0]))&4095)
	b = b[off : off+n]
	for i := range b {
		b[i] = byte(i*7 + 13)
	}
	return b
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	must(err)
	must(d.Sync())
	d.Close()
}

func prepare(dir, layout string, i int) string {
	name := filepath.Join(dir, fmt.Sprintf("w%d.%s", i, layout))
	switch layout {
	case "grow":
		os.Remove(name)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		must(err)
		must(f.Sync())
		f.Close()
	case "falloc":
		os.Remove(name)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		must(err)
		must(syscall.Fallocate(int(f.Fd()), 0, 0, fileSize))
		must(f.Sync())
		f.Close()
	case "zeroed":
		if st, err := os.Stat(name); err == nil && st.Size() == fileSize {
			break
		}
		f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		must(err)
		z := make([]byte, 4<<20)
		for off := 0; off < fileSize; off += len(z) {
			_, err := f.Write(z)
			must(err)
		}
		must(f.Sync())
		f.Close()
	}
	syncDir(dir)
	return name
}

func run(dir, dev, mode, layout string, group, writers, rep int, dur time.Duration) cell {
	names := make([]string, writers)
	for i := range writers {
		names[i] = prepare(dir, layout, i)
	}
	flags := os.O_WRONLY
	direct := strings.HasPrefix(mode, "direct")
	if direct {
		flags |= syscall.O_DIRECT
	}
	if strings.HasSuffix(mode, "dsync") && !strings.HasSuffix(mode, "fdatasync") {
		flags |= syscall.O_DSYNC
	}
	files := make([]*os.File, writers)
	for i, n := range names {
		f, err := os.OpenFile(n, flags, 0)
		must(err)
		files[i] = f
	}
	exec.Command("sync").Run()
	time.Sleep(300 * time.Millisecond)

	type res struct {
		op, sy, wr []float64
		bytes      int64
	}
	out := make([]res, writers)
	var wg sync.WaitGroup
	startGate := make(chan struct{})
	d0 := readDisk(dev)
	var t0 time.Time
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runtime.LockOSThread()
			buf := aligned(group)
			fd := int(files[i].Fd())
			var off int64
			r := &out[i]
			<-startGate
			end := t0.Add(dur)
			for {
				a := time.Now()
				if !a.Before(end) {
					break
				}
				if layout != "grow" && off+int64(group) > fileSize {
					off = 0
				}
				_, err := syscall.Pwrite(fd, buf, off)
				must(err)
				b := time.Now()
				switch {
				case strings.HasSuffix(mode, "fdatasync"):
					must(syscall.Fdatasync(fd))
				case strings.HasSuffix(mode, "fsync"):
					must(syscall.Fsync(fd))
				}
				c := time.Now()
				off += int64(group)
				r.bytes += int64(group)
				r.wr = append(r.wr, float64(b.Sub(a).Nanoseconds())/1e3)
				r.sy = append(r.sy, float64(c.Sub(b).Nanoseconds())/1e3)
				r.op = append(r.op, float64(c.Sub(a).Nanoseconds())/1e3)
			}
		}(i)
	}
	t0 = time.Now()
	close(startGate)
	wg.Wait()
	el := time.Since(t0).Seconds()
	d1 := readDisk(dev)
	for _, f := range files {
		f.Close()
	}
	var op, sy, wr []float64
	var bytes int64
	for _, r := range out {
		op = append(op, r.op...)
		sy = append(sy, r.sy...)
		wr = append(wr, r.wr...)
		bytes += r.bytes
	}
	var sum float64
	for _, v := range op {
		sum += v
	}
	c := cell{Mode: mode, Layout: layout, Group: group, Writers: writers, Rep: rep, Seconds: el,
		Ops: len(op), AppBytes: bytes, MiBps: float64(bytes) / el / (1 << 20), OpsPerS: float64(len(op)) / el,
		OpMean: sum / float64(max(1, len(op))),
		OpP50:  pct(op, .5), OpP99: pct(op, .99), SyncP50: pct(sy, .5), SyncP99: pct(sy, .99),
		WriteP50: pct(wr, .5), WriteP99: pct(wr, .99),
		DevBytes: int64(d1.sectors-d0.sectors) * 512, DevWrites: d1.writes - d0.writes, DevFlushes: d1.flushes - d0.flushes,
		DevUtil:        float64(d1.ioMs-d0.ioMs) / 1e3 / el,
		DevWriteMsPerS: float64(d1.writeMs-d0.writeMs) / el, DevFlushMsPerS: float64(d1.flushMs-d0.flushMs) / el,
	}
	c.DevAmp = float64(c.DevBytes) / float64(max(1, bytes))
	if layout != "zeroed" {
		for _, n := range names {
			os.Remove(n)
		}
		syncDir(dir)
		time.Sleep(time.Second) // the mount discards freed blocks
	}
	return c
}

func main() {
	dir := flag.String("dir", "", "directory on the disk under test")
	dev := flag.String("dev", "sda", "device name in /proc/diskstats")
	out := flag.String("out", "cells.jsonl", "output")
	dur := flag.Duration("dur", 2*time.Second, "time per cell")
	reps := flag.Int("reps", 3, "repetitions")
	modes := flag.String("modes", "buf-fsync,buf-fdatasync,buf-dsync,direct-fdatasync,direct-dsync,direct-nosync", "")
	layouts := flag.String("layouts", "grow,falloc,zeroed", "")
	sizes := flag.String("sizes", "4,16,64,256,1024,4096", "group sizes in KiB")
	writers := flag.String("writers", "1,3", "")
	flag.Parse()
	f, err := os.Create(*out)
	must(err)
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for rep := 1; rep <= *reps; rep++ {
		for _, layout := range strings.Split(*layouts, ",") {
			for _, mode := range strings.Split(*modes, ",") {
				for _, s := range strings.Split(*sizes, ",") {
					kib, _ := strconv.Atoi(s)
					for _, ws := range strings.Split(*writers, ",") {
						n, _ := strconv.Atoi(ws)
						c := run(*dir, *dev, mode, layout, kib<<10, n, rep, *dur)
						must(enc.Encode(c))
						w.Flush()
						fmt.Printf("rep%d %-6s %-16s %5dK w%d %8.1f MiB/s %8.0f ops/s op p50 %7.0f p99 %7.0f sync p50 %7.0f us dev %6.1f MiB/s amp %.3f util %.2f flush/s %.0f\n",
							rep, layout, mode, kib, n, c.MiBps, c.OpsPerS, c.OpP50, c.OpP99, c.SyncP50,
							float64(c.DevBytes)/c.Seconds/(1<<20), c.DevAmp, c.DevUtil, float64(c.DevFlushes)/c.Seconds)
					}
				}
			}
		}
	}
	w.Flush()
	f.Close()
}
