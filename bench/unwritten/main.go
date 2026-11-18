// Command unwritten measures, on the runner's disk, what an O_DIRECT,
// O_DSYNC append costs into space fallocate left unwritten (as the log's
// segments are allocated ahead) against space written before: time per
// write and bytes the device wrote, with one writer and three at once.
// A measurement for #602's ceilings, never merged.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func sectorsWritten(dev string) int64 {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) > 9 && fs[2] == dev {
			v, _ := strconv.ParseInt(fs[9], 10, 64)
			return v
		}
	}
	return -1
}

func prepare(path string, total int64, prewrite bool) {
	_ = os.Remove(path)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	if prewrite {
		buf := make([]byte, 1<<20)
		for off := int64(0); off < total; off += int64(len(buf)) {
			if _, err := unix.Pwrite(fd, buf, off); err != nil {
				panic(err)
			}
		}
	} else if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, total); err != nil {
		panic(err)
	}
	if err := unix.Fsync(fd); err != nil {
		panic(err)
	}
	_ = unix.Close(fd)
}

func write(path string, writes, size int) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_DIRECT|unix.O_DSYNC, 0)
	if err != nil {
		panic(err)
	}
	raw := make([]byte, size+4096)
	off := int(uintptr(unsafe.Pointer(&raw[0])) & 4095)
	buf := raw[(4096-off)%4096:][:size]
	for i := range buf {
		buf[i] = byte(i)
	}
	for i := range writes {
		if _, err := unix.Pwrite(fd, buf, int64(i*size)); err != nil {
			panic(err)
		}
	}
	_ = unix.Close(fd)
}

func main() {
	dir, dev := os.Args[1], os.Args[2]
	for _, writers := range []int{1, 3} {
		for _, size := range []int{256 << 10, 1 << 20} {
			for _, prewrite := range []bool{false, true, false, true} {
				writes := (600 << 20) / size
				paths := make([]string, writers)
				for i := range paths {
					paths[i] = filepath.Join(dir, fmt.Sprintf("unwritten-%d", i))
					prepare(paths[i], int64(writes*size), prewrite)
				}
				_ = unix.Sync
				unix.Sync()
				before := sectorsWritten(dev)
				t := time.Now()
				var wg sync.WaitGroup
				for _, p := range paths {
					wg.Go(func() { write(p, writes, size) })
				}
				wg.Wait()
				d := time.Since(t)
				after := sectorsWritten(dev)
				data := int64(writers * writes * size)
				disk := (after - before) * 512
				fmt.Printf("writers %d, %4d KiB a write, written before %-5v: %6.0f us a write, %6.1f MiB/s together, device wrote %.3f bytes per byte\n",
					writers, size>>10, prewrite, float64(d.Microseconds())/float64(writes), float64(data)/d.Seconds()/(1<<20), float64(disk)/float64(data))
				for _, p := range paths {
					_ = os.Remove(p)
				}
			}
		}
	}
}
