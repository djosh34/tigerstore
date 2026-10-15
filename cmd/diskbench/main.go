// Command diskbench measures the most a disk can take as a sequential log:
// O_DIRECT writes into a file that is already written, each followed by an
// fdatasync, so no write allocates blocks or commits a journal. This is the
// device ceiling of #134, which bench.yml divides by the device bytes per
// write.
//
//	diskbench -dir <dir on the disk> [-size 1GiB] [-duration 5s]
//
// It prints one JSON line per group size and a last line with the maximum.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type cell struct {
	Group   int     `json:"group_bytes"`
	Writes  int     `json:"writes"`
	MiBps   float64 `json:"mib_per_s"`
	Seconds float64 `json:"seconds"`
}

func main() {
	dir := flag.String("dir", "", "directory on the disk under test")
	size := flag.Int64("size", 1<<30, "file size in bytes")
	dur := flag.Duration("duration", 5*time.Second, "time per group size")
	flag.Parse()
	if err := run(*dir, *size, *dur); err != nil {
		fmt.Fprintln(os.Stderr, "diskbench:", err)
		os.Exit(1)
	}
}

func run(dir string, size int64, dur time.Duration) (err error) {
	if dir == "" {
		return errors.New("need -dir")
	}
	name := filepath.Join(dir, "diskbench.data")
	defer func() { err = errors.Join(err, os.Remove(name)) }()
	if err := fill(name, size); err != nil {
		return err
	}
	best := cell{}
	enc := json.NewEncoder(os.Stdout)
	for _, group := range []int{256 << 10, 1 << 20, 4 << 20} {
		c, err := measure(name, size, group, dur)
		if err != nil {
			return err
		}
		if err := enc.Encode(c); err != nil {
			return err
		}
		if c.MiBps > best.MiBps {
			best = c
		}
	}
	return enc.Encode(struct {
		Max float64 `json:"max_mib_per_s"`
	}{best.MiBps})
}

// fill writes size bytes of zeros to name and syncs it, so later writes go
// to allocated, written blocks.
func fill(name string, size int64) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	zeros := make([]byte, 4<<20)
	for off := int64(0); off < size && err == nil; off += int64(len(zeros)) {
		_, err = f.Write(zeros)
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

func measure(name string, size int64, group int, dur time.Duration) (c cell, err error) {
	fd, err := unix.Open(name, unix.O_WRONLY|unix.O_DIRECT, 0)
	if err != nil {
		return cell{}, fmt.Errorf("open %s with O_DIRECT: %w", name, err)
	}
	defer func() { err = errors.Join(err, unix.Close(fd)) }()
	buf := aligned(group)
	c = cell{Group: group}
	start := time.Now()
	var off int64
	for time.Since(start) < dur {
		if off+int64(group) > size {
			off = 0
		}
		n, err := unix.Pwrite(fd, buf, off)
		if err != nil {
			return c, err
		}
		if n != group {
			return c, fmt.Errorf("short write: %d of %d", n, group)
		}
		if err := unix.Fdatasync(fd); err != nil {
			return c, err
		}
		off += int64(group)
		c.Writes++
	}
	c.Seconds = time.Since(start).Seconds()
	c.MiBps = float64(c.Writes) * float64(group) / (1 << 20) / c.Seconds
	return c, nil
}

// aligned returns n bytes at a 4 KiB boundary, as O_DIRECT needs.
func aligned(n int) []byte {
	b := make([]byte, n+4096)
	off := (4096 - int(uintptr(unsafe.Pointer(&b[0]))&4095)) & 4095
	b = b[off : off+n]
	for i := range b {
		b[i] = byte(i*7 + 13)
	}
	return b
}
