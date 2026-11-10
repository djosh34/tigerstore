// Command ceiling measures what the bench's runner allows, for the ceilings
// cmd/benchreport puts tigerstore and etcd against:
//
//   - the most the disk the three nodes share makes durable as a
//     sequential log: O_DIRECT writes into a file that is already written,
//     each followed by an fdatasync, so no write allocates blocks or commits
//     a journal; the best of three group sizes;
//   - the primitives the least CPU of an operation is made of, and
//     loopback's bandwidth (primitives.go).
//
// It prints one JSON object. Run it pinned to one CPU, with nothing else
// running.
//
//	ceiling -dir <dir on the disk> [-disk <name>] [-cpu <model>] [-size 1GiB] [-duration 5s] [-cpu-time 2s]
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type report struct {
	Kernel string `json:"kernel"`
	CPU    string `json:"cpu"`
	Disk   string `json:"disk"`
	// DiskMiBps is the best of DiskGroups.
	DiskMiBps  float64 `json:"disk_mib_per_s"`
	DiskGroups []cell  `json:"disk_groups"`
	// Primitives are the steps of the least CPU an operation needs.
	Primitives primitives `json:"primitives"`
}

type cell struct {
	Group   int     `json:"group_bytes"`
	Writes  int     `json:"writes"`
	MiBps   float64 `json:"mib_per_s"`
	Seconds float64 `json:"seconds"`
}

func main() {
	dir := flag.String("dir", "", "directory on the disk under test")
	disk := flag.String("disk", "", "name of the disk, copied into the report")
	cpu := flag.String("cpu", "", "CPU model, copied into the report")
	size := flag.Int64("size", 1<<30, "disk test file size in bytes")
	dur := flag.Duration("duration", 5*time.Second, "time per disk group size")
	cpuTime := flag.Duration("cpu-time", 2*time.Second, "time per CPU measurement")
	flag.Parse()
	r := report{Disk: *disk, CPU: *cpu}
	err := r.measure(*dir, *size, *dur, *cpuTime)
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(r)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ceiling:", err)
		os.Exit(1)
	}
}

func (r *report) measure(dir string, size int64, dur, cpuTime time.Duration) error {
	if dir == "" {
		return errors.New("need -dir")
	}
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return err
	}
	r.Kernel = unix.ByteSliceToString(u.Release[:])
	if err := r.disk(dir, size, dur); err != nil {
		return err
	}
	return r.Primitives.measure(cpuTime, dir)
}

func (r *report) disk(dir string, size int64, dur time.Duration) (err error) {
	name := filepath.Join(dir, "ceiling.data")
	if err := fill(name, size); err != nil {
		return errors.Join(err, removeIfThere(name))
	}
	defer func() { err = errors.Join(err, os.Remove(name)) }()
	for _, group := range []int{256 << 10, 1 << 20, 4 << 20} {
		c, err := measureDisk(name, size, group, dur)
		if err != nil {
			return err
		}
		r.DiskGroups = append(r.DiskGroups, c)
		r.DiskMiBps = max(r.DiskMiBps, c.MiBps)
	}
	return nil
}

// removeIfThere removes name if it exists.
func removeIfThere(name string) error {
	if err := os.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
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

func measureDisk(name string, size int64, group int, dur time.Duration) (c cell, err error) {
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

// threadCPU returns the user and system CPU time of the calling thread.
func threadCPU() (time.Duration, error) {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_THREAD, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}
