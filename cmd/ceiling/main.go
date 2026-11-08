// Command ceiling measures what the bench's runner allows, for the ceilings
// cmd/benchreport puts tigerstore and etcd against:
//
//   - the most the disk the three nodes share makes durable as a
//     sequential log: O_DIRECT writes into a file that is already written,
//     each followed by an fdatasync, so no write allocates blocks or commits
//     a journal; the best of three group sizes;
//   - the same with three writers at once, each on a file of its own, as
//     the three nodes write their logs to the one disk, for comparison;
//   - how many bytes one core encodes into protobuf key-values, encrypts
//     with AES-128-GCM, the cipher the stores' TLS 1.3 negotiates, and
//     sends on a loopback TCP socket.
//
// It prints one JSON object. Run it pinned to one CPU, with nothing else
// running.
//
//	ceiling -dir <dir on the disk> [-disk <name>] [-cpu <model>] [-size 1GiB] [-duration 5s] [-cpu-time 2s]
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protowire"
)

type report struct {
	Kernel string `json:"kernel"`
	CPU    string `json:"cpu"`
	Disk   string `json:"disk"`
	// DiskMiBps is the best of DiskGroups.
	DiskMiBps  float64 `json:"disk_mib_per_s"`
	DiskGroups []cell  `json:"disk_groups"`
	// DiskThreeMiBps is the best of DiskThreeGroups, three writers at
	// once, their bytes together.
	DiskThreeMiBps  float64 `json:"disk_three_mib_per_s"`
	DiskThreeGroups []cell  `json:"disk_three_groups"`
	// One core's cost per byte of each step a byte of an answer takes.
	EncodeNsPerByte  float64 `json:"encode_ns_per_byte"`
	EncryptNsPerByte float64 `json:"encrypt_ns_per_byte"`
	SendNsPerByte    float64 `json:"send_ns_per_byte"`
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
	r.EncodeNsPerByte = perByte(cpuTime, encode())
	seal, err := encrypt()
	if err != nil {
		return err
	}
	r.EncryptNsPerByte = perByte(cpuTime, seal)
	r.SendNsPerByte, err = send(cpuTime)
	return err
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
	var names []string
	for i := range 3 {
		n := filepath.Join(dir, fmt.Sprintf("ceiling-%d.data", i))
		names = append(names, n)
		if err := fill(n, size/3); err != nil {
			return errors.Join(err, removeIfThere(n))
		}
		defer func() { err = errors.Join(err, os.Remove(n)) }()
	}
	for _, group := range []int{256 << 10, 1 << 20, 4 << 20} {
		cells := make([]cell, len(names))
		errs := make([]error, len(names))
		var wg sync.WaitGroup
		for i, n := range names {
			wg.Go(func() { cells[i], errs[i] = measureDisk(n, size/3, group, dur) })
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return err
		}
		c := cell{Group: group}
		for _, w := range cells {
			c.Writes += w.Writes
			c.Seconds = max(c.Seconds, w.Seconds)
		}
		c.MiBps = float64(c.Writes) * float64(group) / (1 << 20) / c.Seconds
		r.DiskThreeGroups = append(r.DiskThreeGroups, c)
		r.DiskThreeMiBps = max(r.DiskThreeMiBps, c.MiBps)
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

// perByte runs step until d has passed and returns the time per byte it
// handled. step returns the bytes of one call.
func perByte(d time.Duration, step func() int) float64 {
	var n int64
	start := time.Now()
	for time.Since(start) < d {
		n += int64(step())
	}
	return float64(time.Since(start).Nanoseconds()) / float64(n)
}

// encode returns a step that encodes a page of 300 key-values of a 60-byte
// key and a 1,458-byte value, the mean of the Kubernetes dump, as a
// RangeResponse carries them. The bytes it counts are keys and values.
func encode() func() int {
	key := make([]byte, 60)
	value := make([]byte, 1458)
	var kv, page []byte
	return func() int {
		page = page[:0]
		for range 300 {
			kv = protowire.AppendBytes(protowire.AppendTag(kv[:0], 1, protowire.BytesType), key)
			kv = protowire.AppendVarint(protowire.AppendTag(kv, 3, protowire.VarintType), 1<<40)
			kv = protowire.AppendBytes(protowire.AppendTag(kv, 5, protowire.BytesType), value)
			page = protowire.AppendBytes(protowire.AppendTag(page, 2, protowire.BytesType), kv)
		}
		return 300 * (len(key) + len(value))
	}
}

// encrypt returns a step that seals one TLS record of 16 KiB.
func encrypt() (func() int, error) {
	// Fixed key and nonce: this times the cipher and protects nothing.
	block, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	record := make([]byte, 16<<10)
	out := make([]byte, 0, len(record)+aead.Overhead())
	return func() int {
		out = aead.Seal(out[:0], nonce, record, nil)
		return len(record)
	}, nil
}

// send writes 16 KiB at a time to a loopback TCP socket that another
// goroutine drains, and returns the CPU time of the writing thread per
// byte, which leaves the reader's out.
func send(d time.Duration) (ns float64, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, ln.Close()) }()
	drained := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			drained <- err
			return
		}
		_, err = io.Copy(io.Discard, c)
		drained <- errors.Join(err, c.Close())
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return 0, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	before, err := threadCPU()
	if err != nil {
		return 0, errors.Join(err, c.Close())
	}
	chunk := make([]byte, 16<<10)
	var n int64
	for start := time.Now(); time.Since(start) < d; {
		w, err := c.Write(chunk)
		n += int64(w)
		if err != nil {
			return 0, errors.Join(err, c.Close())
		}
	}
	after, err := threadCPU()
	err = errors.Join(err, c.Close(), <-drained)
	if err != nil {
		return 0, err
	}
	return float64((after - before).Nanoseconds()) / float64(n), nil
}

// threadCPU returns the user and system CPU time of the calling thread.
func threadCPU() (time.Duration, error) {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_THREAD, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}
