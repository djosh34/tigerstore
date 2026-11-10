package main

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

// The primitives are the steps the least CPU of an operation is made of,
// each timed alone in a loop on one thread, by its CPU time:
// cmd/benchreport adds them up by what each operation carries. Each has a
// cost per call and per byte, fitted from two sizes, so neither hides in
// the other.
type primitives struct {
	// TLS 1.3 records, AES-128-GCM, the cipher the stores negotiate.
	Seal cost `json:"tls_seal"`
	Open cost `json:"tls_open"`
	// Loopback TCP: the writer's CPU per write, which on loopback also
	// runs the receiver's TCP work, and the reader's per read.
	Send cost `json:"tcp_send"`
	Recv cost `json:"tcp_recv"`
	// CRC32C, as the log checks its records, and a copy in memory.
	CRC  cost `json:"crc32c"`
	Copy cost `json:"copy"`
	// The log on the shared disk: an O_DIRECT write into written space,
	// and an fdatasync after it, the CPU of each.
	Write cost `json:"wal_write"`
	Sync  cost `json:"fdatasync"`
	// SyncLatencyNs is the wall time of a 4 KiB O_DIRECT write and its
	// fdatasync, the least time a write takes to be durable.
	SyncLatencyNs float64 `json:"sync_latency_ns"`
}

// cost is a primitive's CPU time: per call, plus per byte.
type cost struct {
	CallNs float64 `json:"call_ns"`
	ByteNs float64 `json:"byte_ns"`
}

// fit returns the cost of a step that handles small and large bytes a
// call, from its CPU time per call at each size.
func fit(small, large int, smallNs, largeNs float64) cost {
	perByte := (largeNs - smallNs) / float64(large-small)
	return cost{CallNs: max(smallNs-perByte*float64(small), 0), ByteNs: perByte}
}

// perCall runs step on this thread until d has passed and returns its CPU
// time per call.
func perCall(d time.Duration, step func()) (float64, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	before, err := threadCPU()
	if err != nil {
		return 0, err
	}
	var n int64
	for start := time.Now(); time.Since(start) < d; n++ {
		step()
	}
	after, err := threadCPU()
	return float64((after - before).Nanoseconds()) / float64(n), err
}

// sizedCost times step at 1 KiB and 16 KiB, the largest TLS record.
func sizedCost(d time.Duration, step func(size int) func()) (cost, error) {
	const small, large = 1 << 10, 16 << 10
	s, err := perCall(d, step(small))
	if err != nil {
		return cost{}, err
	}
	l, err := perCall(d, step(large))
	return fit(small, large, s, l), err
}

func (p *primitives) measure(d time.Duration, dir string) error {
	// Fixed key and nonce: this times the cipher and protects nothing.
	block, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if p.Seal, err = sizedCost(d, func(size int) func() {
		record, out := make([]byte, size), make([]byte, 0, size+aead.Overhead())
		return func() { out = aead.Seal(out[:0], nonce, record, nil) }
	}); err != nil {
		return err
	}
	var openErr error
	p.Open, err = sizedCost(d, func(size int) func() {
		sealed := aead.Seal(nil, nonce, make([]byte, size), nil)
		out := make([]byte, 0, size)
		return func() {
			if _, err := aead.Open(out[:0], nonce, sealed, nil); err != nil {
				openErr = err
			}
		}
	})
	if err == nil {
		err = openErr
	}
	if err != nil {
		return err
	}
	const small, large = 4 << 10, 64 << 10
	sendS, recvS, err := tcp(d, small)
	if err != nil {
		return err
	}
	sendL, recvL, err := tcp(d, large)
	p.Send, p.Recv = fit(small, large, sendS, sendL), fit(small, large, recvS, recvL)
	if err != nil {
		return err
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	if p.CRC, err = sizedCost(d, func(size int) func() {
		b := make([]byte, size)
		return func() { crc32.Update(0, table, b) }
	}); err != nil {
		return err
	}
	if p.Copy, err = sizedCost(d, func(size int) func() {
		from, to := make([]byte, size), make([]byte, size)
		return func() { copy(to, from) }
	}); err != nil {
		return err
	}
	return p.wal(d, dir)
}

// wal times O_DIRECT writes of 64 KiB and 1 MiB into a file already
// written, and the fdatasync after each, by CPU time; and the wall time of
// a 4 KiB write and its fdatasync.
func (p *primitives) wal(d time.Duration, dir string) (err error) {
	const small, large, size = 64 << 10, 1 << 20, 64 << 20
	name := filepath.Join(dir, "ceiling-wal.data")
	if err := fill(name, size); err != nil {
		return errors.Join(err, removeIfThere(name))
	}
	defer func() { err = errors.Join(err, os.Remove(name)) }()
	fd, err := unix.Open(name, unix.O_WRONLY|unix.O_DIRECT, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unix.Close(fd)) }()
	var ioErr error
	write := func(n int, sync bool) func() {
		buf := aligned(n)
		var off int64
		return func() {
			if off+int64(n) > size {
				off = 0
			}
			if _, err := unix.Pwrite(fd, buf, off); err != nil {
				ioErr = err
			}
			off += int64(n)
			if sync {
				if err := unix.Fdatasync(fd); err != nil {
					ioErr = err
				}
			}
		}
	}
	var ns [2][2]float64 // [size][sync]
	for i, n := range []int{small, large} {
		for j, sync := range []bool{false, true} {
			if ns[i][j], err = perCall(d, write(n, sync)); err != nil {
				return err
			}
		}
	}
	p.Write = fit(small, large, ns[0][0], ns[1][0])
	// What the sync adds to each write, per call, at either size.
	p.Sync = cost{CallNs: max((ns[0][1]-ns[0][0]+ns[1][1]-ns[1][0])/2, 0)}
	step := write(4<<10, true)
	var calls int64
	start := time.Now()
	for ; time.Since(start) < d; calls++ {
		step()
	}
	p.SyncLatencyNs = float64(time.Since(start).Nanoseconds()) / float64(calls)
	return ioErr
}

// tcp writes size bytes at a time to a loopback TCP connection for d
// while another thread reads them, size bytes a read at most, and returns
// the CPU time of each side per call.
func tcp(d time.Duration, size int) (sendNs, recvNs float64, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, 0, err
	}
	type result struct {
		ns  float64
		err error
	}
	read := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		c, err := ln.Accept()
		if err != nil {
			read <- result{err: err}
			return
		}
		buf := make([]byte, size)
		before, err := threadCPU()
		var calls int64
		for err == nil {
			_, err = c.Read(buf)
			calls++
		}
		after, cpuErr := threadCPU()
		if errors.Is(err, io.EOF) {
			err = nil
		}
		read <- result{float64((after - before).Nanoseconds()) / float64(calls), errors.Join(err, cpuErr, c.Close())}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return 0, 0, errors.Join(err, ln.Close())
	}
	chunk := make([]byte, size)
	var writeErr error
	sendNs, err = perCall(d, func() {
		if _, werr := c.Write(chunk); werr != nil {
			writeErr = werr
		}
	})
	err = errors.Join(err, writeErr, c.Close())
	r := <-read
	return sendNs, r.ns, errors.Join(err, r.err, ln.Close())
}
