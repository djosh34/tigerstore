package main

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"net"
	"runtime"
	"time"
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

func (p *primitives) measure(d time.Duration) error {
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
	return err
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
