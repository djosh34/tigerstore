package main

import (
	"crypto/aes"
	"crypto/cipher"
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
	return err
}
