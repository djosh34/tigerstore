package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protowire"
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
	// The index at a million keys of 60 bytes: a lookup in a hash map,
	// and an insert or a delete in a B-tree, each with the cache misses
	// a big index has; and a read of a cold value, one miss.
	LookupNs float64 `json:"lookup_ns"`
	InsertNs float64 `json:"insert_ns"`
	DeleteNs float64 `json:"delete_ns"`
	MissNs   float64 `json:"miss_ns"`
	// Messages: a protobuf request walked field by field, without copies,
	// per call and per byte; the frames of one gRPC call over HTTP/2, a
	// HEADERS and a DATA frame header with the gRPC prefix, read and
	// written; and the metadata of one key-value an answer carries, its
	// key and revisions, the value not copied.
	Parse   cost    `json:"proto_parse"`
	FrameNs float64 `json:"h2_frames_ns"`
	KVNs    float64 `json:"kv_metadata_ns"`
	// LoopbackBytesPerS is what three loopback TCP connections carry
	// together, each with its writer and reader on threads of their own.
	LoopbackBytesPerS float64 `json:"loopback_bytes_per_s"`
}

// cost is a primitive's CPU time: per call, plus per byte.
type cost struct {
	CallNs float64 `json:"call_ns"`
	ByteNs float64 `json:"byte_ns"`
}

// fit returns the cost of a step that handles small and large bytes a
// call, from its CPU time per call at each size.
func fit(small, large int, smallNs, largeNs float64) cost {
	perByte := max((largeNs-smallNs)/float64(large-small), 0)
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
	if err := p.wal(d, dir); err != nil {
		return err
	}
	if err := p.index(d); err != nil {
		return err
	}
	if err := p.messages(d); err != nil {
		return err
	}
	p.LoopbackBytesPerS, err = loopback(d, 3)
	return err
}

func (p *primitives) messages(d time.Duration) (err error) {
	// A Txn as kube-apiserver sends it: a compare of a key's mod
	// revision, a put of the key, and a read of it on failure.
	txn := func(value int) []byte {
		key := make([]byte, 60)
		var cmp, put, op, get, m []byte
		cmp = protowire.AppendVarint(protowire.AppendTag(cmp, 2, protowire.VarintType), 2)
		cmp = protowire.AppendBytes(protowire.AppendTag(cmp, 3, protowire.BytesType), key)
		cmp = protowire.AppendVarint(protowire.AppendTag(cmp, 6, protowire.VarintType), 1<<30)
		put = protowire.AppendBytes(protowire.AppendTag(put, 1, protowire.BytesType), key)
		put = protowire.AppendBytes(protowire.AppendTag(put, 2, protowire.BytesType), make([]byte, value))
		op = protowire.AppendBytes(protowire.AppendTag(op, 2, protowire.BytesType), put)
		get = protowire.AppendBytes(protowire.AppendTag(get, 1, protowire.BytesType), protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), key))
		m = protowire.AppendBytes(protowire.AppendTag(m, 1, protowire.BytesType), cmp)
		m = protowire.AppendBytes(protowire.AppendTag(m, 2, protowire.BytesType), op)
		return protowire.AppendBytes(protowire.AppendTag(m, 3, protowire.BytesType), get)
	}
	var walkErr error
	var walk func(b []byte, depth int)
	walk = func(b []byte, depth int) {
		for len(b) > 0 {
			num, typ, n := protowire.ConsumeTag(b)
			if n < 0 {
				walkErr = protowire.ParseError(n)
				return
			}
			b = b[n:]
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				walkErr = protowire.ParseError(m)
				return
			}
			// A Txn's success and failure ops hold a RequestOp, which
			// holds the put or range; compares and those hold no
			// messages.
			if typ == protowire.BytesType && (depth == 0 && num != 1 || depth == 1) {
				v, _ := protowire.ConsumeBytes(b)
				walk(v, depth+1)
			}
			b = b[m:]
		}
	}
	small, large := txn(100), txn(4000)
	s, err := perCall(d, func() { walk(small, 0) })
	if err != nil {
		return err
	}
	l, err := perCall(d, func() { walk(large, 0) })
	if err != nil {
		return err
	}
	p.Parse = fit(len(small), len(large), s, l)
	if walkErr != nil {
		return walkErr
	}
	var frames []byte
	var header [9]byte
	if p.FrameNs, err = perCall(d, func() {
		frames = appendFrame(frames[:0], 0x1, 12, 1)
		frames = appendFrame(frames, 0x0, 300, 1)
		frames = append(frames, 0, 0, 0, 1, 44)
		copy(header[:], frames[9:18]) // the DATA frame's header
		if binary.BigEndian.Uint32(header[5:])&(1<<31-1) != 1 {
			walkErr = errors.New("frame: wrong stream")
		}
	}); err != nil {
		return err
	}
	key := make([]byte, 60)
	var kv []byte
	p.KVNs, err = perCall(d, func() {
		kv = protowire.AppendBytes(protowire.AppendTag(kv[:0], 1, protowire.BytesType), key)
		kv = protowire.AppendVarint(protowire.AppendTag(kv, 2, protowire.VarintType), 1<<30)
		kv = protowire.AppendVarint(protowire.AppendTag(kv, 3, protowire.VarintType), 1<<30)
		kv = protowire.AppendVarint(protowire.AppendTag(kv, 4, protowire.VarintType), 7)
	})
	return errors.Join(err, walkErr)
}

// appendFrame appends an HTTP/2 frame header of length bytes.
func appendFrame(b []byte, typ byte, length int, stream uint32) []byte {
	b = append(b, byte(length>>16), byte(length>>8), byte(length), typ, 0)
	return binary.BigEndian.AppendUint32(b, stream)
}

// indexKeys is the size of the index the lookups run in.
const indexKeys = 1 << 20

func (p *primitives) index(d time.Duration) (err error) {
	rng := rand.New(rand.NewPCG(1, 2))
	key := func() string {
		return fmt.Sprintf("/registry/pods/namespace-%04d/pod-%033d", rng.IntN(1000), rng.Uint64())
	}
	keys := make([]string, indexKeys)
	hash := make(map[string]int32, indexKeys)
	tree := btree.NewG(32, func(a, b string) bool { return a < b })
	for i := range keys {
		keys[i] = key()
		hash[keys[i]] = int32(i)
		tree.ReplaceOrInsert(keys[i])
	}
	var found int32
	p.LookupNs, err = perCall(d, func() { found = hash[keys[rng.IntN(indexKeys)]] })
	runtime.KeepAlive(found)
	if err != nil {
		return err
	}
	// Inserts of new keys, then deletes of the same keys, each of a key
	// the tree has.
	fresh := make([]string, indexKeys)
	for i := range fresh {
		fresh[i] = key()
	}
	var inserted, deleted int
	if p.InsertNs, err = perCall(d, func() {
		tree.ReplaceOrInsert(fresh[inserted%len(fresh)])
		inserted++
	}); err != nil {
		return err
	}
	if p.DeleteNs, err = perCall(d, func() {
		tree.Delete(fresh[deleted%min(inserted, len(fresh))])
		deleted++
	}); err != nil {
		return err
	}
	// A chase through 256 MiB of pointers in a random cycle: each step
	// waits for the one before, a miss each.
	next := make([]uint32, 64<<20)
	order := rng.Perm(len(next))
	for j := range order {
		next[order[j]] = uint32(order[(j+1)%len(order)])
	}
	var at uint32
	p.MissNs, err = perCall(d, func() { at = next[at] })
	runtime.KeepAlive(at)
	return err
}

// loopback returns the bytes a second conns loopback TCP connections carry
// together for d, 64 KiB a write.
func loopback(d time.Duration, conns int) (float64, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	errs := make(chan error, 2*conns)
	var total atomic.Int64
	start := time.Now()
	for range conns {
		go func() {
			c, err := ln.Accept()
			if err != nil {
				errs <- err
				return
			}
			n, err := io.Copy(io.Discard, c)
			total.Add(n)
			errs <- errors.Join(err, c.Close())
		}()
		go func() {
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			chunk := make([]byte, 64<<10)
			for time.Since(start) < d && err == nil {
				_, err = c.Write(chunk)
			}
			errs <- errors.Join(err, c.Close())
		}()
	}
	var all []error
	for range 2 * conns {
		all = append(all, <-errs)
	}
	elapsed := time.Since(start)
	return float64(total.Load()) / elapsed.Seconds(), errors.Join(append(all, ln.Close())...)
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
