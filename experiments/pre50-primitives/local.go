package main

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/proto"
)

func timed(name string, rep, n, size int, fn func(int) error) error {
	if err := checkTime(); err != nil {
		return err
	}
	runtime.GC()
	c := cpuNS()
	t := time.Now()
	for i := 0; i < n; i++ {
		if err := fn(i); err != nil {
			return err
		}
	}
	wall := time.Since(t).Nanoseconds()
	cpu := cpuNS() - c
	return emit(map[string]any{"kind": "cost", "name": name, "rep": rep, "n": n, "bytes": size, "wall_ns": wall, "cpu_ns": cpu, "wall_ns_per_op": float64(wall) / float64(n), "cpu_ns_per_op": float64(cpu) / float64(n)})
}

// A single cached buffer, no socket or syscall. crypto/tls still builds and
// parses real TLS 1.3 records with its own traffic keys and sequence counters.
type memConn struct {
	net.Conn
	active bool
	reader io.Reader
	writer io.Writer
}

func (c *memConn) Read(p []byte) (int, error) {
	if c.active {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}
func (c *memConn) Write(p []byte) (int, error) {
	if c.active {
		return c.writer.Write(p)
	}
	return c.Conn.Write(p)
}
func tlsPair(dir string) (*tls.Conn, *tls.Conn, *memConn, *memConn, error) {
	cc, sc, err := tlsConfigs(dir)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	a, b := net.Pipe()
	ma, mb := &memConn{Conn: a}, &memConn{Conn: b}
	client, server := tls.Client(ma, cc), tls.Server(mb, sc)
	ch := make(chan error, 1)
	go func() { ch <- server.Handshake() }()
	if err = client.Handshake(); err != nil {
		return nil, nil, nil, nil, err
	}
	if err = <-ch; err != nil {
		return nil, nil, nil, nil, err
	}
	ma.active = true
	mb.active = true
	ma.writer = io.Discard
	mb.writer = io.Discard
	ma.reader = bytes.NewReader(nil)
	mb.reader = bytes.NewReader(nil)
	return client, server, ma, mb, nil
}
func local(dir string) error {
	value := bytes.Repeat([]byte{0x61}, 1024)
	request := &pb.PutRequest{Key: []byte("/registry/pods/ns/pre50-00000001"), Value: value}
	response := &pb.PutResponse{Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 12345, RaftTerm: 3}}
	requestWire, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	responseWire, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	if err = emit(map[string]any{"kind": "sizes", "value": len(value), "key": len(request.Key), "PutRequest": len(requestWire), "PutResponse": len(responseWire)}); err != nil {
		return err
	}
	frame := new(bytes.Buffer)
	w := http2.NewFramer(frame, nil)
	if err = w.WriteData(1, true, value); err != nil {
		return err
	}
	reader := bytes.NewReader(frame.Bytes())
	fr := http2.NewFramer(nil, reader)
	block, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, 12)
	for rep := 1; rep <= 5; rep++ {
		if err = timed("loop-control", rep, 10000000, 0, func(i int) error { sink += uint64(i); return nil }); err != nil {
			return err
		}
		dst := make([]byte, 1024)
		if err = timed("memcpy-1KiB-cached", rep, 10000000, 1024, func(i int) error { sink += uint64(copy(dst, value)); return nil }); err != nil {
			return err
		}
		// Consume the copied destination outside timing. Keep it live until here.
		for _, b := range dst {
			sink += uint64(b)
		}
		runtime.KeepAlive(dst)
		for _, size := range []int{1024, len(responseWire)} {
			plain := make([]byte, size+1)
			copy(plain, value)
			plain[size] = 23
			aad := []byte{23, 3, 3, 0, 0}
			binary.BigEndian.PutUint16(aad[3:], uint16(len(plain)+16))
			out := make([]byte, 0, len(plain)+16)
			name := fmt.Sprintf("AES128GCM-TLS-inner-%dB-seal", size)
			if err = timed(name, rep, 1000000, size, func(i int) error {
				binary.BigEndian.PutUint64(nonce[4:], uint64(i))
				out = gcm.Seal(out[:0], nonce, plain, aad)
				sink += uint64(out[0])
				return nil
			}); err != nil {
				return err
			}
			// Fixed authenticated input is safe for a decryption-only cost test. Seal
			// above uses unique sequence nonces. This is not a live traffic-key policy.
			binary.BigEndian.PutUint64(nonce[4:], 1)
			encrypted := gcm.Seal(nil, nonce, plain, aad)
			decoded := make([]byte, 0, len(plain))
			if err = timed(fmt.Sprintf("AES128GCM-TLS-inner-%dB-open", size), rep, 1000000, size, func(i int) error {
				var e error
				decoded, e = gcm.Open(decoded[:0], nonce, encrypted, aad)
				if e != nil {
					return e
				}
				sink += uint64(decoded[0])
				return nil
			}); err != nil {
				return err
			}
			c, s, mc, ms, e := tlsPair(dir)
			if e != nil {
				return e
			}
			payload := value
			if size != 1024 {
				payload = responseWire
			}
			if err = timed(fmt.Sprintf("crypto-tls-record-%dB-encrypt-discard", size), rep, 200000, size, func(i int) error {
				n, e := c.Write(payload)
				if e != nil {
					return e
				}
				if n != len(payload) {
					return io.ErrShortWrite
				}
				sink += uint64(n)
				return nil
			}); err != nil {
				return err
			}
			if e = mc.Conn.Close(); e != nil {
				return e
			}
			if e = ms.Conn.Close(); e != nil {
				return e
			}
			// Build 100k genuine records outside the decrypt timing, then consume each
			// exactly once, in sequence, through tls.Conn.Read. No replay shortcut.
			c, s, mc, ms, e = tlsPair(dir)
			if e != nil {
				return e
			}
			capture := new(bytes.Buffer)
			capture.Grow(100000 * (size + 22))
			mc.writer = capture
			for i := 0; i < 100000; i++ {
				if _, e = c.Write(payload); e != nil {
					return e
				}
			}
			if e = emit(map[string]any{"kind": "TLS-record-evidence", "rep": rep, "app_bytes": size, "records": 100000, "wire_bytes": capture.Len(), "version": c.ConnectionState().Version, "cipher": tls.CipherSuiteName(c.ConnectionState().CipherSuite)}); e != nil {
				return e
			}
			ms.reader = bytes.NewReader(capture.Bytes())
			decodedRecord := make([]byte, size)
			if err = timed(fmt.Sprintf("crypto-tls-record-%dB-decrypt-memory", size), rep, 100000, size, func(i int) error {
				n, e := io.ReadFull(s, decodedRecord)
				if e != nil {
					return e
				}
				if n != size {
					return errors.New("TLS payload mismatch")
				}
				sink += uint64(n)
				return nil
			}); err != nil {
				return err
			}
			if !bytes.Equal(decodedRecord, payload) {
				return errors.New("TLS plaintext mismatch")
			}
			if e = mc.Conn.Close(); e != nil {
				return e
			}
			if e = ms.Conn.Close(); e != nil {
				return e
			}
		}
		if err = timed("HTTP2-DATA-parse-1KiB", rep, 1000000, 1024, func(i int) error {
			reader.Reset(frame.Bytes())
			f, e := fr.ReadFrame()
			if e != nil {
				return e
			}
			d, ok := f.(*http2.DataFrame)
			if !ok || d.StreamID != 1 || len(d.Data()) != 1024 {
				return errors.New("wrong frame")
			}
			sink += uint64(len(d.Data()))
			return nil
		}); err != nil {
			return err
		}
		for _, obj := range []struct {
			name    string
			message proto.Message
			wire    []byte
			fresh   func() proto.Message
		}{{"PutRequest", request, requestWire, func() proto.Message { return new(pb.PutRequest) }}, {"PutResponse", response, responseWire, func() proto.Message { return new(pb.PutResponse) }}} {
			envelope := make([]byte, 5+len(obj.wire))
			binary.BigEndian.PutUint32(envelope[1:], uint32(len(obj.wire)))
			copy(envelope[5:], obj.wire)
			if err = timed("grpc-envelope-"+obj.name+"-decode-fresh", rep, 1000000, len(obj.wire), func(i int) error {
				if envelope[0] != 0 || int(binary.BigEndian.Uint32(envelope[1:5])) != len(envelope)-5 {
					return errors.New("bad envelope")
				}
				m := obj.fresh()
				if e := proto.Unmarshal(envelope[5:], m); e != nil {
					return e
				}
				sink += uint64(len(envelope))
				runtime.KeepAlive(m)
				return nil
			}); err != nil {
				return err
			}
			if err = timed("grpc-envelope-"+obj.name+"-encode-fresh", rep, 1000000, len(obj.wire), func(i int) error {
				b, e := proto.MarshalOptions{}.MarshalAppend(make([]byte, 5, 5+proto.Size(obj.message)), obj.message)
				if e != nil {
					return e
				}
				binary.BigEndian.PutUint32(b[1:5], uint32(len(b)-5))
				sink += uint64(len(b))
				return nil
			}); err != nil {
				return err
			}
		}
		entry := &raftpb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: raftpb.EntryNormal.Enum(), Data: value}
		entries := make([]struct {
			Term  uint64
			Index uint64
			Type  raftpb.EntryType
			Data  []byte
		}, 4096)
		if err = timed("raft-entry-memory-metadata-and-copy-ring", rep, 1000000, 1024, func(i int) error {
			j := i % len(entries)
			e := &entries[j]
			if e.Data == nil {
				e.Data = make([]byte, 1024)
			}
			e.Term = 1
			e.Index = uint64(i + 1)
			e.Type = raftpb.EntryNormal
			copy(e.Data, value)
			sink += e.Index
			return nil
		}); err != nil {
			return err
		}
		// Stock MemoryStorage.Append retains immutable Entry pointers. The 1KiB
		// payload is shared and entry construction is outside timing. No deep copy.
		stockEntries := make([]*raftpb.Entry, 1000000)
		for i := range stockEntries {
			stockEntries[i] = &raftpb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(uint64(i + 1)), Type: raftpb.EntryNormal.Enum(), Data: value}
		}
		storage := raft.NewMemoryStorage()
		if err = timed("raft-MemoryStorage-Append-prebuilt-pointer-no-data-copy", rep, len(stockEntries), 1024, func(i int) error { return storage.Append(stockEntries[i : i+1]) }); err != nil {
			return err
		}
		last, e := storage.LastIndex()
		if e != nil {
			return e
		}
		if last != uint64(len(stockEntries)) {
			return errors.New("wrong MemoryStorage last index")
		}
		sink += last
		runtime.KeepAlive(storage)
		stockEntries = nil
		storage = nil
		runtime.GC()
		file, e := os.CreateTemp(os.Getenv("TMPDIR"), "buffered-entry-*")
		if e != nil {
			return e
		}
		bw := bufio.NewWriterSize(file, 64*1024)
		buf := make([]byte, 4, 1100)
		err = timed("raft-entry-protobuf-framed-buffered-file-no-sync", rep, 200000, 1024, func(i int) error {
			*entry.Index = uint64(i + 1)
			b, e := proto.MarshalOptions{}.MarshalAppend(buf[:4], entry)
			if e != nil {
				return e
			}
			binary.BigEndian.PutUint32(b[:4], uint32(len(b)-4))
			n, e := bw.Write(b)
			if e != nil {
				return e
			}
			if n != len(b) {
				return io.ErrShortWrite
			}
			sink += uint64(n)
			return nil
		})
		// Final userspace flush is charged separately in evidence. Regular 64KiB
		// flushes and kernel page-cache copies already occur inside the timed loop.
		ct := cpuNS()
		ft := time.Now()
		if e = bw.Flush(); e != nil {
			return e
		}
		if e = emit(map[string]any{"kind": "buffered-final-flush", "rep": rep, "wall_ns": time.Since(ft).Nanoseconds(), "cpu_ns": cpuNS() - ct, "file_bytes": func() int64 {
			s, e := file.Stat()
			if e != nil {
				return -1
			}
			return s.Size()
		}()}); e != nil {
			return e
		}
		if e = file.Close(); e != nil {
			return e
		}
		if e = os.Remove(file.Name()); e != nil {
			return e
		}
		if err != nil {
			return err
		}
	}
	return emit(map[string]any{"kind": "sink", "value": sink})
}
func fsyncBench(dir string) error {
	for rep := 1; rep <= 5; rep++ {
		if err := timed("fsync-probe-pair-control", rep, 1000000, 0, func(i int) error {
			sc := cpuNS()
			st := time.Now()
			sink += uint64(time.Since(st).Nanoseconds() + cpuNS() - sc)
			return nil
		}); err != nil {
			return err
		}
		sizes := []int{4096, 65536, 1048576}
		if rep%2 == 0 {
			sizes = []int{1048576, 65536, 4096}
		}
		for _, size := range sizes {
			if err := checkTime(); err != nil {
				return err
			}
			f, err := os.CreateTemp(dir, "fsync-*")
			if err != nil {
				return err
			}
			const capacity = 64 * 1024 * 1024
			data := bytes.Repeat([]byte{byte(rep)}, size)
			// Initialize, then fsync file and directory outside timing. The test
			// overwrites already-written extents cyclically, not newly allocated ones.
			initial := bytes.Repeat([]byte{0x73}, 1024*1024)
			for i := 0; i < capacity/len(initial); i++ {
				if _, err = f.Write(initial); err != nil {
					return err
				}
			}
			if err = f.Sync(); err != nil {
				return err
			}
			d, err := os.Open(dir)
			if err != nil {
				return err
			}
			if err = d.Sync(); err != nil {
				return err
			}
			if err = d.Close(); err != nil {
				return err
			}
			runtime.GC()
			samples := make([]int64, 0, 20000)
			syncSamples := make([]int64, 0, 20000)
			c := cpuNS()
			start := time.Now()
			n := 0
			syncWall := int64(0)
			syncCPU := int64(0)
			for time.Since(start) < 2*time.Second {
				t := time.Now()
				offset := int64(n * size % capacity)
				written, e := f.WriteAt(data, offset)
				if e != nil {
					return e
				}
				if written != size {
					return io.ErrShortWrite
				}
				// These two clock/rusage probes are charged to combined CPU and wall.
				sc := cpuNS()
				st := time.Now()
				if e = f.Sync(); e != nil {
					return e
				}
				sw := time.Since(st).Nanoseconds()
				syncCPU += cpuNS() - sc
				syncWall += sw
				syncSamples = append(syncSamples, sw)
				samples = append(samples, time.Since(t).Nanoseconds())
				n++
			}
			wall := time.Since(start).Nanoseconds()
			cpu := cpuNS() - c
			if err = emit(map[string]any{"kind": "fsync", "rep": rep, "batch_bytes": size, "n": n, "wall_ns": wall, "cpu_ns": cpu, "wall_ns_per_batch": float64(wall) / float64(n), "cpu_ns_per_batch": float64(cpu) / float64(n), "fsync_wall_ns_per_batch": float64(syncWall) / float64(n), "fsync_cpu_ns_per_batch": float64(syncCPU) / float64(n), "batches_per_second": float64(n) * 1e9 / float64(wall), "write_sync_quantiles": stats(samples), "sync_quantiles": stats(syncSamples)}); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
			if err = os.Remove(f.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}
