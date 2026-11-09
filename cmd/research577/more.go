package main

// More independent primitives for #577: the costs the first program did not
// cover. None of them runs tigerstore code.

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

var diskDir = flag.String("disk-dir", os.TempDir(), "directory on the disk under test")

// measureCalls is measure with a fixed number of calls, for slow calls
// such as a disk sync, where 32 per time check would take too long.
func measureCalls(name string, size, calls int, f func()) {
	for rep := 0; rep < *reps; rep++ {
		runtime.GC()
		var a, z runtime.MemStats
		runtime.ReadMemStats(&a)
		begin := time.Now()
		start := cpu()
		for range calls {
			f()
		}
		used := cpu() - start
		wall := time.Since(begin)
		runtime.ReadMemStats(&z)
		emit(result{"primitive", name, size, calls, rep, float64(used) / float64(calls), float64(wall.Nanoseconds()) / float64(calls), float64(z.TotalAlloc-a.TotalAlloc) / float64(calls), float64(z.Mallocs-a.Mallocs) / float64(calls)})
	}
}

func more(fs []fixture) {
	// The kernel's floor: one syscall that does nothing.
	measure("syscall_getppid", 0, func() { sink += uint64(unix.Getppid()) })

	// CRC32C, as the WAL and the value checks use it.
	tab := crc32.MakeTable(crc32.Castagnoli)
	for _, n := range []int{64, 1024, 16384, 262144} {
		b := make([]byte, n)
		measure("crc32c", n, func() { sink += uint64(crc32.Checksum(b, tab)) })
	}
	for _, n := range []int{65536, 262144, 1 << 20} {
		src := make([]byte, n)
		dst := make([]byte, n)
		measure("copy", n, func() { copy(dst, src); sink += uint64(dst[n-1]) })
	}

	// HPACK floor: a request's header block compared with the one the
	// connection sent last, and a response's constant blocks appended.
	reqBlock := []byte{0x83, 0x87, 0xbe, 0xbd, 0xbc, 0xbb}
	last := bytes.Clone(reqBlock)
	respHeaders := []byte{0x88, 0xbf}                 // :status 200, content-type (indexed)
	respTrailers := []byte{0x40, 0x0b, 'g', 'r', 'p'} // stands for a grpc-status block
	out := make([]byte, 0, 64)
	measure("hpack_floor_compare_and_constant", len(reqBlock), func() {
		if !bytes.Equal(reqBlock, last) {
			panic("block")
		}
		out = append(append(out[:0], respHeaders...), respTrailers...)
		sink += uint64(len(out))
	})

	// The real protobuf library on the same weighted corpus as the alias
	// parser: unmarshal every request into Go objects, marshal every reply.
	corpus := weightedCorpus(fs)
	at := 0
	var txn pb.TxnRequest
	var rr pb.RangeRequest
	opts := proto.UnmarshalOptions{}
	measure("protobuf_lib_unmarshal_request_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		var e error
		if f.Write != 0 {
			txn.Reset()
			e = opts.Unmarshal(f.Request, &txn)
		} else {
			rr.Reset()
			e = opts.Unmarshal(f.Request, &rr)
		}
		must(e)
	})
	replies := make([]proto.Message, len(fs))
	for i, f := range fs {
		switch {
		case f.Name == "list":
			replies[i] = &pb.RangeStreamResponse{RangeResponse: &pb.RangeResponse{Header: header(), Kvs: f.KVs, Count: int64(len(f.KVs))}}
		case len(f.KVs) > 0 || f.Name == "rev":
			replies[i] = &pb.RangeResponse{Header: header(), Kvs: f.KVs, Count: int64(len(f.KVs))}
		default:
			var m pb.TxnResponse
			must(proto.Unmarshal(f.Reply, &m))
			replies[i] = &m
		}
	}
	index := map[*fixture]int{}
	for i := range fs {
		index[&fs[i]] = i
	}
	buf := make([]byte, 0, 2<<20)
	mo := proto.MarshalOptions{}
	measure("protobuf_lib_marshal_reply_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		var e error
		buf, e = mo.MarshalAppend(buf[:0], replies[index[f]])
		must(e)
		sink += uint64(len(buf))
	})
	// The same reply bytes, by the hand encoder, for the leader's share:
	// only the header and the metadata of each key-value are encoded;
	// key and value bytes are referenced, not copied (an iovec), and the
	// TLS seal reads them in place. Counted: bytes that would be sealed.
	measure("protobuf_reply_metadata_only_mixed", 0, func() {
		f := corpus[at%len(corpus)]
		at++
		buf = buf[:0]
		for _, k := range f.KVs {
			buf = vfield(buf, 2, uint64(k.CreateRevision))
			buf = vfield(buf, 3, uint64(k.ModRevision))
			buf = vfield(buf, 4, uint64(k.Version))
			sink += uint64(len(k.Key) + len(k.Value))
		}
		sink += uint64(len(buf))
	})

	// Goroutine handoff on one P: what one helper goroutine per step costs
	// when a request passes from reader to committer to writer.
	prev := runtime.GOMAXPROCS(1)
	ping, pong := make(chan uint64), make(chan uint64)
	go func() {
		for v := range ping {
			pong <- v + 1
		}
	}()
	measure("goroutine_handoff_roundtrip_1P", 0, func() { ping <- sink; sink = <-pong })
	close(ping)
	runtime.GOMAXPROCS(prev)

	disk()
	writev()
}

func weightedCorpus(fs []fixture) []*fixture {
	cdf := make([]float64, len(fs))
	total := 0.0
	for i, f := range fs {
		total += f.Weight
		cdf[i] = total
	}
	corpus := make([]*fixture, 10000)
	j := 0
	for i := range corpus {
		v := (float64(i) + .5) / float64(len(corpus))
		for j < len(cdf)-1 && cdf[j] < v {
			j++
		}
		corpus[i] = &fs[j]
	}
	return corpus
}

// disk measures the CPU of writing a log: buffered appends to a growing
// file, overwrites of a preallocated file, and O_DIRECT overwrites, each
// followed by fdatasync. CPU is the process's (user + system); the wait
// for the device is not CPU.
func disk() {
	must(os.MkdirAll(*diskDir, 0o755))
	sizes := []int{4096, 65536, 262144, 1 << 20}
	for _, n := range sizes {
		calls := 200
		if n >= 262144 {
			calls = 100
		}
		data := alignedBuf(n)
		// Buffered append, no sync: the copy into the page cache.
		path := filepath.Join(*diskDir, "r577-append")
		f, e := os.Create(path)
		must(e)
		off := int64(0)
		measureCalls("disk_pwrite_append_nosync", n, calls*4, func() {
			_, e := f.WriteAt(data, off)
			must(e)
			off += int64(n)
		})
		must(f.Close())
		must(os.Remove(path))
		// Buffered append + fdatasync.
		f, e = os.Create(path)
		must(e)
		off = 0
		measureCalls("disk_pwrite_append_fdatasync", n, calls, func() {
			_, e := f.WriteAt(data, off)
			must(e)
			off += int64(n)
			must(unix.Fdatasync(int(f.Fd())))
		})
		must(f.Close())
		must(os.Remove(path))
		// Preallocated, written once, then overwritten in a ring + fdatasync.
		ring := int64(64 << 20)
		f, e = os.Create(path)
		must(e)
		must(unix.Fallocate(int(f.Fd()), 0, 0, ring))
		zero := alignedBuf(1 << 20)
		for o := int64(0); o < ring; o += int64(len(zero)) {
			_, e := f.WriteAt(zero, o)
			must(e)
		}
		must(f.Sync())
		off = 0
		measureCalls("disk_pwrite_prealloc_fdatasync", n, calls, func() {
			_, e := f.WriteAt(data, off)
			must(e)
			off = (off + int64(n)) % ring
			must(unix.Fdatasync(int(f.Fd())))
		})
		must(f.Close())
		// O_DIRECT overwrite of the same preallocated file + fdatasync.
		fd, e := unix.Open(path, unix.O_RDWR|unix.O_DIRECT, 0)
		if e != nil {
			emit(map[string]any{"kind": "skip", "name": "disk_odirect", "error": e.Error()})
		} else {
			off = 0
			measureCalls("disk_odirect_prealloc_fdatasync", n, calls, func() {
				_, e := unix.Pwrite(fd, data, off)
				must(e)
				off = (off + int64(n)) % ring
				must(unix.Fdatasync(fd))
			})
			// O_DIRECT|O_DSYNC: one syscall per append.
			must(unix.Close(fd))
			fd, e = unix.Open(path, unix.O_RDWR|unix.O_DIRECT|unix.O_DSYNC, 0)
			must(e)
			off = 0
			measureCalls("disk_odirect_dsync_prealloc", n, calls, func() {
				_, e := unix.Pwrite(fd, data, off)
				must(e)
				off = (off + int64(n)) % ring
			})
			must(unix.Close(fd))
		}
		must(os.Remove(path))
	}
}

func alignedBuf(n int) []byte {
	b := make([]byte, n+4096)
	off := 4096 - int(uintptr(unsafe.Pointer(&b[0]))&4095)
	if off == 4096 {
		off = 0
	}
	b = b[off : off+n]
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// writev: many small replies in one gather write (net.Buffers is writev
// on TCP), against one reply per write, on TCP to a drainer process on CPU 3.
func writev() {
	exe, e := os.Executable()
	must(e)
	for _, parts := range []int{1, 4, 16, 64} {
		const each = 1024
		c, cleanup := dialHelper(exe, "drain,tcp,65536")
		iov := make([][]byte, parts)
		for i := range iov {
			iov[i] = make([]byte, each)
		}
		bufs := make(net.Buffers, parts)
		measure(fmt.Sprintf("tcp_writev_%dx1KiB", parts), parts*each, func() {
			bufs = bufs[:parts]
			copy(bufs, iov)
			b := bufs
			_, e := b.WriteTo(c)
			must(e)
		})
		cleanup()
	}
}

func dialHelper(exe, spec string) (net.Conn, func()) {
	cmd := exec.Command("taskset", "-c", "3", exe, "-helper", spec, "-certs", *certDir)
	cmd.Stderr = os.Stderr
	stdout, e := cmd.StdoutPipe()
	must(e)
	must(cmd.Start())
	line, e := bufio.NewReader(stdout).ReadString('\n')
	must(e)
	c, e := net.Dial("tcp", strings.TrimSpace(line))
	must(e)
	return c, func() {
		must(c.Close())
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}
