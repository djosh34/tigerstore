package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type offsets struct {
	Head, Tail, Mask, Entries, Flags, Dropped, Array, Reserved uint32
	Addr                                                       uint64
}
type cqOffsets struct {
	Head, Tail, Mask, Entries, Overflow, CQEs, Flags, Reserved uint32
	Addr                                                       uint64
}
type params struct {
	SQ, CQ, Flags, CPU, Idle, Features, WQ uint32
	Reserved                               [3]uint32
	S                                      offsets
	C                                      cqOffsets
}
type sqe struct {
	Op, Flags        uint8
	Priority         uint16
	FD               int32
	Offset, Addr     uint64
	Len, OpFlags     uint32
	Data             uint64
	Buf, Personality uint16
	Splice           int32
	Pad              [2]uint64
}
type cqe struct {
	Data  uint64
	Res   int32
	Flags uint32
}
type bufreg struct {
	Addr         uint64
	Entries      uint32
	Group, Flags uint16
	Reserved     [3]uint64
}
type pbuf struct {
	Addr         uint64
	Len          uint32
	ID, Reserved uint16
}
type connection struct {
	FD        int
	Remainder int
	Opcode    byte
	Pending   int
	Busy      bool
	Closed    bool
}

const nbuf = 256
const recvSize = 16384
const sendSize = 262144

func ptr[T any](b []byte, off uint32) *T { return (*T)(unsafe.Pointer(&b[off])) }
func mapped(fd, size int, off int64) []byte {
	b, e := unix.Mmap(fd, off, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	check(e)
	return b
}
func anon(size int) []byte {
	b, e := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	check(e)
	return b
}
func uringServer() {
	runtime.LockOSThread()
	p := params{Flags: 1<<12 | 1<<13}
	fd, _, err := unix.Syscall(unix.SYS_IO_URING_SETUP, 1024, uintptr(unsafe.Pointer(&p)), 0)
	if err != 0 {
		panic(err)
	}
	if unsafe.Sizeof(p) != 120 || unsafe.Sizeof(sqe{}) != 64 {
		panic("ABI")
	}
	size := max(int(p.S.Array+p.SQ*4), int(p.C.CQEs+p.CQ*16))
	if p.Features&1 == 0 {
		panic("single mmap required")
	}
	rings := mapped(int(fd), size, 0)
	sqmem := mapped(int(fd), int(p.SQ)*64, 0x10000000)
	sh := ptr[uint32](rings, p.S.Head)
	st := ptr[uint32](rings, p.S.Tail)
	sm := *ptr[uint32](rings, p.S.Mask)
	ch := ptr[uint32](rings, p.C.Head)
	ct := ptr[uint32](rings, p.C.Tail)
	cm := *ptr[uint32](rings, p.C.Mask)
	localTail := atomic.LoadUint32(st)
	submitted := localTail
	submit := func(op uint8, sock int, data uint64) *sqe {
		if localTail-atomic.LoadUint32(sh) >= p.SQ {
			panic("SQ full")
		}
		index := localTail & sm
		s := (*sqe)(unsafe.Pointer(&sqmem[index*64]))
		*s = sqe{Op: op, FD: int32(sock), Data: data}
		*ptr[uint32](rings, p.S.Array+index*4) = index
		localTail++
		return s
	}
	br := anon(nbuf * 16)
	recv := anon(nbuf * recvSize)
	send := anon(sendSize)
	for i := range send {
		send[i] = 7
	}
	reg := bufreg{Addr: uint64(uintptr(unsafe.Pointer(&br[0]))), Entries: nbuf, Group: 1}
	_, _, err = unix.Syscall6(unix.SYS_IO_URING_REGISTER, fd, 22, uintptr(unsafe.Pointer(&reg)), 1, 0, 0)
	if err != 0 {
		panic(err)
	}
	iov := unix.Iovec{Base: &send[0], Len: uint64(len(send))}
	_, _, err = unix.Syscall6(unix.SYS_IO_URING_REGISTER, fd, 0, uintptr(unsafe.Pointer(&iov)), 1, 0, 0)
	if err != 0 {
		panic(err)
	}
	var btail uint16
	// Ring tail overlays entry zero's last halfword; publish with an aligned
	// release store preserving entry zero's buffer ID in the low halfword.
	recycle := func(id uint16) {
		b := (*pbuf)(unsafe.Pointer(&br[(int(btail)&(nbuf-1))*16]))
		b.Addr = uint64(uintptr(unsafe.Pointer(&recv[int(id)*recvSize])))
		b.Len = recvSize
		b.ID = id
		btail++
		word := ptr[uint32](br, 12)
		atomic.StoreUint32(word, (*word&65535)|uint32(btail)<<16)
	}
	for i := 0; i < nbuf; i++ {
		recycle(uint16(i))
	}
	l, e := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	check(e)
	check(unix.SetsockoptInt(l, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1))
	check(unix.Bind(l, &unix.SockaddrInet4{Port: 9910, Addr: [4]byte{127, 0, 0, 1}}))
	check(unix.Listen(l, 256))
	accept := func() { s := submit(13, l, 1); s.Priority = 1; s.OpFlags = unix.SOCK_CLOEXEC }
	conns := make([]connection, 128)
	next := 0
	receive := func(i int) {
		s := submit(27, conns[i].FD, uint64(i+1)<<8|2)
		s.Priority = 2
		s.Flags = 1 << 5
		s.Buf = 1
	}
	accept()
	fmt.Println("READY")
	stopped := false
	for !stopped {
		atomic.StoreUint32(st, localTail)
		n := localTail - submitted
		r, _, err := unix.Syscall6(unix.SYS_IO_URING_ENTER, fd, uintptr(n), 1, 1, 0, 0)
		if err == unix.EINTR {
			continue
		}
		if err != 0 {
			panic(err)
		}
		submitted += uint32(r)
		head := atomic.LoadUint32(ch)
		tail := atomic.LoadUint32(ct)
		for head != tail {
			c := *(*cqe)(unsafe.Pointer(&rings[p.C.CQEs+(head&cm)*16]))
			head++
			kind := c.Data & 255
			if kind == 1 {
				if c.Res < 0 {
					panic("accept failed")
				}
				if next == len(conns) {
					panic("too many connections")
				}
				i := next
				next++
				conns[i].FD = int(c.Res)
				check(unix.SetsockoptInt(int(c.Res), unix.IPPROTO_TCP, unix.TCP_NODELAY, 1))
				receive(i)
				if c.Flags&2 == 0 {
					accept()
				}
				continue
			}
			i := int(c.Data>>8) - 1
			state := &conns[i]
			if kind == 3 {
				if c.Flags&8 != 0 {
					if measuring.Load() {
						zcNotifications++
						if uint32(c.Res)&(1<<31) != 0 {
							zcCopied++
						}
					}
					continue
				}
				state.Busy = false
				if c.Res < 0 {
					panic(fmt.Sprintf("send: %d", c.Res))
				}
				state.Pending -= int(c.Res)
				continue
			}
			if c.Res <= 0 {
				if c.Res == -int32(unix.ENOBUFS) {
					receive(i)
					continue
				}
				state.Closed = true
				unix.Close(state.FD)
				continue
			}
			if c.Flags&1 == 0 {
				panic("missing selected buffer")
			}
			bid := uint16(c.Flags >> 16)
			b := recv[int(bid)*recvSize : int(bid)*recvSize+int(c.Res)]
			// Requests are 100 bytes; retain the opcode across partial frames.
			var requests uint64
			pos := 0
			if state.Remainder > 0 {
				take := min(100-state.Remainder, len(b))
				state.Remainder += take
				pos += take
				if state.Remainder == 100 {
					state.Remainder = 0
					if state.Opcode == 254 {
						begin()
						state.Pending += *reply
					} else if state.Opcode == 253 {
						stopped = true
					} else {
						state.Pending += *reply
						requests++
					}
				}
			}
			for pos+100 <= len(b) {
				v := b[pos]
				pos += 100
				if v == 254 {
					begin()
					state.Pending += *reply
				} else if v == 253 {
					stopped = true
				} else {
					state.Pending += *reply
					requests++
				}
			}
			if pos < len(b) {
				if state.Remainder == 0 {
					state.Opcode = b[pos]
				}
				state.Remainder += len(b) - pos
			}
			if measuring.Load() {
				count.Add(requests)
			}
			recycle(bid)
			if c.Flags&2 == 0 {
				receive(i)
			}
		}
		atomic.StoreUint32(ch, head)
		for i := 0; i < next; i++ {
			s := &conns[i]
			if !s.Closed && !s.Busy && s.Pending > 0 {
				op := uint8(26)
				if *mode == "uringzc" {
					op = 47
				}
				q := submit(op, s.FD, uint64(i+1)<<8|3)
				q.Addr = uint64(uintptr(unsafe.Pointer(&send[0])))
				q.Len = uint32(min(s.Pending, sendSize))
				q.OpFlags = unix.MSG_NOSIGNAL
				if op == 47 {
					q.Priority = 4 | 8 // fixed registered buffer + report zero-copy fallback
				}
				s.Busy = true
			}
		}
	}
	finish()
	if *ptr[uint32](rings, p.C.Overflow) != 0 {
		panic("CQ overflow")
	}
	runtime.KeepAlive(recv)
	runtime.KeepAlive(send)
	runtime.KeepAlive(br)
}
