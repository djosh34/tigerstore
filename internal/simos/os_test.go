package simos

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"net"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// A node's files lie on its disk under its data dir, with the system's
// calls and errors: create, write, read back at an offset and in turn,
// seek, sync, truncate, rename, remove, list a directory, and refuse what
// the system refuses.
func TestANodesFilesBehaveAsTheSystems(t *testing.T) {
	o := New()
	disk := simfs.New()
	n := o.NewNode("/data/", disk, nil)
	if _, err := n.Open("/elsewhere/f", unix.O_RDWR|unix.O_CREAT, 0o600); !errors.Is(err, unix.EACCES) {
		t.Fatalf("a file outside the data dir: %v", err)
	}
	// A directory is there once made, and a file only in one that is
	// (#689's area-1 review, finding 25).
	if _, err := n.Open("/data/wal/a", unix.O_RDWR|unix.O_CREAT, 0o600); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("a file created in a directory not made: %v", err)
	}
	if _, err := n.Open("/data/wal", unix.O_RDONLY|unix.O_DIRECTORY, 0); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("a directory not made opened: %v", err)
	}
	if _, err := n.ReadDir("/data/wal"); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("a directory not made read: %v", err)
	}
	for _, dir := range []string{"/data/wal", "/data/ckpt", "/data/wal"} {
		if err := n.MkdirAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	if names, err := n.ReadDir("/data"); err != nil || !slices.Equal(slices.Sorted(slices.Values(names)), []string{"ckpt", "wal"}) {
		t.Fatalf("the data dir holds %v %v, want its two empty directories", names, err)
	}
	if _, err := n.Open("/data/wal/missing", unix.O_RDWR, 0); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("a missing file: %v", err)
	}
	fd, err := n.Open("/data/wal/a", unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Open("/data/wal/a", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600); !errors.Is(err, unix.EEXIST) {
		t.Fatalf("O_EXCL on a file that exists: %v", err)
	}
	if k, err := o.Write(fd, []byte("hello")); err != nil || k != 5 {
		t.Fatalf("write: %d %v", k, err)
	}
	if k, err := o.Pwrite(fd, []byte("HE"), 0); err != nil || k != 2 {
		t.Fatalf("pwrite: %d %v", k, err)
	}
	if err := o.Fsync(fd); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 5)
	if k, err := o.Pread(fd, p, 0); err != nil || k != 5 || string(p) != "HEllo" {
		t.Fatalf("pread: %q %d %v", p[:k], k, err)
	}
	if k, err := o.PreadNoWait(fd, p[:3], 2); err != nil || k != 3 || string(p[:3]) != "llo" {
		t.Fatalf("pread from the page cache: %q %d %v", p[:k], k, err)
	}
	if off, err := o.Seek(fd, 1, 0); err != nil || off != 1 {
		t.Fatalf("seek from the start: %d %v", off, err)
	}
	if k, err := o.Read(fd, p[:2]); err != nil || string(p[:k]) != "El" {
		t.Fatalf("read after the seek: %q %v", p[:k], err)
	}
	if off, err := o.Seek(fd, -1, 2); err != nil || off != 4 {
		t.Fatalf("seek from the end: %d %v", off, err)
	}
	if off, err := o.Seek(fd, 0, 1); err != nil || off != 4 {
		t.Fatalf("seek from where it is: %d %v", off, err)
	}
	if err := o.Close(fd); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Pread(fd, p, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("a read of a closed descriptor: %v", err)
	}
	if err := n.Truncate("/data/wal/a", 2); err != nil {
		t.Fatal(err)
	}
	// simfs refuses a rename between two directories (#730), and simos
	// says so.
	if err := n.Rename("/data/wal/a", "/data/ckpt/b"); !errors.Is(err, simfs.ErrCrossDir) {
		t.Fatalf("a rename between two directories: %v, want simfs.ErrCrossDir", err)
	}
	if err := n.Rename("/data/wal/a", "/data/wal/b"); err != nil {
		t.Fatal(err)
	}
	if err := n.Rename("/data/wal/b", "/data/ckpt/b"); err == nil {
		t.Fatal("a second rename between two directories was taken")
	}
	// The file goes to ckpt/ by a copy, for the checks below.
	if fd, err := n.Open("/data/ckpt/b", unix.O_WRONLY|unix.O_CREAT, 0o600); err != nil {
		t.Fatal(err)
	} else {
		if _, err := o.Pwrite(fd, []byte("HE"), 0); err != nil {
			t.Fatal(err)
		}
		_ = o.Close(fd)
	}
	if err := n.Remove("/data/wal/b"); err != nil {
		t.Fatal(err)
	}
	if names, err := n.ReadDir("/data"); err != nil || !slices.Equal(slices.Sorted(slices.Values(names)), []string{"ckpt", "wal"}) {
		t.Fatalf("the data dir holds %v %v", names, err)
	}
	if names, err := n.ReadDir("/data/wal"); err != nil || len(names) != 0 {
		t.Fatalf("wal/, emptied, holds %v %v", names, err)
	}
	if names, err := n.ReadDir("/data/ckpt"); err != nil || !slices.Equal(names, []string{"b"}) {
		t.Fatalf("ckpt/ holds %v %v", names, err)
	}
	fd, err = n.Open("/data/ckpt/b", unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if k, err := o.Pread(fd, p, 0); err != nil || string(p[:k]) != "HE" {
		t.Fatalf("the renamed, truncated file reads %q %v", p[:k], err)
	}
	// A truncate past the end grows the file with zeros (#726).
	if err := n.Truncate("/data/ckpt/b", 5); err != nil {
		t.Fatal(err)
	}
	if k, err := o.Pread(fd, p, 0); err != nil || k != 5 || string(p[:k]) != "HE\x00\x00\x00" {
		t.Fatalf("the file grown by a truncate reads %q %v", p[:k], err)
	}
	_ = o.Close(fd)
	if _, err := n.Open("/data/ckpt/b", unix.O_RDONLY|unix.O_DIRECTORY, 0); !errors.Is(err, unix.ENOTDIR) {
		t.Fatalf("a file opened as a directory: %v", err)
	}
	// A directory opens, for its sync.
	dfd, err := n.Open("/data/ckpt", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Fsync(dfd); err != nil {
		t.Fatalf("a directory's sync: %v", err)
	}
	_ = o.Close(dfd)
	if err := n.Remove("/data/ckpt/b"); err != nil {
		t.Fatal(err)
	}
	if err := n.Remove("/data/ckpt/b"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removing a file twice: %v", err)
	}
	if err := n.MkdirAll("/data/x/y"); err != nil {
		t.Fatal(err)
	}
	if !disk.IsDir("x") || !disk.IsDir("x/y") {
		t.Fatalf("MkdirAll made %v, want x and x/y", disk.Dirs())
	}
	if free, err := n.FreeSpace("/data"); err != nil || free <= 0 {
		t.Fatalf("free space: %d %v", free, err)
	}
}

// A file opened for direct writes makes each write durable when it
// returns, as O_DIRECT|O_DSYNC; space allocated ahead keeps its size.
func TestADirectWriteIsDurableWhenItReturns(t *testing.T) {
	o := New()
	disk := simfs.New()
	n := o.NewNode("/data", disk, nil)
	if err := n.MkdirAll("/data/wal"); err != nil {
		t.Fatal(err)
	}
	fd, err := n.Open("/data/wal/s", unix.O_WRONLY|unix.O_CREAT, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_ = o.Close(fd)
	// The new name is durable once its directory is synced, as wl's log
	// syncs it, and the directory's own once the data dir is.
	for _, dir := range []string{"/data", "/data/wal"} {
		dfd, err := n.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.Fsync(dfd); err != nil {
			t.Fatal(err)
		}
		_ = o.Close(dfd)
	}
	fd, err = n.OpenDirect("/data/wal/s")
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Allocate(fd, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Pwrite(fd, []byte("durable"), 0); err != nil {
		t.Fatal(err)
	}
	if got := string(disk.DurableBytes("wal/s")); got != "durable" {
		t.Fatalf("after a direct write the disk holds %q durably, want the write and no more", got)
	}
}

// Under SetUncached a read that must not wait finds its bytes out of the
// page cache, as preadv2 with RWF_NOWAIT does.
func TestAReadThatMustNotWaitCanMissThePageCache(t *testing.T) {
	o := New()
	n := o.NewNode("/data", simfs.New(), nil)
	fd, err := n.Open("/data/f", unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Pwrite(fd, []byte("abc"), 0); err != nil {
		t.Fatal(err)
	}
	o.SetUncached(func() bool { return true })
	if _, err := o.PreadNoWait(fd, make([]byte, 3), 0); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("a read out of the page cache: %v", err)
	}
	if _, err := o.Pread(fd, make([]byte, 3), 0); err != nil {
		t.Fatalf("a read that may wait: %v", err)
	}
	o.SetUncached(nil)
	if _, err := o.PreadNoWait(fd, make([]byte, 3), 0); err != nil {
		t.Fatalf("a read once the page cache holds everything again: %v", err)
	}
}

// An eventfd adds what is written and hands the sum to one read; epoll
// reports it readable while it holds a count, and wakes a wait as a write
// comes.
func TestEpollWakesForAnEventfd(t *testing.T) {
	o := New()
	ep, err := o.EpollCreate()
	if err != nil {
		t.Fatal(err)
	}
	efd, err := o.Eventfd(true)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	if _, err := o.Read(efd, b); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("a read of an empty non-blocking eventfd: %v", err)
	}
	if _, err := o.Write(efd, b[:4]); !errors.Is(err, unix.EINVAL) {
		t.Fatalf("a write of 4 bytes: %v", err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_ADD, efd, unix.EPOLLIN, 7); err != nil {
		t.Fatal(err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_ADD, efd, unix.EPOLLIN, 7); !errors.Is(err, unix.EEXIST) {
		t.Fatalf("adding a descriptor twice: %v", err)
	}
	events := make([]unix.EpollEvent, 4)
	if k, err := o.EpollWait(ep, events, 0); err != nil || k != 0 {
		t.Fatalf("a wait with nothing ready: %d %v", k, err)
	}
	if k, err := o.EpollWait(ep, events, 1); err != nil || k != 0 {
		t.Fatalf("a wait that times out: %d %v", k, err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], 2)
		_, _ = o.Write(efd, w[:])
		_, _ = o.Write(efd, w[:])
	}()
	k, err := o.EpollWait(ep, events, -1)
	if err != nil || k != 1 || events[0].Fd != 7 || events[0].Events&unix.EPOLLIN == 0 {
		t.Fatalf("a wait woken by a write: %d %v %v", k, events[:k], err)
	}
	time.Sleep(20 * time.Millisecond)
	if k, err := o.Read(efd, b); err != nil || k != 8 || binary.LittleEndian.Uint64(b) != 4 {
		t.Fatalf("a read of the eventfd: %d %d %v", k, binary.LittleEndian.Uint64(b), err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_MOD, efd, unix.EPOLLOUT, 8); err != nil {
		t.Fatal(err)
	}
	if k, err := o.EpollWait(ep, events, 0); err != nil || k != 1 || events[0].Fd != 8 || events[0].Events != unix.EPOLLOUT {
		t.Fatalf("an eventfd is always writable: %d %v %v", k, events[:k], err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_DEL, efd, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_MOD, efd, unix.EPOLLIN, 7); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("modifying a descriptor not added: %v", err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_ADD, ep, unix.EPOLLIN, 1); !errors.Is(err, unix.EPERM) {
		t.Fatalf("an epoll in an epoll: %v", err)
	}
	// A blocking eventfd's read waits for a write.
	bfd, err := o.Eventfd(false)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], 1)
		_, _ = o.Write(bfd, w[:])
	}()
	if k, err := o.Read(bfd, b); err != nil || k != 8 || binary.LittleEndian.Uint64(b) != 1 {
		t.Fatalf("a blocking read of an eventfd: %d %v", k, err)
	}
	// A descriptor closed while added drops out of the epoll.
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_ADD, bfd, unix.EPOLLOUT, 9); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(bfd); err != nil {
		t.Fatal(err)
	}
	if k, err := o.EpollWait(ep, events, 0); err != nil || k != 0 {
		t.Fatalf("a wait after its only descriptor closed: %d %v", k, err)
	}
}

// A connection a node dialed or accepted works as the system's socket
// once taken as a descriptor: non-blocking, a read with nothing there is
// EAGAIN and one after the other end closed is 0; blocking, a read waits
// for what comes, or its timeout's EAGAIN; epoll reports it readable once
// bytes arrive. A connection of another network is refused.
func TestASocketBehavesAsTheSystems(t *testing.T) {
	o := New()
	nw := newMemNet()
	ln := nw.listen("b:1")
	a, b := o.NewNode("/a", simfs.New(), nw), o.NewNode("/b", simfs.New(), nw)
	ctx := t.Context()
	nc, err := a.Dial(ctx, "b:1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	afd, err := a.ConnFD(nc)
	if err != nil {
		t.Fatal(err)
	}
	bfd, err := b.ConnFD(accepted)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 16)
	if _, err := o.Read(bfd, p); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("a read of a non-blocking socket with nothing there: %v", err)
	}
	ep, err := o.EpollCreate()
	if err != nil {
		t.Fatal(err)
	}
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_ADD, bfd, unix.EPOLLIN|unix.EPOLLRDHUP, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Write(afd, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	events := make([]unix.EpollEvent, 2)
	if k, err := o.EpollWait(ep, events, 1000); err != nil || k != 1 || events[0].Fd != 3 {
		t.Fatalf("epoll on a socket that got bytes: %d %v %v", k, events[:k], err)
	}
	if k, err := o.Read(bfd, p); err != nil || string(p[:k]) != "ping" {
		t.Fatalf("a read: %q %v", p[:k], err)
	}
	if err := b.Blocking(bfd, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Read(bfd, p); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("a blocking read past its timeout: %v", err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = o.Write(afd, []byte("pong"))
	}()
	if k, err := o.Read(bfd, p); err != nil || string(p[:k]) != "pong" {
		t.Fatalf("a blocking read that waits for its bytes: %q %v", p[:k], err)
	}
	if err := b.Nonblocking(bfd); err != nil {
		t.Fatal(err)
	}
	if err := a.Blocking(afd, 0); err != nil {
		t.Fatal(err)
	}
	if k, err := o.Write(afd, []byte("x")); err != nil || k != 1 {
		t.Fatalf("a blocking write: %d %v", k, err)
	}
	// Registered for EPOLLRDHUP alone, as a node registers a connection it
	// stopped reading, it is woken by the other end's close and not by
	// bytes (#689's re-check of d33d3711: woken by them, the node read on).
	if err := o.EpollCtl(ep, unix.EPOLL_CTL_MOD, bfd, unix.EPOLLRDHUP, 3); err != nil {
		t.Fatal(err)
	}
	if k, err := o.EpollWait(ep, events, 0); err != nil || k != 0 {
		t.Fatalf("epoll for EPOLLRDHUP alone on a socket with bytes to read: %d %v %v", k, events[:k], err)
	}
	if err := o.Close(afd); err != nil {
		t.Fatal(err)
	}
	if k, err := o.EpollWait(ep, events, 1000); err != nil || k != 1 || events[0].Events != unix.EPOLLRDHUP {
		t.Fatalf("epoll for EPOLLRDHUP alone once the other end closed: %d %v %v", k, events[:k], err)
	}
	if k, err := o.Read(bfd, p); err != nil || string(p[:k]) != "x" {
		t.Fatalf("the read of what came before the close: %q %v", p[:k], err)
	}
	if k, err := o.Read(bfd, p); err != nil || k != 0 {
		t.Fatalf("a read after the other end closed: %d %v", k, err)
	}
	if err := b.Blocking(ep, 0); !errors.Is(err, unix.ENOTSOCK) {
		t.Fatalf("an epoll taken for a socket: %v", err)
	}
	other, _ := net.Pipe()
	if _, err := a.ConnFD(other); err == nil {
		t.Fatal("a connection of another network was taken")
	}
}
