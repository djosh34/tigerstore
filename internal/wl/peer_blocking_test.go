//go:build linux && !(sim || simio)

package wl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A full send buffer makes the blocking write reproducibly interruptible
// before it sends any bytes. flush already retries EINTR: this control
// must keep working while the blocking read side is fixed.
func TestAnInterruptedBlockingFlushAllWritesEveryByte(t *testing.T) {
	fd, peer, reader := peerTestSocketPair(t, snapTimeout)
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
		t.Fatal(err)
	}
	filled := fillPeerSendBuffer(t, fd)
	if err := (sysEnv{}).blocking(fd, snapTimeout); err != nil {
		t.Fatal(err)
	}
	if err := (sysEnv{}).blocking(peer, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("p"), 32<<10)
	s := newSock(&handoff{fd: fd, plain: true}, &counters{})
	s.put(payload)
	tid, flushed, done := peerLockedCall(func() error { return flushAll(s) })
	t.Cleanup(func() { finishPeerCall(t, fd, done) })
	waitForPeerSocketCall(t, tid, unix.SYS_WRITE, fd, len(payload), snapTimeout)
	if err := unix.Tgkill(os.Getpid(), tid, unix.SIGURG); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushed:
		t.Fatalf("the interrupted flush returned while the send buffer was still full: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	want := append(bytes.Repeat([]byte("q"), filled), payload...)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(reader, got); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("the interrupted blocking write failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the flush did not finish after its receiver drained")
	}
	if !bytes.Equal(got, want) || len(s.out) != 0 || s.ow != 0 || s.stats.writes < 2 {
		t.Fatal("the interrupted write lost bytes, did not retry, or left unsent output")
	}
}

// EAGAIN on a timed blocking read must expire the exchange even when a
// TLS record header is already buffered. Retrying a timed-out partial
// record used to restart the socket timeout over and over.
func TestABlockingPeerReadTimeoutStillExpires(t *testing.T) {
	for _, which := range []string{"empty", "partial header", "partial body", "EOF"} {
		t.Run(which, func(t *testing.T) {
			const timeout = 50 * time.Millisecond
			fd, peer, _ := peerTestSocketPair(t, timeout)
			var prefix []byte
			switch which {
			case "partial header":
				prefix = []byte{typeAppData, 3, 3}
			case "partial body":
				prefix = []byte{typeAppData, 3, 3, 0, 32}
			case "EOF":
				if err := unix.Shutdown(peer, unix.SHUT_WR); err != nil {
					t.Fatal(err)
				}
			}
			if len(prefix) > 0 {
				if n, err := unix.Write(peer, prefix); err != nil || n != len(prefix) {
					t.Fatalf("write a partial record: %d bytes, %v", n, err)
				}
			}
			s := newSock(&handoff{fd: fd}, &counters{})
			_, result, done := peerLockedCall(func() error {
				_, err := readFrame(s)
				return err
			})
			t.Cleanup(func() { finishPeerCall(t, fd, done) })
			var err error
			select {
			case err = <-result:
			case <-time.After(2 * time.Second):
				_ = unix.Shutdown(fd, unix.SHUT_RDWR)
				<-done
				t.Fatal("a timed-out partial record kept restarting the blocking read")
			}
			if which == "EOF" {
				if !errors.Is(err, io.EOF) || errors.Is(err, errSnapTimeout) {
					t.Fatalf("EOF was mistaken for a timeout: %v", err)
				}
			} else if !errors.Is(err, errSnapTimeout) {
				t.Fatalf("a real socket timeout was not reported: %v", err)
			}
		})
	}
}

// Retrying an interrupt must still perform the timed read, not turn
// EAGAIN into another retry or return the timeout before that read.
func TestAnInterruptedBlockingReadStillTimesOut(t *testing.T) {
	const timeout = 250 * time.Millisecond
	fd, _, _ := peerTestSocketPair(t, timeout)
	s := newSock(&handoff{fd: fd}, &counters{})
	tid, result, done := peerLockedCall(func() error {
		_, err := readFrame(s)
		return err
	})
	t.Cleanup(func() { finishPeerCall(t, fd, done) })
	waitForPeerSocketCall(t, tid, unix.SYS_READ, fd, inBuf, timeout)
	started := time.Now()
	if err := unix.Tgkill(os.Getpid(), tid, unix.SIGURG); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errSnapTimeout) || s.stats.reads < 2 || time.Since(started) < timeout/2 {
			t.Fatalf("the interrupted read did not retry until its real timeout: reads %d, elapsed %v, error %v", s.stats.reads, time.Since(started), err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the interrupted read retried past its real timeout")
	}
}

func TestABlockingPeerWriteTimeoutStillExpires(t *testing.T) {
	const timeout = 50 * time.Millisecond
	fd, _, _ := peerTestSocketPair(t, timeout)
	fillPeerSendBuffer(t, fd)
	if err := (sysEnv{}).blocking(fd, timeout); err != nil {
		t.Fatal(err)
	}
	s := newSock(&handoff{fd: fd, plain: true}, &counters{})
	s.put([]byte("a frame whose receiver never drains"))
	_, result, done := peerLockedCall(func() error { return flushAll(s) })
	t.Cleanup(func() { finishPeerCall(t, fd, done) })
	select {
	case err := <-result:
		if !errors.Is(err, errSnapTimeout) || s.ow != 0 || len(s.out) == 0 {
			t.Fatalf("a real blocking write timeout lost its pending bytes or did not expire: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the blocking write did not expire")
	}
}

// The accept goroutine blocks while it reads a peer hello, before the
// connection belongs to the loop. An interrupt must not close that peer.
func TestAPeerHelloSurvivesAnInterruptedBlockingRead(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	l := &loop{n: &node{clusterID: 17}, ctx: context.Background(), hoFD: -1, logger: log.New(io.Discard, "", 0)}
	tid, done := make(chan int, 1), make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		tid <- unix.Gettid()
		l.peerHello(&handoff{fd: fds[0], plain: true})
	}()
	t.Cleanup(func() {
		// peerHello closes its fd on failure and hands ownership to the
		// loop on success. Never close a failed fd's reused number.
		_ = unix.Close(fds[1])
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the hello did not stop after its sender closed")
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, h := range l.handoffs {
			_ = sysClose(h.fd)
		}
	})
	thread := <-tid
	waitForPeerSocketCall(t, thread, unix.SYS_READ, fds[0], inBuf, snapTimeout)
	if err := unix.Tgkill(os.Getpid(), thread, unix.SIGURG); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("an interrupt closed the incoming peer before its hello arrived")
	case <-time.After(100 * time.Millisecond):
	}
	hello := peerHelloBytes(2, 17, "")
	if n, err := unix.Write(fds[1], hello); err != nil || n != len(hello) {
		t.Fatalf("write the peer hello after an interrupt: %d bytes, %v", n, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the hello did not hand its peer to the loop")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.handoffs) != 1 || l.handoffs[0].peer != 2 || l.handoffs[0].fd != fds[0] {
		t.Fatal("the interrupted hello did not preserve its connection and identity")
	}
}

// Force EINTR with a blocking fd solely to check the loop method's
// existing contract: it yields without retrying. Real loop sockets stay
// non-blocking; a separate empty-read control checks EAGAIN there too.
func TestTheLoopReadStillYieldsAfterAnInterrupt(t *testing.T) {
	fd, _, _ := peerTestSocketPair(t, snapTimeout)
	s := newSock(&handoff{fd: fd}, &counters{})
	got := false
	tid, result, done := peerLockedCall(func() error {
		var err error
		got, err = s.readSock()
		return err
	})
	t.Cleanup(func() { finishPeerCall(t, fd, done) })
	waitForPeerSocketCall(t, tid, unix.SYS_READ, fd, inBuf, snapTimeout)
	if err := unix.Tgkill(os.Getpid(), tid, unix.SIGURG); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil || got || s.stats.reads != 1 {
			t.Fatalf("the loop's interrupted read changed behavior: got %v, reads %d, error %v", got, s.stats.reads, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the loop method retried instead of yielding on EINTR")
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	if got, err := s.readSock(); got || err != nil {
		t.Fatalf("the non-blocking EAGAIN changed behavior: got %v, error %v", got, err)
	}
}

func peerTestSocketPair(t *testing.T, timeout time.Duration) (int, int, *os.File) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	writer, reader := os.NewFile(uintptr(fds[0]), "peer-writer"), os.NewFile(uintptr(fds[1]), "peer-reader")
	t.Cleanup(func() {
		_ = writer.Close()
		_ = reader.Close()
	})
	for _, fd := range fds {
		if err := (sysEnv{}).blocking(fd, timeout); err != nil {
			t.Fatal(err)
		}
	}
	return fds[0], fds[1], reader
}

func fillPeerSendBuffer(t *testing.T, fd int) int {
	t.Helper()
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	p := bytes.Repeat([]byte("q"), 4096)
	total := 0
	for {
		n, err := unix.Write(fd, p)
		if n > 0 {
			total += n
		}
		if errors.Is(err, unix.EAGAIN) {
			if len(p) > 1 {
				p = p[:1]
				continue
			}
			return total
		}
		if err != nil && !errors.Is(err, unix.EINTR) {
			t.Fatal(err)
		}
		if total > 2*inBuf {
			t.Fatal("the socket's send buffer never filled")
		}
	}
}

func peerLockedCall(work func() error) (int, <-chan error, <-chan struct{}) {
	tid, result, done := make(chan int, 1), make(chan error, 1), make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		tid <- unix.Gettid()
		result <- work()
	}()
	return <-tid, result, done
}

func finishPeerCall(t *testing.T, fd int, done <-chan struct{}) {
	t.Helper()
	// Shutdown wakes a still-blocked call without closing a descriptor
	// it might otherwise use after another thread reuses its number.
	_ = unix.Shutdown(fd, unix.SHUT_RDWR)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("the blocking peer call did not stop after shutdown")
	}
}
