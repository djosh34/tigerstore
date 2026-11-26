//go:build linux && !(sim || simio)

package wl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A members request uses readFrame on a socket with SO_RCVTIMEO. A signal
// interrupting that read must not be mistaken for an expired timeout.
// Hold the real node's loop so its answer cannot race the forced EINTR.
func TestRemoteMembersSurvivesAnInterruptedBlockingRead(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		name := "no signal"
		if interrupt {
			name = "SIGURG during blocking read"
		}
		t.Run(name, func(t *testing.T) { remoteMembersBlockingRead(t, interrupt) })
	}
}

func remoteMembersBlockingRead(t *testing.T, interrupt bool) {
	t.Helper()
	loopOf := watchLoops(t)
	tc := newTestClusterOf(t, 1, nil)
	_ = tc.clientOf(t, tc.endpoints[0])
	ptls, err := loadTLS(tc.certFile, tc.keyFile, tc.caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	l := loopOf(tc.ids[0])
	if l == nil {
		t.Fatal("the member's loop was not observed")
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	snapshotWork := make(chan bool, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	l.post(func() error {
		snapshotWork <- l.n.stats.ckpts != 0 || l.n.stats.snapsSent != 0 || l.n.stats.snapsInstalled != 0 || len(l.n.snapWaiters) != 0 || len(l.n.clientSnaps) != 0
		close(blocked)
		<-release
		return nil
	})
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the member's loop did not enter the controlled pause")
	}
	if <-snapshotWork {
		t.Fatal("the reproduction unexpectedly ran checkpoint or snapshot work")
	}
	type answer struct {
		members []Member
		cluster uint64
		err     error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tid := make(chan int, 1)
	got := make(chan answer, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		tid <- unix.Gettid()
		ms, cluster, err := RemoteMembers(ctx, nil, ptls, []string{"127.0.0.1:1", strings.TrimPrefix(tc.peers[0], "https://")})
		got <- answer{members: ms, cluster: cluster, err: err}
	}()
	thread := <-tid
	fd := waitForMembersSocketRead(t, thread)
	var a answer
	returned := false
	if interrupt {
		started := time.Now()
		if err := unix.Tgkill(os.Getpid(), thread, unix.SIGURG); err != nil {
			t.Fatal(err)
		}
		select {
		case a = <-got:
			returned = true
			t.Logf("interrupted members read: fd %d, SO_RCVTIMEO %v, early result after %v, is errSnapTimeout %v, error %v", fd, snapTimeout, time.Since(started), errors.Is(a.err, errSnapTimeout), a.err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	unblock()
	if !returned {
		select {
		case a = <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("the members request did not finish after the loop resumed")
		}
	}
	if a.err != nil || a.cluster != 0x7769726531 || len(a.members) != 1 {
		t.Fatalf("RemoteMembers did not survive a signal-interrupted read: members %v, cluster %x, error %v", a.members, a.cluster, a.err)
	}
}

// Confirm the syscall result that readSock erases: SO_RCVTIMEO makes a
// signal interrupt this blocking socket read with EINTR, not a timeout.
func TestTimedMembersSocketReadReportsEINTR(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
	})
	if err := (sysEnv{}).blocking(fds[0], snapTimeout); err != nil {
		t.Fatal(err)
	}
	tid, result := make(chan int, 1), make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		tid <- unix.Gettid()
		_, err := sysRead(fds[0], make([]byte, inBuf))
		result <- err
	}()
	thread := <-tid
	fd := waitForMembersSocketRead(t, thread)
	if fd != fds[0] {
		t.Fatal("observed another thread's socket")
	}
	if err := unix.Tgkill(os.Getpid(), thread, unix.SIGURG); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, unix.EINTR) {
			t.Fatalf("the signal did not interrupt the timed socket read: %v", err)
		}
		t.Logf("raw read with SO_RCVTIMEO=%v returned %v immediately after SIGURG", snapTimeout, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the signal did not interrupt the blocked read")
	}
}

// Observe the exact raw read used after handshake, not a timer-based
// guess: the members client owns the fd throughout.
func waitForMembersSocketRead(t *testing.T, tid int) int {
	t.Helper()
	return waitForPeerSocketCall(t, tid, unix.SYS_READ, -1, inBuf, snapTimeout)
}

// waitForPeerSocketCall waits for an owned thread's timed, blocking
// syscall. It opens no copy of the socket and never closes that fd.
func waitForPeerSocketCall(t *testing.T, tid, call, fd, count int, timeout time.Duration) int {
	t.Helper()
	path := fmt.Sprintf("/proc/self/task/%d/syscall", tid)
	for until := time.Now().Add(5 * time.Second); time.Now().Before(until); time.Sleep(time.Millisecond) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(b))
		if len(fields) < 4 {
			continue
		}
		observed, err := strconv.ParseUint(fields[0], 0, 64)
		if err != nil || observed != uint64(call) {
			continue
		}
		at, err := strconv.ParseUint(fields[1], 0, 32)
		if err != nil || fd >= 0 && at != uint64(fd) {
			continue
		}
		n, err := strconv.ParseUint(fields[3], 0, 64)
		if err != nil || n != uint64(count) {
			continue
		}
		option := unix.SO_RCVTIMEO
		if call == unix.SYS_WRITE {
			option = unix.SO_SNDTIMEO
		}
		tv, err := unix.GetsockoptTimeval(int(at), unix.SOL_SOCKET, option)
		if err != nil {
			continue
		}
		gotTimeout := time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
		if gotTimeout < timeout || gotTimeout > timeout+20*time.Millisecond {
			continue
		}
		flags, err := unix.FcntlInt(uintptr(at), unix.F_GETFL, 0)
		if err == nil && flags&unix.O_NONBLOCK == 0 {
			return int(at)
		}
	}
	t.Fatalf("thread %d never entered blocking syscall %d on fd %d", tid, call, fd)
	return -1
}
