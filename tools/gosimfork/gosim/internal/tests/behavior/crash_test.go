//go:build sim

package behavior_test

import (
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func TestCrashTimer(t *testing.T) {
	var before, after atomic.Bool

	// token to allow machine to read before, after
	gosimruntime.TestRaceToken.Release()

	m1 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// test a timer can fire
		time.AfterFunc(1*time.Second, func() {
			before.Store(true)
		})
		select {}
	})

	m2 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// test a crashed timer does not fire
		time.AfterFunc(5*time.Second, func() {
			after.Store(true)
			panic("help")
		})
		select {}
	})

	time.Sleep(2 * time.Second)
	m1.Crash()
	m2.Crash()
	time.Sleep(10 * time.Second)

	if !before.Load() {
		t.Error("expected before")
	}
	if after.Load() {
		t.Error("did not expect after")
	}
}

func TestCrashTimersComplicatedHeap(t *testing.T) {
	var timers []*time.Timer
	for i := 0; i < 10; i++ {
		timers = append(timers, time.NewTimer(time.Duration(i+1)*time.Second))
	}

	m := gosim.NewSimpleMachine(func() {
		var timers []*time.Timer
		for i := 0; i < 10; i++ {
			timers = append(timers, time.NewTimer(time.Duration(i+1)*time.Second))
		}
		select {}
	})
	time.Sleep(time.Millisecond)
	m.Crash()
	for i, timer := range timers {
		if i%2 == 0 {
			timer.Stop()
		}
	}
	// XXX: when the heap indexes are not adjusted this test times out???
	// (that is "h.timers[j].pos = j")
	// XXX: check timer heap invariants
}

func TestCrashSleep(t *testing.T) {
	var before, after atomic.Bool

	// token to allow machine to read before, after
	gosimruntime.TestRaceToken.Release()

	m1 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// test Sleep works
		time.Sleep(1 * time.Second)
		before.Store(true)
	})

	m2 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// test a crashed Sleep stops
		time.Sleep(5 * time.Second)
		after.Store(true)
		panic("help")
	})

	time.Sleep(2 * time.Second)
	m1.Crash()
	m2.Crash()
	time.Sleep(10 * time.Second)

	if !before.Load() {
		t.Error("expected before")
	}
	if after.Load() {
		t.Error("did not expect after")
	}
}

func TestCrashSemaphore(t *testing.T) {
	var before, after atomic.Bool
	var sema uint32

	// token to allow machine to read before, after, sema
	gosimruntime.TestRaceToken.Release()

	m1 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()

		// test that we can acquire the semaphore
		gosimruntime.Semacquire(&sema, false)
		before.Store(true)
	})

	m2 := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()

		// test that an acquire can be crashed
		time.Sleep(2 * time.Second)
		gosimruntime.Semacquire(&sema, false)
		after.Store(true)
	})

	time.Sleep(1 * time.Second)
	// release for m1
	gosimruntime.Semrelease(&sema)

	time.Sleep(3 * time.Second)
	m1.Crash()
	m2.Crash()

	// after crash make sure sema still works
	gosimruntime.Semrelease(&sema)
	time.Sleep(time.Second)
	gosimruntime.Semacquire(&sema, false)

	if !before.Load() {
		t.Error("expected before")
	}
	if after.Load() {
		t.Error("did not expect after")
	}
}

func TestCrashChan(t *testing.T) {
	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)

	m := gosim.NewSimpleMachine(func() {
		// make sure we can recv and send channels
		ch2 <- <-ch1 + 1
		// then hang
		<-ch1
	})

	time.Sleep(time.Second)

	// successful recv and send
	ch1 <- 1
	if v := <-ch2; v != 2 {
		t.Error(v)
	}

	// wait a bit to make sure it's in the recv and crash
	time.Sleep(2 * time.Second)
	m.Crash()
	time.Sleep(2 * time.Second)

	// test channel still works and recv won't consume our value
	ch1 <- 3
	time.Sleep(5 * time.Second)
	if v := <-ch1; v != 3 {
		t.Error(v)
	}
}

func TestCrashSelect(t *testing.T) {
	ch1 := make(chan int, 1)
	ch2 := make(chan int, 0)

	m := gosim.NewSimpleMachine(func() {
		// make sure that select works
		select {
		case x := <-ch1:
			ch2 <- x + 1
		case <-ch2:
		}
		// then hang
		select {
		case <-ch1:
		case ch2 <- 1:
		}
	})

	time.Sleep(2 * time.Second)
	ch1 <- 1
	if v := <-ch2; v != 2 {
		t.Error(v)
	}

	// make sure we are in the select and crash
	time.Sleep(time.Second)
	m.Crash()
	time.Sleep(time.Second)

	// make sure chans still work
	go func() {
		ch2 <- <-ch1 + 1
	}()
	ch1 <- 4
	if v := <-ch2; v != 5 {
		t.Error(v)
	}
}

func TestCrashSyscall(t *testing.T) {
	var before, after, finally atomic.Bool

	// sleeper is a helper machine that will successfully stop
	sleeper := gosim.NewSimpleMachine(func() {
		time.Sleep(5 * time.Second)
	})

	// working machine
	gosimruntime.TestRaceToken.Release()
	waiter := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// wait for sleeper, flag success
		sleeper.Wait()
		before.Store(true)
		// then sleep, flag success after
		time.Sleep(5 * time.Second)
		finally.Store(true)
	})

	gosimruntime.TestRaceToken.Release()
	crasher := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		// wait for waiter, but will get crashed before it's done
		waiter.Wait()
		after.Store(true)
	})

	// see waiter work
	time.Sleep(2 * time.Second)
	if before.Load() {
		t.Error("did not yet expect before")
	}
	time.Sleep(4 * time.Second)
	if !before.Load() {
		t.Error("expected before")
	}

	// then crash crasher
	crasher.Crash()
	crasher.Wait()
	// make sure after did not trigger
	if after.Load() {
		t.Error("did not expect after")
	}

	// make sure waiter is still sleeping
	if finally.Load() {
		t.Error("did not yet expect finally")
	}
	// make sure wait works
	waiter.Wait()
	if !finally.Load() {
		t.Error("did expect finally")
	}
}

func TestCrashStressLightly(t *testing.T) {
	// small stress test that creates a bunch of machines interacting with
	// goroutines, timers, and channels, and crashes them hopefully catching
	// reuse problems
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		// create goroutines every 2 seconds
		time.Sleep(2 * time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var before, after atomic.Bool
			gosimruntime.TestRaceToken.Release()
			// spawn a new machine that we will run for a bit and then crash
			m := gosim.NewSimpleMachine(func() {
				gosimruntime.TestRaceToken.Acquire()
				// another goroutine for good measure
				go func() {
					// sleep a bit
					time.Sleep(1 * time.Second)
					ch := make(chan struct{}, 0)
					// make a timer writer to a channel later
					time.AfterFunc(1*time.Second, func() {
						ch <- struct{}{}
					})
					go func() {
						// sleep a bit then try and then have a timer try to write after
						time.Sleep(1 * time.Second)
						time.AfterFunc(3*time.Second, func() {
							after.Store(true)
						})
					}()
					go func() {
						// sleep a bit and then wait for a chan to maybe write after.
						// chan queues are FIFO so the code after will get to recv.
						time.Sleep(1 * time.Second)
						<-ch
						after.Store(true)
					}()
					// recv and then flag success.
					<-ch
					before.Store(true)
				}()
				select {}
			})
			// run for long enough to allow before to succeed.
			time.Sleep(3 * time.Second)
			m.Crash()
			if !before.Load() {
				t.Error("expected before")
			}
			// allow any lingering code to trigger
			time.Sleep(10 * time.Second)
			if after.Load() {
				t.Error("did not expect after")
			}
		}()
	}
	// ensure all succeed
	wg.Wait()
}

// EchoServer is a small TCP server that listens on 10.0.0.1:8080 and echoes
// all incoming bytes.
func EchoServer() {
	l, err := net.Listen("tcp", "10.0.0.1:8080") // XXX: need to specify IP? wtf?
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer conn.Close()
			for {
				var buf [1024]byte
				n, err := conn.Read(buf[:])
				if err != nil {
					log.Println(err)
					return
				}
				if _, err := conn.Write(buf[:n]); err != nil {
					log.Println(err)
					return
				}
			}
		}()
	}
}

func TestCrashServerTCPConnBreaks(t *testing.T) {
	// run echo server
	m := gosim.NewMachine(gosim.MachineConfig{
		Label:    "a",
		Addr:     netip.AddrFrom4([4]byte{10, 0, 0, 1}),
		MainFunc: EchoServer,
	})

	// let it start listening
	time.Sleep(time.Second)

	// dial it and see it echo
	conn, err := net.Dial("tcp", "10.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	var buf [5]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		t.Fatal(err)
	}
	if string(buf[:]) != "hello" {
		t.Fatal(string(buf[:]))
	}

	// let it sit and cash it
	time.Sleep(time.Second)
	m.Crash()

	// XXX: test without any data sent?

	// XXX: should have network behavior be configurable: host not responding vs rst vs ...

	// then, expect read to eventually fail after write never gets acked
	_, err = conn.Read(buf[:])
	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, syscall.EPIPE) {
		t.Errorf("expected syscall.EPIPE, got %s", err)
	}
}

// XXX: log.Fatal currently does not get reported/failed. fix that somehow?

// EchoClient is a small TCP client that connects to on 10.0.0.1:8080 and writes
// "hello" once and expects it come back.
func EchoClient() {
	conn, err := net.Dial("tcp", "10.0.0.1:8080")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		log.Fatal(err)
	}
	var buf [5]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		log.Fatal(err)
	}
	if string(buf[:]) != "hello" {
		log.Fatal(string(buf[:]))
	}
}

func TestCrashClientTCPConnBreaks(t *testing.T) {
	// run echo server
	var done atomic.Bool
	gosimruntime.TestRaceToken.Release()
	gosim.NewMachine(gosim.MachineConfig{
		Label: "a",
		Addr:  netip.AddrFrom4([4]byte{10, 0, 0, 1}),
		MainFunc: func() {
			gosimruntime.TestRaceToken.Acquire()

			l, err := net.Listen("tcp", "10.0.0.1:8080") // XXX: need to specify IP? wtf?
			if err != nil {
				log.Fatal(err)
			}

			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			var buf [5]byte
			n, err := io.ReadFull(conn, buf[:])
			if err != nil {
				log.Println(err)
				return
			}
			if _, err := conn.Write(buf[:n]); err != nil {
				log.Println(err)
				return
			}

			// then, expect (eventually) an error because the client crashed and the network disconnected
			_, err = conn.Read(buf[:])
			if err == nil {
				t.Error("expected error")
			}
			if !errors.Is(err, syscall.EPIPE) {
				t.Errorf("expected syscall.EPIPE, got %s", err)
			}
			done.Store(true)
		},
	})

	// let it start listening
	time.Sleep(time.Second)

	client := gosim.NewSimpleMachine(EchoClient)

	// let it connect and do the echo
	time.Sleep(time.Second)

	// then crash the client
	client.Crash()

	// wait for the server to notice
	time.Sleep(time.Minute)

	if !done.Load() {
		t.Error("expected done")
	}
}

var crashTestGlobal int

func TestCrashRestartGlobals(t *testing.T) {
	var first atomic.Bool
	gosimruntime.TestRaceToken.Release()
	m := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		if crashTestGlobal != 0 {
			t.Error("expect 0")
		}
		crashTestGlobal = 1
		if crashTestGlobal != 1 {
			t.Error("expect 1")
		}
		first.Store(true)
	})
	time.Sleep(time.Second)
	m.Crash()
	if !first.Load() {
		t.Error("expected first")
	}
	var second atomic.Bool
	gosimruntime.TestRaceToken.Release()
	m.SetMainFunc(func() {
		gosimruntime.TestRaceToken.Acquire()
		if crashTestGlobal != 0 {
			t.Error("expect 0")
		}
		crashTestGlobal = 1
		if crashTestGlobal != 1 {
			t.Error("expect 1")
		}
		second.Store(true)
	})
	m.Restart()
	time.Sleep(time.Second)
	if !second.Load() {
		t.Error("expected second")
	}
}

func TestCrashRestartPendingDisk(t *testing.T) {
	var first atomic.Bool
	gosimruntime.TestRaceToken.Release()
	m := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Write([]byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := a.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		d, err := os.OpenFile(".", os.O_RDONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("b", []byte("b"), 0o644); err != nil {
			t.Fatal(err)
		}
		first.Store(true)
		gosim.CurrentMachine().Crash()
	})
	m.Wait()
	if !first.Load() {
		t.Error("expected first")
	}
	var second atomic.Bool
	gosimruntime.TestRaceToken.Release()
	m.SetMainFunc(func() {
		gosimruntime.TestRaceToken.Acquire()
		contents, err := os.ReadFile("a")
		if err != nil {
			t.Fatal(err)
		}
		if string(contents) != "a" {
			t.Fatal("bad contents")
		}
		_, err = os.ReadFile("b")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected %v, got %v", os.ErrNotExist, err)
		}
		second.Store(true)
	})
	m.Restart()
	m.Wait()
	if !second.Load() {
		t.Error("expected second")
	}
}

func checkEcho(t *testing.T) {
	t.Helper()
	// dial it and see it echo
	conn, err := net.Dial("tcp", "10.0.0.1:8080")
	defer conn.Close()

	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	var buf [5]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		t.Fatal(err)
	}
	if string(buf[:]) != "hello" {
		t.Fatal(string(buf[:]))
	}
}

func TestCrashRestartNetwork(t *testing.T) {
	// run echo server
	m := gosim.NewMachine(gosim.MachineConfig{
		Label:    "a",
		Addr:     netip.AddrFrom4([4]byte{10, 0, 0, 1}),
		MainFunc: EchoServer,
	})

	// let it start listening
	time.Sleep(time.Second)

	// check we can dial
	conn, err := net.Dial("tcp", "10.0.0.1:8080")
	if err != nil {
		t.Fatalf("expected dial success, got %v", err)
	}
	conn.Close()

	// check echo works
	checkEcho(t)

	// crash
	m.Crash()

	// check dialing fails
	_, err = net.Dial("tcp", "10.0.0.1:8080")
	if err == nil {
		t.Fatalf("expected dial failure, got %v", err)
	}

	// restart
	m.Restart() // still runs the old EchoServer

	time.Sleep(time.Second)

	// check we can dial again
	conn, err = net.Dial("tcp", "10.0.0.1:8080")
	if err != nil {
		t.Fatalf("expected dial success, got %v", err)
	}
	conn.Close()

	// check echo works again
	checkEcho(t)

	// XXX: test what happens with in-flight packets
}

func TestCrashRestartFilesystemPartialInner(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		if err := os.WriteFile("foo", []byte("hello"), 0o644); err != nil {
			// XXX: could this be log.Fatal?
			t.Fatal(err)
		}
		gosim.CurrentMachine().Crash()
	})
	m.Wait()
	// now see what we wrote to disk. see details below on scenarios.
	m.SetMainFunc(func() {
		contents, err := os.ReadFile("foo")
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				contents = []byte("<missing>")
			} else {
				t.Fatal(err)
			}
		}
		slog.Info("read", "contents", string(contents))
	})
	m.RestartWithPartialDisk()
	m.Wait()
}

func TestCrashRestartBootProgram(t *testing.T) {
	var done atomic.Bool
	gosimruntime.TestRaceToken.Release()

	m := gosim.NewSimpleMachine(func() {
		gosimruntime.TestRaceToken.Acquire()
		done.Store(true)
	})

	// give m time to change its boot program
	time.Sleep(time.Second)
	m.Crash()
	if !done.Load() {
		t.Fatal("expected done")
	}
	done.Store(false)
	m.Restart()

	time.Sleep(time.Second)
	if !done.Load() {
		t.Fatal("expected done")
	}
}

func TestCrashRestartNewBootProgram(t *testing.T) {
	var done atomic.Bool
	gosimruntime.TestRaceToken.Release()

	m := gosim.NewSimpleMachine(func() {
		gosim.CurrentMachine().SetMainFunc(func() {
			gosimruntime.TestRaceToken.Acquire()
			done.Store(true)
		})
	})

	// give m time to change its boot program
	time.Sleep(time.Second)
	m.Crash()
	if done.Load() {
		t.Fatal("did not expect done")
	}
	m.Restart()

	time.Sleep(time.Second)
	if !done.Load() {
		t.Fatal("expected done")
	}
}

// XXX: keep some ops on disk after crash?

// XXX: set a "program" that automatically starts after every restart?

// XXX: test that a grpc client will reconnect to a restarted server
