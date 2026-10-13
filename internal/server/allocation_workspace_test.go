package server

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func allocationFixture(t *testing.T, mode string) (*Server, *conn) {
	t.Helper()
	t.Setenv("TIGER_ALLOC_STRATEGY", mode)
	s := New(nil, Config{})
	a, b := net.Pipe()
	c := newConn(s, a, a)
	t.Cleanup(func() { c.close(); c.readerDone(); _ = b.Close() })
	return s, c
}

func allocationHeaders(id uint32, timeout string) *http2.MetaHeadersFrame {
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":path", Value: "/etcdserverpb.KV/Put"}, {Name: "content-type", Value: "application/grpc"}}
	if timeout != "" {
		fields = append(fields, hpack.HeaderField{Name: "grpc-timeout", Value: timeout})
	}
	return &http2.MetaHeadersFrame{HeadersFrame: &http2.HeadersFrame{StreamID: id}, Fields: fields}
}

func assertAllocationHeld(t *testing.T, s *Server, slots uint64) {
	t.Helper()
	stats := s.AllocationStats()
	if stats["held_slots"] != slots || stats["held_bytes"] > stats["byte_budget"] || stats["peak_bytes"] > stats["byte_budget"] || stats["fallback_allocations"] != 0 {
		t.Fatalf("held slots want %d: %v", slots, stats)
	}
}

func TestAllocationWorkspaceQuotaAndFairness(t *testing.T) {
	for _, mode := range []string{"ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			s, c := allocationFixture(t, mode)
			for i := range allocationQuota {
				st := c.newStream(uint32(i*2+1), allocationHeaders(uint32(i*2+1), ""))
				if st == nil {
					t.Fatalf("quota denied at %d", i)
				}
				c.releaseReader(st)
			}
			assertAllocationHeld(t, s, allocationQuota)
			if st := c.newStream(999, allocationHeaders(999, "")); st != nil {
				t.Fatal("quota did not refuse stream")
			}
			if s.AllocationStats()["denied_count"] != 1 {
				t.Fatal("missing denial")
			}
			a, b := net.Pipe()
			other := newConn(s, a, a)
			st := other.newStream(1, allocationHeaders(1, ""))
			if st == nil {
				t.Fatal("slow connection exhausted healthy connection")
			}
			other.releaseReader(st)
			other.close()
			other.readerDone()
			_ = b.Close()
			c.close()
			c.readerDone()
			assertAllocationHeld(t, s, 0)
		})
	}
}

func TestAllocationWorkspaceResetBeforeFlushAndReaderPin(t *testing.T) {
	for _, mode := range []string{"ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			s, c := allocationFixture(t, mode)
			st := c.newStream(1, allocationHeaders(1, ""))
			// A tiny valid wire Put; it is admitted but has NOT reached Submit.
			wire := []byte{0, 0, 0, 0, 6, 10, 1, 'k', 18, 1, 'v'}
			if err := st.receive(wire); err != nil {
				t.Fatal(err)
			}
			c.admit(st)
			c.resetStream(1, 0)
			want := uint64(1)
			if mode == "arena" {
				want = arenaSlots
			}
			assertAllocationHeld(t, s, want)
			c.releaseReader(st)              // map removal did not invalidate reader's pointer
			assertAllocationHeld(t, s, want) // queued admission still owns it
			c.readerDone()                   // abort unsubmitted batch, then seal its arena
			assertAllocationHeld(t, s, 0)
		})
	}
}

func TestAllocationWorkspaceFramesPinUntilFlushOrAbort(t *testing.T) {
	for _, mode := range []string{"ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			s, c := allocationFixture(t, mode)
			st := c.newStream(1, allocationHeaders(1, ""))
			st.inEOF = true
			copy(st.slot.response[:], "immutable reply")
			c.mu.Lock()
			st.pushLocked(st.slot.response[:15], true, nil)
			c.sealArenaLocked()
			c.mu.Unlock()
			c.releaseReader(st)
			c.mu.Lock()
			frames := c.take(nil)
			c.mu.Unlock()
			if len(frames) != 3 {
				t.Fatalf("frames=%d", len(frames))
			}
			want := uint64(1)
			if mode == "arena" {
				want = arenaSlots
			}
			assertAllocationHeld(t, s, want)
			if !bytes.Equal(frames[1].b, []byte("immutable reply")) {
				t.Fatal("reply changed at endStream")
			}
			c.close() // abort cannot recycle frame aliases owned by writer
			assertAllocationHeld(t, s, want)
			c.retireFrames(frames) // same cleanup boundary used after Flush/error
			assertAllocationHeld(t, s, 0)
		})
	}
}

func TestAllocationWorkspaceActiveTimerOwnsReference(t *testing.T) {
	for _, mode := range []string{"ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			s, c := allocationFixture(t, mode)
			st := c.newStream(1, allocationHeaders(1, "1H"))
			c.mu.Lock() // let callback start, but block its push
			c.stopDeadlineLocked(st)
			st.deadline = nil
			st.slot.timerRef = true
			started := make(chan struct{})
			st.deadline = time.AfterFunc(time.Nanosecond, func() { close(started); st.expire() })
			<-started
			deadline := time.Now().Add(time.Second)
			for !st.done.Load() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !st.done.Load() {
				c.mu.Unlock()
				t.Fatal("timer callback did not start")
			}
			c.stopDeadlineLocked(st)
			if !st.slot.timerRef {
				c.mu.Unlock()
				t.Fatal("Stop(false) lost active callback reference")
			}
			st.reset = true
			st.slot.mapRef = false
			delete(c.streams, st.id)
			st.slot.readerRefs--
			c.sealArenaLocked()
			c.retireSlotLocked(st.slot)
			c.mu.Unlock()
			for s.AllocationStats()["held_slots"] != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			assertAllocationHeld(t, s, 0)
		})
	}
}

func TestAllocationWorkspaceRawTwoMiBAndNoPrimitiveChurn(t *testing.T) {
	for _, mode := range []string{"ring", "arena"} {
		t.Run(mode, func(t *testing.T) {
			s, c := allocationFixture(t, mode)
			st := c.newStream(1, allocationHeaders(1, ""))
			var prefix [5]byte
			binary.BigEndian.PutUint32(prefix[1:], 2<<20)
			if err := st.receive(prefix[:]); err != nil {
				t.Fatal(err)
			}
			if cap(st.msg) != 2<<20 {
				t.Fatalf("raw capacity=%d", cap(st.msg))
			}
			chunk := make([]byte, 16<<10)
			for range 128 {
				if err := st.receive(chunk); err != nil {
					t.Fatal(err)
				}
			}
			if len(st.body) != 2<<20 || st.msgs != 1 {
				t.Fatal("raw 2MiB body not accepted")
			}
			c.resetStream(1, 0)
			c.releaseReader(st)
			c.mu.Lock()
			c.sealArenaLocked()
			c.mu.Unlock()
			assertAllocationHeld(t, s, 0)
			allocs := testing.AllocsPerRun(100, func() {
				c.mu.Lock()
				st := c.acquireSlot()
				c.mu.Unlock()
				if st == nil {
					t.Fatal("unexpected denial")
				}
				if _, err := st.managedBody(1024); err != nil {
					t.Fatal(err)
				}
				c.mu.Lock()
				st.slot.mapRef = false
				st.slot.readerRefs--
				c.retireSlotLocked(st.slot)
				c.sealArenaLocked()
				c.mu.Unlock()
			})
			if allocs != 0 {
				t.Fatalf("bounded slot/body primitive allocations=%g", allocs)
			}
			assertAllocationHeld(t, s, 0)
		})
	}
}

func TestAllocationWorkspaceStartupBudget(t *testing.T) {
	if allocationStartupBytes > allocationBudget {
		t.Fatalf("startup=%d budget=%d", allocationStartupBytes, allocationBudget)
	}
}
