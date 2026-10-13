package main

import (
	"os"
	"testing"
	"time"
)

func TestDispatchArrivalBecomesDueDuringFlush(t *testing.T) {
	s := fixedSchedule{start: 1234567890, stop: 1234567890 + int64(time.Second), rate: 5000}
	sent, expected := int64(1), s.count()
	beforeFlush := s.start + int64(100*time.Microsecond)
	waitingForArrival := sent >= s.due(beforeFlush)
	if !waitingForArrival {
		t.Fatal("fixture did not stop admission for the next arrival")
	}
	for _, afterFlush := range []int64{s.at(sent), s.at(sent) + int64(time.Millisecond)} {
		// Synthetic times make the interleaving deterministic: all outstanding
		// responses have completed during the flush, with no further wakeup.
		var inflight inflightCounter
		inflight.started()
		inflight.current.Add(-1)
		wake := make(chan struct{}, 1)
		if inflight.current.Load() != 0 || len(wake) != 0 || s.due(afterFlush) <= sent {
			t.Fatal("fixture lacks an overdue arrival and zero inflight requests")
		}
		poll := s.start + int64(100*time.Millisecond)
		wait := dispatchArrivalWait(poll, s, sent, expected, waitingForArrival)
		if wait != s.at(sent) || wait > afterFlush {
			t.Fatalf("after flush %d: wait %d, want immediate retry at %d", afterFlush, wait, s.at(sent))
		}
		// Recomputing the stop reason after flushing was the bug. It loses
		// the arrival deadline and leaves only the 100ms observation poll.
		if oldWait := dispatchArrivalWait(poll, s, sent, expected, sent >= s.due(afterFlush)); oldWait != poll {
			t.Fatal("fixture does not reproduce the old 100ms fallthrough")
		}
	}
}

func TestDispatchBlockedConnectionDoesNotSpin(t *testing.T) {
	s := fixedSchedule{start: 1234567890, stop: 1234567890 + int64(time.Second), rate: 5000}
	sent, expected := int64(1), s.count()
	afterFlush := s.start + int64(time.Millisecond)
	if sent >= s.due(afterFlush) {
		t.Fatal("fixture has no scheduled backlog")
	}
	for _, tc := range []struct {
		name string
		conn *conn
	}{
		{"flow-credit", &conn{maxStreams: 8, nextID: 1}},
		{"stream-limit", &conn{maxStreams: 1, nextID: 1, sendWin: 100, streams: map[uint32]*stream{1: {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.conn.canStart(&template{dataLen: 100}, 8) {
				t.Fatal("fixture connection is not blocked")
			}
			// Failed admission leaves waitingForArrival false, even if there
			// are no inflight requests, as with exhausted connection credit.
			poll := s.start + int64(100*time.Millisecond)
			wait := dispatchArrivalWait(poll, s, sent, expected, false)
			if wait != poll || wait <= afterFlush {
				t.Fatalf("blocked connection busy-spins: wait %d, poll %d", wait, poll)
			}
		})
	}
}

func TestDispatchArrivalWaitBoundaries(t *testing.T) {
	s := fixedSchedule{start: 1234567890, stop: 1234567890 + int64(time.Second), rate: 5000}
	poll := s.start + int64(100*time.Millisecond)
	for _, tc := range []struct {
		name     string
		schedule fixedSchedule
		sent     int64
		expected int64
		waiting  bool
		want     int64
	}{
		{"future-arrival", s, 1, s.count(), true, s.at(1)},
		{"draining-complete-cohort", s, s.count(), s.count(), true, poll},
		{"global-capacity-blocked", s, 1, s.count(), false, poll},
		{"closed-loop", fixedSchedule{}, 1, 2, true, poll},
		{"earlier-drain-deadline", s, 1000, s.count(), true, poll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if wait := dispatchArrivalWait(poll, tc.schedule, tc.sent, tc.expected, tc.waiting); wait != tc.want {
				t.Fatalf("wait %d, want %d", wait, tc.want)
			}
		})
	}
}

func TestShapedFixedRateBalance(t *testing.T) {
	var endpoints []string
	for i := range 3 {
		fs := startFake(t, 5*time.Millisecond, time.Second)
		fs.rev.Store(int64(i) * 1000000)
		endpoints = append(endpoints, fs.addr)
	}
	res, err := run(config{
		endpoints: endpoints, workload: "put", valueSize: 64, keys: 12, conns: 3, streams: 256,
		concurrency: 3, arrival: "fixed", rate: 300, duration: 40 * time.Millisecond, drain: time.Second,
		pids: []int{os.Getpid()}, seed: 1,
	})
	if err != nil || !res.Valid {
		t.Fatalf("result %+v, error %v", res, err)
	}
	if res.Cohort != 12 || res.Latency.Count != 12 || res.CohortFailed != 0 || res.Unsent != 0 || res.Shape.PeakInflight > 3 {
		t.Fatalf("fixed cohort or cap changed: %+v", res)
	}
	for i, ep := range res.Shape.Endpoints {
		want := ticketsForEndpoint(12, i, 3)
		if ep.Sent != want || ep.Success != want || ep.Failed != 0 || ep.Unsent != 0 {
			t.Fatalf("endpoint %d: %+v, want %d", i, ep, want)
		}
	}
	last := res.Shape.Queue[len(res.Shape.Queue)-1]
	if last.Scheduled != 12 || last.Sent != 12 || last.Queued != 0 || last.Inflight != 0 {
		t.Fatalf("cohort did not fully drain: %+v", last)
	}
}
