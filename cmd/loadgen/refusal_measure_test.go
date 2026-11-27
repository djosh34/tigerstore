package main

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

func TestResourceExhaustedUsesTheIntendedSendCohort(t *testing.T) {
	s := stats{winStart: 100, winStop: 200, errs: map[string]int64{}}
	busy := grpcError("8", "etcdserver: too many requests")
	for _, tc := range []struct {
		intended, at int64
		err          error
	}{
		{99, 150, busy},  // completed in the window, but not in the cohort
		{100, 300, busy}, // late refusal still belongs to intended-send cohort
		{199, 300, fmt.Errorf("txn: %w", busy)},
		{200, 250, busy}, // exact right endpoint excluded
		{150, 160, grpcError("14", "unavailable")},
		{160, 350, nil}, // late success: p95 cohort, not completion goodput
		{90, 170, nil},  // successful completion goodput, not p95 cohort
	} {
		s.complete(&stream{intended: tc.intended, sent: tc.intended, msgs: 1}, tc.at, tc.err)
	}
	if s.cohort != 4 || s.cohortFailed != 3 || s.resourceExhaustedCohort != 2 || s.ops != 1 || s.acked != 2 || s.resp.summary().Count != 1 {
		t.Fatalf("cohort/outcomes/histogram mixed windows: %+v", s)
	}
	r := (&session{cfg: config{duration: 1}, workers: []*worker{{c: &conn{}, stats: s}}}).summarize(1, [2]cpuSample{})
	if r.ResourceExhaustedCohort != 2 || r.Cohort != 4 || r.CohortFailed != 3 || r.Latency.Count != 1 {
		t.Fatalf("lost refusal aggregation: %+v", r)
	}
}

func TestBusyOnlyNativeInvalidityDoesNotHideCPUFailure(t *testing.T) {
	for _, pid := range []int{os.Getpid(), 1 << 30} {
		fs := startFake(t, 0, time.Second)
		fs.busyEvery.Store(2)
		res, err := run(config{endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 10, conns: 1, streams: 4, warmup: 50 * time.Millisecond, duration: 100 * time.Millisecond, pids: []int{pid}, seed: 1})
		if err == nil || res == nil || res.Valid || res.ResourceExhaustedCohort <= 0 || res.ResourceExhaustedCohort != res.CohortFailed || res.Latency.Count != uint64(res.Cohort-res.CohortFailed) {
			t.Fatalf("invalid busy outcome/counter/sample: error=%v result=%+v", err, res)
		}
		if pid == os.Getpid() && !slices.Equal(res.InvalidCauses, []string{"request_errors"}) {
			t.Fatalf("busy-only native causes: %v", res.InvalidCauses)
		}
		if pid != os.Getpid() && (!slices.Contains(res.InvalidCauses, "server_cpu") || !slices.Contains(res.InvalidCauses, "request_errors")) {
			t.Fatalf("CPU error hidden by busy: %v", res.InvalidCauses)
		}
	}
}

func TestResourceExhaustedCountsStatusNotErrorText(t *testing.T) {
	s := stats{winStart: 100, winStop: 200, errs: map[string]int64{}}
	for _, err := range []error{grpcError("8", "message too large"), errors.New("grpc ResourceExhausted: etcdserver: too many requests"), grpcError("14", "etcdserver: too many requests")} {
		s.complete(&stream{intended: 150, msgs: 1}, 160, err)
	}
	if s.resourceExhaustedCohort != 1 || s.cohortFailed != 3 || s.resp.summary().Count != 0 {
		t.Fatalf("status counter/histogram incorrect: %+v", s)
	}
}
