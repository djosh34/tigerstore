package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

func (cfg config) validateConnectionControl() error {
	switch {
	case len(cfg.endpoints) != 1:
		return errors.New("-dispatch connections requires exactly one endpoint")
	case cfg.rate != 0 || cfg.arrival == "fixed" || cfg.total != 0:
		return errors.New("-dispatch connections supports duration-based closed-loop runs only")
	case cfg.conns <= 0 || cfg.concurrency <= 0 || cfg.concurrency%cfg.conns != 0:
		return errors.New("-dispatch connections needs positive -concurrency exactly divisible by -conns")
	}
	return nil
}

// dispatchConnections is a generator control, not a balanced-ingress mode.
// The original workers each keep C/conns streams busy. Their admission,
// batching, wakeups and timestamps stay unchanged. The shared meter only
// observes starts/completions; it does not gate or serialize admission.
func dispatchConnections(cfg config, workers []*worker, loads []load, duration time.Duration) (*shapeResult, [2]cpuSample, error) {
	out := &shapeResult{Concurrency: cfg.concurrency, Arrival: "closed-loop", Dispatch: "static-connections"}
	for _, wk := range workers {
		wk.c.mu.Lock()
		maxStreams := wk.c.maxStreams
		wk.c.mu.Unlock()
		if uint64(cfg.concurrency/cfg.conns) > uint64(maxStreams) {
			return out, [2]cpuSample{}, fmt.Errorf("server stream limit %d is below control's %d streams per connection", maxStreams, cfg.concurrency/cfg.conns)
		}
	}
	before, beforeErr := sampleCPU(cfg.pids)
	start := now()
	startWall := time.Now().UnixNano()
	stop := start + int64(duration)
	var meter inflightCounter
	for _, wk := range workers {
		wk.stats.winStart, wk.stats.winStop = start, stop
		wk.stats.active = &meter
	}
	var window cpuSample
	var windowErr error
	var sampler sync.WaitGroup
	sampler.Go(func() {
		time.Sleep(time.Duration(stop - now()))
		window, windowErr = sampleCPU(cfg.pids)
	})
	phaseErr := runPhase(workers, loads, phase{start: start, stop: stop, drain: cfg.drain})
	if phaseErr != nil {
		// A failed write can leave streams open. End them before done so
		// late trailers cannot change the result during the parent's fence.
		for _, wk := range workers {
			c := wk.c
			c.mu.Lock()
			for id, st := range c.streams {
				delete(c.streams, id)
				st.h.done(st, now(), phaseErr)
			}
			c.mu.Unlock()
			c.close()
		}
	}
	end := now()
	after, afterErr := sampleCPU(cfg.pids)
	sampler.Wait()
	counts := endpointCounts{Endpoint: cfg.endpoints[0]}
	var completedWindow int64
	for _, wk := range workers {
		wk.c.mu.Lock()
		wk.stats.active = nil
		counts.Sent += wk.stats.sent
		counts.Success += wk.stats.cohortSuccess
		counts.Failed += wk.stats.cohortFailed - wk.unsent
		counts.Unsent += wk.unsent
		completedWindow += wk.stats.completedWindow
		wk.c.mu.Unlock()
	}
	out.Expected = counts.Sent
	out.Success = counts.Success
	out.PeakInflight = meter.peak.Load()
	out.Endpoints = []endpointCounts{counts}
	out.InflightAtStop = max(counts.Sent-completedWindow, 0)
	if cfg.workload == "put" {
		out.ExpectedRevDelta = out.Expected
	}
	out.StartUnixNS, out.StopUnixNS, out.DoneUnixNS = startWall, startWall+stop-start, startWall+end-start
	out.ArrivalWindowS = duration.Seconds()
	out.ElapsedS = float64(end-start) / 1e9
	out.DrainS = float64(max(end-stop, 0)) / 1e9
	out.CohortThroughput = float64(out.Success) / out.ElapsedS
	// This closed-loop control has no scheduled-arrival backlog. Keep the
	// same queue schema with start, arrival-stop and through-drain boundaries.
	out.Queue = []queueSample{
		{},
		{ElapsedS: duration.Seconds(), Scheduled: counts.Sent, Sent: counts.Sent, Inflight: out.InflightAtStop},
		{ElapsedS: out.ElapsedS, Scheduled: counts.Sent, Sent: counts.Sent, Inflight: meter.current.Load()},
	}
	if beforeErr == nil && afterErr == nil {
		out.CPUThroughDrain = cpuThroughDrain(before, after, out.Success)
	}
	if out.PeakInflight > int64(cfg.concurrency) || meter.current.Load() != 0 || counts.Success+counts.Failed != counts.Sent {
		phaseErr = errors.Join(phaseErr, errors.New("static-connections concurrency or outcome accounting mismatch"))
	}
	return out, [2]cpuSample{before, window}, errors.Join(phaseErr, beforeErr, windowErr, afterErr)
}
