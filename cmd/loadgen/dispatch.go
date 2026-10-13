package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// dispatch has one writer, independent response readers, and no per-call
// goroutines or channels. Request tickets select endpoints strictly round
// robin. Within that endpoint it uses the next available connection. A full
// endpoint stalls admission instead of silently sending to a faster endpoint.
// Frames accumulate per connection and flush in batches, so wire timestamps
// across different sockets do not define the ticket order.
func dispatch(cfg config, workers []*worker, loads []load, wake <-chan struct{}, duration time.Duration, total int64) (*shapeResult, [2]cpuSample, error) {
	out := &shapeResult{Concurrency: cfg.concurrency, Arrival: "closed-loop", Dispatch: "global-ticket-round-robin"}
	groups := make([][]int, len(cfg.endpoints))
	for i := range workers {
		groups[i%len(groups)] = append(groups[i%len(groups)], i)
	}
	cursor := make([]int, len(groups))
	var meter inflightCounter
	var samples [2]cpuSample
	before, sampleErr := sampleCPU(cfg.pids)
	start := now()
	startWall := time.Now().UnixNano()
	stop := start + int64(duration)
	if total > 0 {
		stop = int64(^uint64(0) >> 1)
	}
	for _, wk := range workers {
		wk.stats.winStart, wk.stats.winStop = start, stop
		wk.stats.active = &meter
	}
	schedule := fixedSchedule{start: start, stop: stop, rate: cfg.rate}
	var expected int64
	if cfg.rate > 0 {
		out.Arrival = "fixed"
		expected = schedule.count()
	}
	// A separate timer samples the completion window even when a socket
	// write blocks. It never sends RPCs, and joins before done publication.
	var sampler sync.WaitGroup
	var window cpuSample
	var windowErr error
	if total == 0 {
		sampler.Go(func() {
			time.Sleep(time.Duration(stop - now()))
			window, windowErr = sampleCPU(cfg.pids)
		})
	}
	var sent, sentBeforeStop int64
	var phaseErr error
	lastQueueAt := start
	lastDepth := int64(0)
	var queueArea float64
	observe := func(at int64, due int64, force bool) {
		depth := max(due-sent, 0)
		out.QueueMax = max(out.QueueMax, depth)
		if !force && at-lastQueueAt < int64(100*time.Millisecond) {
			return
		}
		queueArea += float64(at-lastQueueAt) * float64(lastDepth+depth) / 2
		lastQueueAt, lastDepth = at, depth
		out.Queue = append(out.Queue, queueSample{
			ElapsedS: float64(at-start) / 1e9, Scheduled: due, Sent: sent, Queued: depth, Inflight: meter.current.Load(),
		})
	}
	stopObserved := false
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		t := now()
		due := sent
		if cfg.rate > 0 {
			due = schedule.due(t)
		}
		if total == 0 && t >= stop && !stopObserved {
			out.QueueAtStop = max(due-sent, 0)
			out.InflightAtStop = meter.current.Load()
			stopObserved = true
			observe(t, due, true)
		} else {
			observe(t, due, false)
		}
		// Bound each dispatch turn by bytes and count. This also ensures
		// control frames and queue observations get turns at high load.
		admitted := 0
		for admitted < 256 && meter.current.Load() < int64(cfg.concurrency) {
			t = now()
			if cfg.rate > 0 {
				due = schedule.due(t)
				if sent >= due {
					break
				}
			} else if (total > 0 && sent >= total) || (total == 0 && t >= stop) {
				break
			}
			ep := int(sent % int64(len(groups)))
			group := groups[ep]
			started := false
			for offset := range len(group) {
				pos := (cursor[ep] + offset) % len(group)
				i := group[pos]
				wk, l := workers[i], loads[i]
				c := wk.c
				c.mu.Lock()
				if c.err != nil {
					phaseErr = c.err
					c.mu.Unlock()
					break
				}
				if len(c.out) >= 256<<10 {
					c.mu.Unlock()
					continue
				}
				st := c.newStream(l)
				st.intended, st.sent = t, now()
				if cfg.rate > 0 {
					st.intended = schedule.at(sent)
				}
				if l.start(c, st) {
					if st.sent < stop {
						sentBeforeStop++
					}
					meter.started()
					wk.stats.sent++
					started = true
				} else {
					c.free = append(c.free, st)
				}
				c.mu.Unlock()
				if started {
					cursor[ep] = (pos + 1) % len(group)
					break
				}
			}
			if phaseErr != nil || !started {
				break
			}
			sent++
			admitted++
		}
		if phaseErr != nil {
			break
		}
		for _, wk := range workers {
			c := wk.c
			c.mu.Lock()
			c.appendControl()
			err := c.err
			c.mu.Unlock()
			if err == nil {
				err = c.flush()
			}
			if err != nil {
				phaseErr = err
				break
			}
		}
		if phaseErr != nil {
			break
		}
		t = now()
		arrivalsOver := t >= stop
		if total > 0 {
			arrivalsOver = sent == total
		} else if cfg.rate > 0 {
			arrivalsOver = arrivalsOver && sent == expected
		}
		if arrivalsOver && meter.current.Load() == 0 {
			break
		}
		// A finite closed-loop cohort has a timeout too, measured from
		// its start rather than a duration it has intentionally replaced.
		deadline := stop + int64(cfg.drain)
		if total > 0 {
			deadline = start + int64(preloadTimeout)
		}
		if t >= deadline {
			phaseErr = fmt.Errorf("drain timed out with %d in flight and %d unsent", meter.current.Load(), max(expected-sent, 0))
			break
		}
		if admitted == 256 {
			continue
		}
		wait := min(deadline, lastQueueAt+int64(100*time.Millisecond))
		if total == 0 && t < stop {
			wait = min(wait, stop)
		}
		if cfg.rate > 0 && sent >= schedule.due(t) && sent < expected {
			wait = min(wait, schedule.at(sent))
		}
		if wait <= t {
			continue
		}
		timer.Reset(time.Duration(wait - t))
		select {
		case <-wake:
		case <-timer.C:
		}
	}
	if cfg.rate == 0 {
		expected = sent
		if total > 0 {
			expected = total
		}
	}
	if phaseErr != nil {
		// End and account every outstanding request before done. No late
		// response may turn a censored request into a success afterward.
		for _, wk := range workers {
			c := wk.c
			c.mu.Lock()
			for id, st := range c.streams {
				delete(c.streams, id)
				st.h.done(st, now(), phaseErr)
			}
			c.mu.Unlock()
		}
		// Unsent tickets still belong to the scheduled measured cohort.
		for ep, group := range groups {
			n := ticketsForEndpoint(expected, ep, len(groups)) - ticketsForEndpoint(sent, ep, len(groups))
			wk := workers[group[0]]
			wk.stats.cohort += n
			wk.stats.cohortFailed += n
			wk.unsent += n
		}
		for _, wk := range workers {
			wk.c.close()
		}
	}
	end := now()
	after, drainErr := sampleCPU(cfg.pids)
	if total > 0 {
		stop = end
		window, windowErr = after, drainErr
	}
	sampler.Wait()
	samples = [2]cpuSample{before, window}
	for _, wk := range workers {
		wk.c.mu.Lock()
		wk.stats.active = nil
		wk.c.mu.Unlock()
	}
	out.Expected, out.PeakInflight = expected, meter.peak.Load()
	var completedWindow int64
	for _, wk := range workers {
		completedWindow += wk.stats.completedWindow
	}
	// Derive the stop boundary from timestamped admission/completion
	// counts, not a dispatcher observation that may happen during drain.
	out.InflightAtStop = max(sentBeforeStop-completedWindow, 0)
	if cfg.rate > 0 {
		out.QueueAtStop = expected - sentBeforeStop
	}
	for ep, group := range groups {
		counts := endpointCounts{Endpoint: cfg.endpoints[ep]}
		for _, i := range group {
			wk := workers[i]
			counts.Sent += wk.stats.sent
			counts.Success += wk.stats.cohortSuccess
			counts.Failed += wk.stats.cohortFailed - wk.unsent
			counts.Unsent += wk.unsent
		}
		out.Success += counts.Success
		out.Endpoints = append(out.Endpoints, counts)
	}
	if cfg.workload == "put" {
		out.ExpectedRevDelta = expected
	}
	out.StartUnixNS, out.StopUnixNS, out.DoneUnixNS = startWall, startWall+stop-start, startWall+end-start
	out.ArrivalWindowS = float64(stop-start) / 1e9
	out.ElapsedS = float64(end-start) / 1e9
	out.DrainS = float64(max(end-stop, 0)) / 1e9
	out.CohortThroughput = float64(out.Success) / out.ElapsedS
	observe(end, expected, true)
	out.QueueMean = queueArea / float64(max(end-start, 1))
	if sampleErr == nil && drainErr == nil {
		out.CPUThroughDrain = cpuThroughDrain(before, after, out.Success)
	}
	return out, samples, errors.Join(phaseErr, sampleErr, windowErr, drainErr)
}

func ticketsForEndpoint(n int64, ep, endpoints int) int64 {
	return n/int64(endpoints) + boolInt(int64(ep) < n%int64(endpoints))
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func cpuThroughDrain(before, after cpuSample, success int64) *cpuResult {
	out := &cpuResult{ElapsedS: float64(after.at-before.at) / 1e9}
	if success == 0 {
		return out
	}
	for i := range before.servers {
		out.Server = append(out.Server, float64((after.servers[i]-before.servers[i]).Nanoseconds())/1e3/float64(success))
	}
	out.Client = float64((after.self - before.self).Nanoseconds()) / 1e3 / float64(success)
	return out
}
