package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// These opt-in schedules need the binary built by storage-overlay.py.
var storageFaults = map[string]string{
	"rotation-power":         "rotation-rename",
	"checkpoint-power":       "checkpoint-sync",
	"clean-power":            "clean-pack-synced",
	"install-power":          "install-checkpoint done",
	"recovery-power":         "recovery-open",
	"checkpoint-full-cycle":  "checkpoint-sync",
	"checkpoint-fsync-eio":   "checkpoint-sync",
	"checkpoint-stall-cycle": "checkpoint-sync",
}

func isStorageFault(s string) bool      { _, ok := storageFaults[s]; return ok }
func (c *cluster) gateDir(i int) string { return filepath.Join(c.dir, fmt.Sprintf("gate-node%d", i+1)) }

func (r *runner) armGate(i int, stage string) error {
	dir := r.c.gateDir(i)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for _, name := range []string{"hit", "release", "arm"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.WriteFile(filepath.Join(dir, "arm"), []byte(stage), 0600)
}

func (r *runner) releaseGate(i int, result string) error {
	return os.WriteFile(filepath.Join(r.c.gateDir(i), "release"), []byte(result), 0600)
}

// storageFault repeats the same fault on the same node, not a new random
// target. Every round waits for an exact storage boundary and then checks
// that an acknowledged, unique canary is still readable through every node.
// The complete history, including the canaries and compactions, also goes
// through all the ordinary checkers at the end.
func (r *runner) storageFault(ctx context.Context, s step, lead, follower int, rec *record) error {
	target := follower
	if s.Pick%3 == 0 && s.Fault != "install-power" {
		target = lead
	}
	rec.Env = &envRecord{Node: fmt.Sprintf("node%d", target+1)}
	n, d := r.c.nodes[target], r.e.disks[target]
	stage := storageFaults[s.Fault]
	for cycle := range 3 {
		key := fmt.Sprintf("%sstorage/%s/%d", prefix, s.Fault, cycle)
		value := fmt.Sprintf("acked-before-%s-%d-%d", s.Fault, cycle, s.Pick)
		op := r.t.r.call(r.t.cs[others(target)[0]], 60, history.Request{Put: &history.PutRequest{Key: key, Value: value}}, livenessBound)
		if op.Resp == nil {
			return fmt.Errorf("canary before cycle %d: %v", cycle, op.Err)
		}
		if err := r.c.ready(ctx, 30*time.Second); err != nil {
			return err
		}
		var err error
		lead, err = r.c.leader(ctx, 15*time.Second)
		if err != nil {
			return err
		}
		if s.Fault == "install-power" && target == lead {
			return fmt.Errorf("install target became leader before cycle %d", cycle)
		}
		logBefore := logSize(n)
		if s.Fault == "recovery-power" {
			if _, _, err := n.signal(syscall.SIGKILL); err != nil {
				return err
			}
		}
		if s.Fault == "install-power" {
			if _, _, err := n.signal(syscall.SIGSTOP); err != nil {
				return err
			}
			for k := range 160 {
				q := history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sinstall%d", prefix, k%16), Value: fmt.Sprintf("%d-%d-%s", cycle, k, strings.Repeat("b", 16384))}}
				r.t.r.call(r.t.cs[lead], 61, q, r.t.cfg.requestTimeout)
			}
		}
		if err := r.armGate(target, stage); err != nil {
			return err
		}
		switch s.Fault {
		case "recovery-power":
			if err := r.c.start(n); err != nil {
				return err
			}
		case "install-power":
			if _, _, err := n.signal(syscall.SIGCONT); err != nil {
				return err
			}
		}
		// Defragment starts cleaning even when no disk budget is configured.
		var defrag <-chan error
		if s.Fault == "clean-power" {
			done := make(chan error, 1)
			defrag = done
			go func() {
				cctx, cancel := context.WithTimeout(ctx, 70*time.Second)
				defer cancel()
				_, err := r.t.cs[target].Defragment(cctx, r.t.cs[target].Endpoints()[0])
				done <- err
			}()
		}
		hit := filepath.Join(r.c.gateDir(target), "hit")
		if !waitUntil(ctx, 60*time.Second, func() bool { b, err := os.ReadFile(hit); return err == nil && string(b) == stage }) {
			releaseErr := r.releaseGate(target, "ok")
			if defrag != nil {
				<-defrag
			}
			return errors.Join(fmt.Errorf("cycle %d did not reach storage gate %s within 60s", cycle, stage), releaseErr)
		}
		rec.Installed = true
		rec.Env.Parts = append(rec.Env.Parts, fmt.Sprintf("cycle %d hit %s at %v", cycle, stage, r.since()))
		r.logf("%s node%d cycle %d hit %s", s.Fault, target+1, cycle, stage)
		switch s.Fault {
		case "checkpoint-full-cycle":
			n.mu.Lock()
			n.expecting = true
			n.mu.Unlock()
			filled, err := d.Fill()
			if err != nil {
				return err
			}
			rec.Env.Filled += filled
			// The next checkpoint allocation fails on the real full filesystem.
			if err := r.releaseGate(target, "ok"); err != nil {
				return err
			}
			if err := pause(ctx, 3*time.Second); err != nil {
				return err
			}
			if err := d.Unfill(); err != nil {
				return err
			}
		case "checkpoint-fsync-eio":
			n.mu.Lock()
			n.expecting = true
			n.mu.Unlock()
			// Fail this exact checkpoint sync at the file layer, before it can
			// report success. Device errors alone may instead hit ext4 metadata.
			if err := r.releaseGate(target, "eio"); err != nil {
				return err
			}
			if err := pause(ctx, 3*time.Second); err != nil {
				return err
			}
		case "checkpoint-stall-cycle":
			if err := d.Stall(); err != nil {
				return err
			}
			if err := r.releaseGate(target, "ok"); err != nil {
				return errors.Join(err, d.Healthy())
			}
			holdErr := r.hold(ctx, 5*time.Second, others(target)[0])
			if err := errors.Join(holdErr, d.Healthy()); err != nil {
				return err
			}
		}
		if s.Fault != "checkpoint-stall-cycle" {
			n.mu.Lock()
			exited := n.exitedNow
			n.mu.Unlock()
			if !exited {
				if _, _, err := n.signal(syscall.SIGKILL); err != nil {
					return err
				}
			}
			if strings.Contains(s.Fault, "power") {
				report, err := d.PowerLoss()
				rec.Env.LazyFS += report
				if err != nil {
					return err
				}
			}
			if err := r.c.start(n); err != nil {
				return err
			}
		}
		if defrag != nil {
			<-defrag
		} // Its connection was deliberately killed.
		if err := r.c.ready(ctx, 45*time.Second); err != nil {
			return fmt.Errorf("cycle %d catch-up: %w", cycle, err)
		}
		text := logSince(n, logBefore)
		switch s.Fault {
		case "checkpoint-full-cycle":
			if !strings.Contains(text, "no space left on device") {
				return fmt.Errorf("cycle %d: full disk did not produce ENOSPC", cycle)
			}
		case "checkpoint-fsync-eio":
			if !strings.Contains(text, "input/output error") {
				return fmt.Errorf("cycle %d: checkpoint EIO not observed", cycle)
			}
		case "checkpoint-stall-cycle":
			if !strings.Contains(text, "slow write to the log") {
				return fmt.Errorf("cycle %d: stall did not hold a log write", cycle)
			}
		}
		for i, cl := range r.t.cs {
			got := r.t.r.call(cl, 60, history.Request{Range: &history.RangeRequest{Key: key}}, livenessBound)
			if got.Resp == nil || len(got.Resp.Range.KVs) != 1 || got.Resp.Range.KVs[0].Value != value {
				return fmt.Errorf("cycle %d node%d lost acknowledged canary: %+v", cycle, i+1, got)
			}
		}
		rec.Env.Parts = append(rec.Env.Parts, fmt.Sprintf("cycle %d caught up; acknowledged canary checked through all nodes", cycle))
		rec.Effect = true
	}
	rec.Healed, rec.HealedAt = true, r.since()
	return nil
}
