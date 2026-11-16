//go:build simio || sim

package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// The first runs of wl nodes (Config.wl). wl's seam needs the build tag
// sim, which gosim's translation sets, or simio natively: scripts/sim.sh
// builds the native test binary with it.

// A wl cluster runs a seed with no faults: its clients' calls, through
// gRPC, agree with the committed log, which every node applied alike.
func TestWLRunsACalmSeed(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := CalmConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		cfg.Duration, cfg.Heal = 5*Second, 5*Second
		res := run(t, cfg)
		t.Logf("calls: %d ok, %d failed, %d ambiguous, %d by clientv3; log %d writes to revision %d; %s",
			res.Stats.OK, res.Stats.Failed, res.Stats.Ambiguous, res.Stats.V3OK, len(res.Log), res.FinalRev, watched(res))
		if res.Stats.OK < 100 {
			t.Fatalf("only %d calls succeeded", res.Stats.OK)
		}
		return res.Stats
	}, nil)
}

// A wl cluster runs a seed with crashes, kills and power losses, link
// changes and delays, slow syncs and pauses: every node recovers what it
// reported durable, and the checks hold.
func TestWLRunsASeedWithCrashes(t *testing.T) {
	simrun.Each(t, []uint64{2}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		cfg.Faults = Faults{
			CrashEvery: 3 * Second, MaxDown: 2 * Second,
			LinkEvery: 2 * Second, MaxDelay: 5 * Millisecond,
			SlowDisk: 2, PauseEvery: 6 * Second,
		}
		res := run(t, cfg)
		t.Logf("calls: %d ok, %d failed, %d ambiguous; crashes %d (power losses %d, kills %d); log %d writes to revision %d; %s",
			res.Stats.OK, res.Stats.Failed, res.Stats.Ambiguous, res.Stats.Crashes, res.Stats.PowerLosses, res.Stats.Kills, len(res.Log), res.FinalRev, watched(res))
		if res.Stats.Crashes == 0 {
			t.Fatal("no crash")
		}
		return res.Stats
	}, nil)
}

// A wl cluster runs DefaultConfig's faults but those of main's engine
// alone (recovery's and a snapshot fetch's steps, rot of a value in a
// segment, cold restarts from checkpoints) and damage to a disk at rest,
// which wl does not run yet: disk errors, short writes, stalls, file
// systems that shut down or go read-only, full disks, checkpoints that
// cut the log, link stalls, half-open connections, netem, resets, gray
// nodes and clients, slow watchers, clock drift and held-down nodes.
func TestWLRunsTheFaults(t *testing.T) {
	simrun.Each(t, []uint64{8}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		f := &cfg.Faults
		f.Checkpoint, f.RecoveryCrash, f.StepCrash, f.FlipSnapshot, f.FetchCrash, f.SlowFetch, f.Rot, f.Corrupt = false, false, false, false, false, false, false, false
		res := run(t, cfg)
		st := res.Stats
		t.Logf("calls: %d ok, %d failed, %d ambiguous; crashes %d, disk faults %d, file systems down %d, disks full %d, half-open %d, link stalls %d, gray streams %d, slow watchers %d; %s",
			st.OK, st.Failed, st.Ambiguous, st.Crashes, st.DiskFaults, st.FSDowns, st.DiskFulls, st.HalfOpens, st.LinkStalls, st.GrayStreams, st.SlowWatchers, watched(res))
		if st.DiskFaults == 0 {
			t.Fatal("no disk fault fired")
		}
		return st
	}, nil)
}

// The log's stage race (internal/wl/wal.go, writeLoop): the helper writes
// the stage's last block whole while the loop appends past the durable
// end in it. A crash right after such a write and before its sync keeps
// the block torn at sectors, half new and half old. The restarted node
// recovers every entry it reported durable and none past the last it
// handed its log, and what it then applies matches the other nodes'.
func TestWLRecoversFromATornWriteOfTheStage(t *testing.T) {
	simrun.Each(t, []uint64{3, 4, 5}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults, cfg.stageCrash = true, true, true
		cfg.Faults = Faults{MaxDelay: 5 * Millisecond, SlowDisk: 2}
		res := run(t, cfg)
		t.Logf("calls: %d ok; torn crashes %d, recovered %d; log %d writes to revision %d",
			res.Stats.OK, res.Stats.StageCrashes, res.Stats.StageRecoveries, len(res.Log), res.FinalRev)
		for _, l := range res.Trace {
			if strings.Contains(l, "torn") || strings.Contains(l, "unsynced bytes") {
				t.Log(l)
			}
		}
		if res.Stats.StageCrashes != 1 || res.Stats.StageRecoveries != 1 {
			t.Fatalf("%d torn crashes, %d recoveries after one; want one each", res.Stats.StageCrashes, res.Stats.StageRecoveries)
		}
		return res.Stats
	}, nil)
}

// watched says what the run's watches got, which checkWatches checked
// against the replayed log.
func watched(res *Result) string {
	events := 0
	for _, w := range res.Watches {
		for _, r := range w.Responses {
			events += len(r.Resp.Events)
		}
	}
	if len(res.Watches) == 0 || events == 0 {
		return "no watch got an event"
	}
	return fmt.Sprintf("%d logical watches got %d events, %d compacted", len(res.Watches), events, res.Stats.WatchCompacted)
}
