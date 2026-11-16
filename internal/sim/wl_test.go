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
// alone (a snapshot fetch's steps, rot of a value in a segment, cold
// restarts from checkpoints), which wl does not run yet: crashes inside
// recovery and at checkpoint steps, damage to a disk at rest, disk
// errors, short writes, stalls, file
// systems that shut down or go read-only, full disks, checkpoints that
// cut the log, link stalls, half-open connections, netem, resets, gray
// nodes and clients, slow watchers, clock drift and held-down nodes.
// Seed 7 lost an entry its start had read from the page cache (#701);
// seed 1002 left a segment past the log's end after a crash (#702); on
// seed 2 the run dropped an ambiguous Txn that took effect from the
// history, and the durability check found its entry without a client
// (#703); on seed 10161 a node started again before the cluster committed
// its first ConfChanges, knew no member, and never answered its leader; on
// seed 10155 a node that installed a snapshot canceled a watch from a
// revision still to come as compacted.
func TestWLRunsTheFaults(t *testing.T) {
	simrun.Each(t, []uint64{2, 7, 8, 1002, 10155, 10161}, func(t *testing.T, seed uint64) Stats {
		st := runWLFaults(t, seed)
		if st.DiskFaults == 0 {
			t.Fatal("no disk fault fired")
		}
		return st
	}, nil)
}

// TestWLFaultSeeds is TestWLRunsTheFaults over a set of seeds, for a
// campaign (sim-campaign.yml); alone it runs one.
func TestWLFaultSeeds(t *testing.T) {
	simrun.Seeds(t, 1000, 1, runWLFaults, nil)
}

// runWLFaults runs seed on wl nodes with the faults TestWLRunsTheFaults
// names.
func runWLFaults(t *testing.T, seed uint64) Stats {
	res := run(t, wlFaultsConfig(seed))
	checkers(t, res)
	st := res.Stats
	t.Logf("calls: %d ok, %d failed, %d ambiguous; crashes %d (in recovery %d, at a step %d), disk faults %d, damage %v (fenced %d, misreads refused %d), file systems down %d, disks full %d, half-open %d, link stalls %d, gray streams %d, slow watchers %d; %s",
		st.OK, st.Failed, st.Ambiguous, st.Crashes, st.RecoveryCrashes, st.StepCrashes, st.DiskFaults, st.Damage, st.Fenced, st.MisreadRefusals, st.FSDowns, st.DiskFulls, st.HalfOpens, st.LinkStalls, st.GrayStreams, st.SlowWatchers, watched(res))
	return st
}

// A wl node refuses a disk the run damaged where its start reads it,
// and is fenced; a start that misread its log refuses it and starts again
// (Sim.damageWL). One test a kind of damage, on one seed each.
func TestWLRefusesAFlippedCheckpoint(t *testing.T) {
	runWLDamage(t, flippedRoot, true, func(st Stats) int { return min(st.Damage[flippedRoot], st.Fenced) })
}

func TestWLRefusesAMisdirectedWrite(t *testing.T) {
	runWLDamage(t, misdirected, false, func(st Stats) int { return min(st.Damage[misdirected], st.Fenced) })
}

func TestWLRefusesALostWrite(t *testing.T) {
	runWLDamage(t, lostWrite, false, func(st Stats) int { return min(st.Damage[lostWrite], st.Fenced) })
}

func TestWLRefusesABadSector(t *testing.T) {
	runWLDamage(t, badSector, false, func(st Stats) int { return min(st.Damage[badSector], st.Fenced, st.Disk.BadReads) })
}

func TestWLRefusesAMisreadLog(t *testing.T) {
	runWLDamage(t, misread, false, func(st Stats) int { return min(st.Damage[misread], st.MisreadRefusals) })
}

// runWLDamage runs seed 1 on wl nodes with crashes and damage of kind after
// them, and fails unless count shows it took effect. With ckpt, nodes
// take checkpoints, which cut the log behind them; without, the log grows
// over the segments that damage to it needs.
func runWLDamage(t *testing.T, kind damageKind, ckpt bool, count func(Stats) int) {
	t.Helper()
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		budget := cfg.Faults.DiskBudget
		cfg.Faults = Faults{CrashEvery: Second, MaxDown: Second, Corrupt: true}
		if ckpt {
			// A checkpoint every DiskBudget of log (startWL).
			cfg.Faults.DiskBudget = budget
		}
		cfg.damageKinds = []damageKind{kind}
		cfg.Duration = 16 * Second
		res := run(t, cfg)
		st := res.Stats
		t.Logf("calls: %d ok; crashes %d; damage %d, fenced %d, misread refusals %d, bad reads %d; log %d writes to revision %d",
			st.OK, st.Crashes, st.Damage[kind], st.Fenced, st.MisreadRefusals, st.Disk.BadReads, len(res.Log), res.FinalRev)
		if count(st) == 0 {
			t.Fatalf("no %v took effect", kind)
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
