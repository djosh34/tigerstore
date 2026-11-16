//go:build simio || sim

package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wl"
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
// alone (a snapshot fetch's steps, rot of a value in a segment), which
// wl does not run yet: cold restarts from copies of the disks with one
// erased, during the run and at its end, crashes inside
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
// revision still to come as compacted; on seed 12024 the clients'
// connection to a node, whose dials a client link refused before healing,
// was still in gRPC's backoff 4 s after it; seed 12731 spun for good in
// gRPC's transparent retries on that connection (#440); on seed 19458 a
// start crashed before it recovered, and the run then required the
// entries that an append the process before had partly written replaced;
// on seed 19159 a leader elected by its peers' votes alone lost its term,
// which was not durable yet, and led the same term again with another log.
func TestWLRunsTheFaults(t *testing.T) {
	simrun.Each(t, []uint64{2, 7, 8, 1002, 10155, 10161, 12024, 12731, 19159, 19458}, func(t *testing.T, seed uint64) Stats {
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
	t.Logf("calls: %d ok, %d failed, %d ambiguous; cold restarts %d; crashes %d (in recovery %d, at a step %d), disk faults %d, damage %v (fenced %d, misreads refused %d), file systems down %d, disks full %d, half-open %d, link stalls %d, gray streams %d, slow watchers %d; %s",
		st.OK, st.Failed, st.Ambiguous, st.ColdRestarts, st.Crashes, st.RecoveryCrashes, st.StepCrashes, st.DiskFaults, st.Damage, st.Fenced, st.MisreadRefusals, st.FSDowns, st.DiskFulls, st.HalfOpens, st.LinkStalls, st.GrayStreams, st.SlowWatchers, watched(res))
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
		cfg.Duration = 24 * Second
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

// A wl cluster adds a fourth member as a learner, moves it to another
// peer URL, promotes it and removes it, one change after the other,
// under TestWLRunsTheFaults' faults spaced as membershipConfig spaces
// them; each answer matches the member list, and every node ends with
// the final members and the removal.
func TestWLMembershipFaults(t *testing.T) {
	simrun.Seeds(t, 1, 4, func(t *testing.T, seed uint64) Stats {
		res := run(t, wlMembershipConfig(seed))
		for i, count := range res.Stats.MembershipChanges {
			if count != 1 {
				t.Fatalf("stage %d completed %d times", i, count)
			}
		}
		checkers(t, res)
		t.Logf("calls: %d ok, %d failed, %d ambiguous; membership attempts %d, during faults %v; cold restarts %d, crashes %d, disk faults %d, link changes %d; %s",
			res.Stats.OK, res.Stats.Failed, res.Stats.Ambiguous, res.Stats.MembershipAttempts, res.Stats.MembershipFaults, res.Stats.ColdRestarts, res.Stats.Crashes, res.Stats.DiskFaults, res.Stats.LinkChanges, watched(res))
		if res.Stats.ColdRestarts == 0 {
			t.Fatal("no cold restart ran")
		}
		return res.Stats
	}, nil)
}

// A wl node of the simulator makes at most 128 MiB at its start, Go's
// memory under gosim, made again at each start: a run of three nodes, its
// restarts and its cold restarts, four runs at once on a campaign's
// runner, fit it. At 64 MiB rings and the real build's tables, a calm seed
// held 3.8 GB live, and the campaign's runners were killed.
func TestAWLNodeOfTheSimulatorIsSmall(t *testing.T) {
	const most = 128 << 20
	if got := wl.SimFootprint(0); got > most {
		t.Fatalf("a wl node of the simulator makes %d MiB at its start, past %d MiB", got>>20, most>>20)
	}
}

// TestWLStalledLeaderIsReplaced is TestStalledLeaderIsReplaced on wl
// nodes: a leader whose disk stalls goes silent within two election
// timeouts and two ticks of its oldest held append, or steps down first,
// and every call to a healthy node that starts 10 election timeouts after
// the stall succeeds (silent.go).
func TestWLStalledLeaderIsReplaced(t *testing.T) {
	election := 10 * TickTime
	simrun.Each(t, []uint64{1, 2, 3}, func(t *testing.T, seed uint64) Stats {
		cfg := wlStalledLeaderConfig(seed)
		res := run(t, cfg)
		at, what := res.SteppedDownAt, "stepped down"
		if len(res.Silences) > 0 && (at == 0 || res.Silences[0] < at) {
			at, what = res.Silences[0], "went silent"
		}
		if at == 0 {
			t.Fatalf("seed %d: the stalled leader never went silent nor stepped down", seed)
		}
		if res.HeldAppendAt == 0 {
			t.Fatalf("seed %d: the stall held no append of the leader", seed)
		}
		limit := res.HeldAppendAt + 2*election + 2*TickTime
		if at < cfg.StallLeaderAt || at > limit {
			t.Fatalf("seed %d: the stalled leader %s at %d ms, want between the stall at %d ms and %d ms, two election timeouts and two ticks after its oldest held append at %d ms",
				seed, what, at/Millisecond, cfg.StallLeaderAt/Millisecond, limit/Millisecond, res.HeldAppendAt/Millisecond)
		}
		checkers(t, res)
		t.Logf("seed %d: the stalled leader %s at %d ms, deadline %d ms after its oldest held append at %d ms", seed, what, at/Millisecond, limit/Millisecond, res.HeldAppendAt/Millisecond)
		return res.Stats
	}, nil)
}

// TestWLRefusesADamagedSnapshot runs wlDamagedSnapshotConfig, B22's: a
// follower held down fetches a snapshot whose stream is damaged, and
// refuses it by its CRC.
func TestWLRefusesADamagedSnapshot(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		res := run(t, wlDamagedSnapshotConfig(seed))
		if res.Stats.SnapshotFlips == 0 {
			t.Fatalf("seed %d: no snapshot stream was damaged (%d sent)", seed, res.Stats.Snapshots)
		}
		checkers(t, res)
		return res.Stats
	}, nil)
}
