package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// Runs of wl nodes (Config.wl). They need wl's seam for the simulator:
// the build tag sim, which gosim's translation sets, or simio natively,
// with which scripts/sim.sh and the race detector's job build the native
// test binary.

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
		checkers(t, res)
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
		checkers(t, res)
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
// which was not durable yet, and led the same term again with another log;
// on seeds 28789, 29384 and 29489 nodes ran out of their disks' room
// (TestWLFormsAndRunsTheFaults); on seed 33970 a key deleted at the
// compacted revision was made again while a checkpoint was being written,
// and trimming dropped the deletion, the key's newest state at the
// checkpoint's revision, so the checkpoint held another state than its
// store (#712).
//
// Its seeds form their cluster under the faults, as the campaign that
// found them did; TestWLFormsAndRunsTheFaults has seeds that form it
// first.
func TestWLRunsTheFaults(t *testing.T) {
	simrun.Each(t, []uint64{2, 7, 8, 1002, 10155, 10161, 12024, 12731, 19159, 19458, 28789, 29384, 29489, 33970}, func(t *testing.T, seed uint64) Stats {
		st := runWLFaults(t, seed, true).Stats
		if st.DiskFaults == 0 {
			t.Fatal("no disk fault fired")
		}
		return st
	}, nil)
}

// TestWLFormsAndRunsTheFaults is TestWLRunsTheFaults on seeds that form
// their cluster first, with no faults and no clients, and start the run
// from its disks (formCluster), as 7 seeds in 8 of TestSeeds do. On seed
// 24040 the leader dropped a snapshot to a follower whose peer connection
// was down without reporting it to Raft, which then waited for good, and
// the follower ended 1,300 entries behind. On seeds 29122 and 29253, as on
// 28789, 29384 and 29489 of TestWLRunsTheFaults, nodes that allocated each
// log segment's space ahead whatever room their small disks had left
// stopped writing checkpoints with ENOSPC, and were down at the end. On
// seed 376 a leader resumed from a pause after the others had elected
// another, and decided to expire a lease the new leader had renewed, in
// its first tick, before it heard of the new leader (#710).
func TestWLFormsAndRunsTheFaults(t *testing.T) {
	simrun.Each(t, []uint64{1, 3, 376, 24040, 29122, 29253}, func(t *testing.T, seed uint64) Stats {
		st := runWLFaults(t, seed, false).Stats
		if !st.Formed {
			t.Fatal("the cluster did not form first")
		}
		return st
	}, nil)
}

// runWLFaults runs seed on wl nodes with the faults TestWLRunsTheFaults
// names; underFaults forms the cluster under them, from the run's first
// moment, and otherwise formsFirst draws how. Damage that finds no place
// flips a record of the log (Config.flipRecords), as TestSeeds' rule over
// a set needs.
func runWLFaults(t *testing.T, seed uint64, underFaults bool) *Result {
	cfg := wlFaultsConfig(seed)
	cfg.FormUnderFaults, cfg.flipRecords = underFaults, true
	res := run(t, cfg)
	checkers(t, res)
	st := res.Stats
	t.Logf("calls: %d ok, %d failed, %d ambiguous; cold restarts %d; crashes %d (in recovery %d, at a step %d), disk faults %d, damage %v (fenced %d, misreads refused %d), rots %d, file systems down %d, disks full %d, half-open %d, link stalls %d, gray streams %d, slow watchers %d; %s",
		st.OK, st.Failed, st.Ambiguous, st.ColdRestarts, st.Crashes, st.RecoveryCrashes, st.StepCrashes, st.DiskFaults, st.Damage, st.Fenced, st.MisreadRefusals, st.Rots, st.FSDowns, st.DiskFulls, st.HalfOpens, st.LinkStalls, st.GrayStreams, st.SlowWatchers, watched(res))
	return res
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
		checkers(t, res)
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
		checkers(t, res)
		return res.Stats
	}, nil)
}

// A wl segment's space is allocated ahead (wal.go's makeSegment), and on
// ext4 a crash may keep a direct write's growth of the file without the
// conversion of its blocks from unwritten, so the write reads as zeros.
// The crash of TestWLRecoversFromATornWriteOfTheStage, on its seeds,
// keeps that image (simfs.Unwritten): the restarted node stops at the
// zeros as at a torn tail, recovers every entry it reported durable and
// none past the last it handed its log, and applies what the others do.
func TestWLRecoversFromAZeroedTailOfItsLog(t *testing.T) {
	simrun.Each(t, []uint64{3, 4, 5}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults, cfg.stageCrash, cfg.stageUnwritten = true, true, true, true
		cfg.Faults = Faults{MaxDelay: 5 * Millisecond, SlowDisk: 2}
		res := run(t, cfg)
		grew := 0
		for _, l := range res.Trace {
			var name string
			var kept, synced int
			if _, err := fmt.Sscanf(l[strings.Index(l, "crash: ")+len("crash: "):], "%s (file %d) keeps %d bytes: %d synced", &name, new(int), &kept, &synced); strings.Contains(l, "crash: ") && err == nil && isSegment(name) && strings.HasSuffix(l, "unwritten") && kept > synced {
				t.Log(l)
				grew++
			}
		}
		t.Logf("calls: %d ok; crashes keeping zeros %d, recovered %d; log %d writes to revision %d", res.Stats.OK, res.Stats.StageCrashes, res.Stats.StageRecoveries, len(res.Log), res.FinalRev)
		if res.Stats.StageCrashes != 1 || res.Stats.StageRecoveries != 1 || grew == 0 {
			t.Fatalf("%d crashes, %d recoveries after one, %d segments grown with zeros; want one, one and some", res.Stats.StageCrashes, res.Stats.StageRecoveries, grew)
		}
		checkers(t, res)
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

// TestWLCrashKeepsALaterCheckpointWrite crashes nodes partway through a
// checkpoint until a crash keeps a later write of it and loses an earlier
// one (wlCheckpointCrashConfig). wl's log has one write in flight, synced
// as it is written, so a crash during the faults, when a node's log holds
// a few hundred bytes and no checkpoint, never does that; TestSeeds runs
// this with each set of 64 seeds or more, for its rule that a set has
// such a crash.
func TestWLCrashKeepsALaterCheckpointWrite(t *testing.T) {
	runCheckpointCrash(t)
}

// runCheckpointCrash runs wlCheckpointCrashConfig on its seed, which fails
// unless a crash kept a later write and lost an earlier one, and returns
// how many did: 0 under gosim, where the driver counts them.
func runCheckpointCrash(t *testing.T) int {
	reordered := 0
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		res := run(t, wlCheckpointCrashConfig(seed))
		checkers(t, res)
		st := res.Stats
		if st.Disk.Reordered == 0 {
			t.Fatalf("seed %d: %d crashes partway through %d checkpoints; none kept a later write and lost an earlier one", seed, st.CkptCrashes, st.Checkpoints)
		}
		t.Logf("seed %d: %d crashes partway through %d checkpoints, %d kept a later write and lost an earlier one", seed, st.CkptCrashes, st.Checkpoints, st.Disk.Reordered)
		return st
	}, func(_ uint64, st Stats) bool {
		reordered += st.Disk.Reordered
		return false
	})
	return reordered
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

// A wl node whose disk rots a value under it never serves the value: its
// start refuses the disk, which fences it, while its store names the
// value (rotWL, #708). Rot alone, on one seed; the rotted node is
// restarted 10 ms after the rot. A read of the value back stops the node
// (internal/wl/rot_test.go); TestWLRunsTheFaults runs rot among the
// other faults.
func TestWLRefusesARottedValue(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		cfg.Faults = Faults{Rot: true, DiskBudget: cfg.Faults.DiskBudget}
		cfg.restartRotted = true
		res := run(t, cfg)
		st := res.Stats
		t.Logf("calls: %d ok; rots %d, stops on one %d, fenced %d; log %d writes to revision %d",
			st.OK, st.Rots, st.RotStops, st.Fenced, len(res.Log), res.FinalRev)
		if st.Rots == 0 || st.Fenced == 0 {
			t.Fatalf("rots %d, fenced %d: want each", st.Rots, st.Fenced)
		}
		checkers(t, res)
		return st
	}, nil)
}

// The source of a wl node's snapshot stream crashes once the follower
// read a prefix of it (FetchCrash, wlHooks.SnapshotCut): the follower
// drops the prefix, and the source starts again from its disk, which the
// stream's file was written on. A follower held down needs the snapshot.
func TestWLCrashesTheSourceOfASnapshot(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.wl, cfg.FormUnderFaults = true, true
		cfg.Faults = Faults{HoldDown: true, FetchCrash: true, DiskBudget: cfg.Faults.DiskBudget}
		res := run(t, cfg)
		st := res.Stats
		t.Logf("calls: %d ok; snapshots %d, sources crashed after a prefix %d; log %d writes to revision %d",
			st.OK, st.Snapshots, st.FetchCrashes, len(res.Log), res.FinalRev)
		if st.FetchCrashes == 0 {
			t.Fatal("no source crashed after a prefix the follower read")
		}
		checkers(t, res)
		return st
	}, nil)
}
