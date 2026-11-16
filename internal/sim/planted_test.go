//go:build planted

package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/check"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// plantedCase is how a planted bug must be caught: within seeds runs of
// cfg, by a failing check on a seed that passes without the bug
// (failsUnplanted), so that the failure is the bug's. want names the
// checks expected to catch it, the assertions the semantics pre-work on
// #47 names; another check counts too and is logged, as a schedule that
// moves can reach another check first (#537). A failure that decides
// nothing about the run, a crash, a stop for lack of real time or a
// witness search that ran out (decides), is no catch and fails the test.
// extra adds an assertion of the case's own. With witness, the witness checker in
// both modes must fail the same seed's history on its own too: Porcupine
// cannot check the full runs (#112). Each cfg also runs in the plain
// tests (TestSeeds, TestStalledLeaderIsReplaced), where it must pass.
type plantedCase struct {
	seeds   int
	cfg     func(seed uint64) Config
	want    string
	extra   func(*Result) error
	witness bool
}

const (
	// A read older than a write that returned before it started.
	staleRead = `before it started|linearizab`
	// An acknowledged append, or a durable term and vote, that recovery or
	// a cold restart lost.
	lostAppend = `reported \d+ durable|erasing node \d+|` + lostVote
	// A node started again with an older term or vote than it held
	// durable.
	lostVote = `but held term \d+ and vote \d+ durable`
	// A node whose leader commits past the end of its log, which lost
	// entries it acknowledged (#493).
	lostEntries = `lost entries it acknowledged`
	// A watch that broke the watch rules.
	badWatch = `watch \d+`
	// A read or the replayed log that disagrees with the model; not a
	// liveness failure, which names the call but not its node.
	badState = `call at \d+ ms on node \d+:|replaying the committed log`
	// A node applied a value that no client wrote.
	// On wl, a node's store differs from another's at a checkpoint's
	// index (sameState), or a checkpoint from its store (CheckpointDone).
	inventedState = `applied different entries|applied the same log to different states|differs from the final log|with another state than node \d+'s|` + badState
	// Recovery refused a log it wrote itself.
	refused = `cannot recover its log`
	// A node read a value that is not the one it wrote, or that its log
	// no longer holds, and stopped.
	badValue = `does not match its checksum|past the end of entry|wal: read \d+ of \d+ bytes`
	// A node that never catches up, so the nodes end at different
	// indexes.
	stuck = `node \d+ applied \d+ entries, node \d+ applied \d+`
)

// openBugs are the failures of open bugs #357 and #361, which the default
// seeds hit in about one in twenty: a lost entry, term or vote, or a
// checkpoint that a restart removed on a root it read from the page
// cache. Such a seed catches nothing, unless the planted bug is caught by
// the same failure and the seed passes without it (failsUnplanted). Their
// fixes remove it.
var openBugs = regexp.MustCompile(`reported \d+ durable|held term \d+ and vote \d+ durable|load the checkpoint at \d+: file does not exist`)

// TestPlantedInventory requires a case for every planted bug.
func TestPlantedInventory(t *testing.T) {
	for name := range planted.Bugs {
		if _, ok := plantedCases[name]; !ok {
			t.Errorf("planted bug %s (%s) has no case", name, planted.Bugs[name])
		}
	}
	for name := range plantedCases {
		if _, ok := planted.Bugs[name]; !ok {
			t.Errorf("case %s plants no known bug", name)
		}
	}
	ci, err := os.ReadFile("../../.github/workflows/planted.yml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*BUGS: (.*)$`).FindSubmatch(ci)
	if m == nil {
		t.Fatal("planted.yml has no BUGS list")
	}
	inCI := strings.Fields(string(m[1]))
	for name := range planted.Bugs {
		if !slices.Contains(inCI, name) {
			t.Errorf("planted bug %s is not in the CI list", name)
		}
	}
	if len(inCI) != len(planted.Bugs) {
		t.Errorf("the CI list has %d bugs, the inventory %d", len(inCI), len(planted.Bugs))
	}
}

// witnessFinding is how the witness checker reports a stale read: an
// answer no state of its revision gives, or one real time does not allow.
// A timeout or a search that ran out of candidates is no catch.
var witnessFinding = regexp.MustCompile(`answers it|real time allows|returned before it was called`)

// witnessCatches reports whether the witness checker finds the stale read
// in a run's history in both modes.
func witnessCatches(t *testing.T, res *Result) bool {
	if res == nil {
		return false
	}
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []check.WitnessMode{check.FromLog, check.FromRevisions} {
		err := check.Witnessed(h, mode, time.Minute, witnessSteps, wallStop)
		if err == nil || !witnessFinding.MatchString(err.Error()) {
			t.Logf("the witness checker (%s) did not find the stale read on this seed: %v", mode, err)
			return false
		}
		t.Logf("the witness checker (%s): %v", mode, err)
	}
	return true
}

// plantedRun is what a seed of TestPlanted found: the failure that
// catches the bug, if any. It counts only if the seed passes without the
// planted bug (SIM_CONTROL).
type plantedRun struct {
	Finding string
}

// plantedCases are the planted bugs, each with the runs that catch it.
var plantedCases = map[string]plantedCase{
	"B01a": {seeds: 64, cfg: DefaultConfig, want: staleRead, witness: true},
	"B01b": {seeds: 64, cfg: DefaultConfig, want: staleRead, witness: true},
	"B15":  {seeds: 64, cfg: DefaultConfig, want: `no client sent it`},
	"B02":  {seeds: 128, cfg: wlFaultsConfig, want: lostAppend + `|applied different entries`},
	"B03":  {seeds: 128, cfg: wlFaultsConfig, want: lostAppend + `|applied different entries`},
	"B04a": {seeds: 64, cfg: wlDamageConfig, want: `without noticing|` + refused},
	// B04b cuts the record before a torn tail, which can be a vote.
	"B04b": {seeds: 256, cfg: wlFaultsConfig, want: lostAppend + `|applied different entries|applied the same log to different states`},
	// B04c: wl reads values back from its log, so the flipped byte is
	// what a node serves or applies.
	"B04c": {seeds: 256, cfg: wlFaultsConfig, want: inventedState + `|without noticing|` + refused + `|` + badValue},
	"B05":  {seeds: 128, cfg: wlFaultsConfig, want: lostAppend + `|` + refused},
	"B06a": {seeds: 32, cfg: wlFaultsConfig, want: badWatch},
	"B06b": {seeds: 32, cfg: wlFaultsConfig, want: badWatch},
	"B06c": {seeds: 32, cfg: wlFaultsConfig, want: badWatch},
	"B06d": {seeds: 32, cfg: wlFaultsConfig, want: `watch \d+ got an event at revision \d+ after one at`},
	"B07":  {seeds: 64, cfg: wlFaultsConfig, want: badWatch},
	// B08 frees the key of the tombstone at the compacted revision, so a
	// watch from there misses its delete.
	"B08":  {seeds: 128, cfg: wlFaultsConfig, want: badWatch},
	"B09a": {seeds: 16, cfg: wlFaultsConfig, want: badState},
	"B09b": {seeds: 16, cfg: wlFaultsConfig, want: badState + `|applied the same log to different states`},
	// B10: a follower revokes the lease's keys itself, outside the log,
	// once it applied keepalives late: its store leaves the others' (the
	// comparison at a checkpoint's index among them), or the lease check
	// sees it expire the lease before the TTL of a renewal ran out.
	"B10": {seeds: 64, cfg: wlFaultsConfig, want: badState + `|applied the same log to different states|with another state than node \d+'s|expired at \d+ ms, but it was granted or renewed at|` + badWatch},
	"B11": {seeds: 64, cfg: wlFaultsConfig, want: lostAppend + `|` + refused},
	// wl writes at fixed offsets, so B12 is the log going on past a short
	// write as if it were whole: what it left out is never written.
	"B12": {seeds: 256, cfg: wlFaultsConfig, want: lostAppend + `|` + refused + `|` + badValue + `|` + lostEntries},
	// wl's writes are durable when they return, so B16 is a segment's
	// last write made through the page cache, with no sync, before the
	// log moves on to the next.
	"B16": {seeds: 128, cfg: wlFaultsConfig, want: lostAppend + `|` + refused},
	// Followers forward their writes to the stalled leader, which stays
	// leader, so the liveness check may find the timed-out calls first.
	"B17": {seeds: 64, cfg: wlStalledLeaderConfig, want: `the stalled leader|liveness: client \d+ call`, extra: func(r *Result) error {
		if len(r.Silences) == 0 {
			return errors.New("the stalled leader never went silent")
		}
		return nil
	}},
	"B18": {seeds: 16, cfg: wlFaultsConfig, want: `holds \d+ watchers`},
	// A wl start that misses the segment stops on it, as a cold restart
	// of seed 1 did.
	"B19": {seeds: 64, cfg: wlFaultsConfig, want: refused + `|not on disk|no such file or directory|` + stuck},
	"B20": {seeds: 64, cfg: wlFaultsConfig, want: `holds entries from \d+ in memory`},
	// wl's nodes take checkpoints at indexes of their own: the run checks
	// each against what the node applied (wlHooks.Checkpoint), and a
	// start from it misses the entries after the applied index.
	"B21": {seeds: 64, cfg: wlFaultsConfig, want: `started a checkpoint at index \d+, but applied to|` + inventedState + `|cold restart did not converge|` + stuck},
	// wl's checkpoint has a CRC of its own, which B22 skips at an
	// install too.
	"B22": {seeds: 256, cfg: wlDamagedSnapshotConfig, want: inventedState + `|malformed|corrupt checkpoint|` + badValue + `|` + refused},
	"B23": {seeds: 64, cfg: wlFaultsConfig, want: inventedState + `|cold restart did not converge|` + stuck},
	"B24": {seeds: 16, cfg: wlFaultsConfig, want: badValue + `|` + inventedState},
	// wl installs a snapshot in place, so B27 keeps the store from before
	// the install: the store it loaded differs from the checkpoint's
	// (sameState). Under wlFaultsConfig a follower rarely installed one
	// (1 seed in 64 caught it on 5795790d, none on d4344bc2).
	"B27": {seeds: 16, cfg: wlInstallConfig, want: inventedState + `|cold restart did not converge|` + stuck},
	"B28": {seeds: 16, cfg: wlMembershipConfig, want: `membership: stage 1 acknowledged`},
	// wl has no value cache: B31 is a read at a revision that ignores
	// the revision and gets the value that overwrote it.
	"B31": {seeds: 32, cfg: wlFaultsConfig, want: badValue + `|` + badState + `|` + badWatch},
	"B33": {seeds: 32, cfg: wlFaultsConfig, want: inventedState + `|malformed batch|` + stuck},
}

// wlDamageConfig runs wl nodes with crashes and, after them, damage at
// rest to the segments a start reads: misdirected and lost writes. No
// checkpoint cuts the log, which grows over the segments the damage
// needs (Sim.damageWL).
func wlDamageConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.FormUnderFaults = true
	cfg.Faults = Faults{CrashEvery: Second, MaxDown: Second, Corrupt: true}
	cfg.damageKinds = []damageKind{misdirected, lostWrite}
	cfg.Duration = 16 * Second
	return cfg
}

// TestPlanted switches on the bug that $SIM_PLANTED names and fails
// unless a check catches it within its seed budget, from seed 1. A crash
// is a failure too, not a catch. The catching run is saved, and its
// replay command printed. With $SIM_CONTROL set, it runs the seed without
// the bug, as the control of a failure that open bugs give too.
func TestPlanted(t *testing.T) {
	name := os.Getenv("SIM_PLANTED")
	if name == "" {
		t.Skip("no SIM_PLANTED bug selected")
	}
	c, ok := plantedCases[name]
	if !ok {
		t.Fatalf("%s has no case", name)
	}
	runPlanted(t, name, c)
}

// runPlanted is TestPlanted's run of case c of planted bug name.
func runPlanted(t *testing.T, name string, c plantedCase) {
	t.Helper()
	test := simrun.Top(strings.Split(t.Name(), "/"))[0]
	want := regexp.MustCompile(c.want)
	control := os.Getenv("SIM_CONTROL") != ""
	planted.Active, activePlanted, activeControl = name, name, control
	if control {
		planted.Active = ""
	}
	t.Cleanup(func() { planted.Active, activePlanted, activeControl = "", "", false })
	tried, caught := 0, false
	if !simrun.Seeds(t, 1, c.seeds, func(t *testing.T, seed uint64) plantedRun {
		res, err := runSeed(t, c.cfg(seed))
		if err == nil {
			err = independent(res)
		}
		if err == nil && c.extra != nil {
			err = c.extra(res)
		}
		switch {
		case err == nil:
			return plantedRun{}
		case control:
			return plantedRun{Finding: err.Error()}
		case !decides(err):
			t.Fatalf("seed %d of %s failed without a finding about the run, which catches nothing: %v", seed, name, err)
		case openBugs.MatchString(err.Error()) && !want.MatchString(err.Error()):
			t.Logf("seed %d of %s hit an open bug instead, #357 or #361, and catches nothing: %v", seed, name, err)
			return plantedRun{}
		case c.witness && !witnessCatches(t, res):
			return plantedRun{}
		case !want.MatchString(err.Error()):
			t.Logf("seed %d of %s failed a check other than the expected ones (%s); it catches the bug if the seed passes without it: %v", seed, name, c.want, err)
		}
		save(t, replayCommand(t.Name(), seed), res, true)
		return plantedRun{Finding: err.Error()}
	}, func(seed uint64, r plantedRun) bool {
		tried++
		if r.Finding == "" || control || failsUnplanted(t, test, name, seed) {
			return false
		}
		caught = true
		if !want.MatchString(r.Finding) {
			// The seed passes without the bug, but the bug may only have
			// moved the schedule onto another failure: one to look at.
			t.Logf("%s (%s) caught by seed %d with a check outside the expected ones (%s), which may be another bug the moved schedule reached: %s", name, planted.Bugs[name], seed, c.want, r.Finding)
		} else {
			t.Logf("%s (%s) caught by seed %d: %s", name, planted.Bugs[name], seed, r.Finding)
		}
		t.Logf("replay: %s", replayCommand(fmt.Sprintf("%s/%d", t.Name(), seed), seed))
		return true
	}) {
		return
	}
	if !caught && !control {
		t.Fatalf("%s (%s) was not caught in %d seeds", name, planted.Bugs[name], tried)
	}
}

// failsUnplanted runs seed of test, TestPlanted, again
// without planted bug name, as a gosim run of its own, the control of a
// catch, and reports whether it fails there too. A seed that fails as open bugs #357 and #361 do
// catches nothing; one that fails on anything else fails the test, as a
// failure with no planted bug. Under the race detector, where a seed does
// not replay, no catch counts.
func failsUnplanted(t *testing.T, test, name string, seed uint64) bool {
	t.Helper()
	if simrun.Race {
		return true
	}
	env := []string{fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1", "SIM_PLANTED=" + name, "SIM_CONTROL=1"}
	res := simrun.Do(t, simrun.Run{Test: test, Seed: seed, Env: env})
	var r plantedRun
	if err := json.Unmarshal(res.Artifacts["seed.json"], &r); err != nil {
		t.Errorf("the control of seed %d left no result: %v", seed, err)
		return true
	}
	switch {
	case r.Finding == "":
		return false
	case openBugs.MatchString(r.Finding):
		t.Logf("seed %d of %s fails without the bug too, and catches nothing: %s", seed, name, r.Finding)
	default:
		t.Errorf("seed %d of %s fails without the bug too: %s", seed, name, r.Finding)
	}
	return true
}

// wlInstallConfig is wlDamagedSnapshotConfig with no stream damaged: a
// follower held down installs the snapshot it fetches.
func wlInstallConfig(seed uint64) Config {
	cfg := wlDamagedSnapshotConfig(seed)
	cfg.Faults.FlipSnapshot = false
	return cfg
}
