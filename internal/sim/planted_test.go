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
	"github.com/djosh34/tigerstore/internal/wal"
)

// plantedCase is how a planted bug must be caught: within seeds runs of
// cfg, by a failure whose message matches want, the assertion the
// semantics pre-work on #47 names for it. Any other failure fails the
// test, so a bug cannot pass on an unrelated check or a crash. extra adds
// an assertion of the case's own. With witness, the witness checker in
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

func stalledLeaderConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{}
	cfg.StallLeaderAt = 5 * Second
	cfg.LiveFrom = cfg.StallLeaderAt + 10*Second
	cfg.Duration = 25 * Second
	return cfg
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
	// A watch that broke the watch rules.
	badWatch = `watch \d+`
	// A read or the replayed log that disagrees with the model; not a
	// liveness failure, which names the call but not its node.
	badState = `call at \d+ ms on node \d+:|replaying the committed log`
	// A node applied a value that no client wrote.
	inventedState = `applied different entries|applied the same log to different states|differs from the final log|` + badState
	// Recovery refused a log it wrote itself.
	refused = `cannot recover its log`
	// A node read a value that is not the one it wrote, or that its log
	// no longer holds, and stopped.
	badValue = `does not match its checksum|past the end of entry|wal: read \d+ of \d+ bytes`
	// A node that never catches up, so the nodes end at different
	// indexes.
	stuck = `node \d+ applied \d+ entries, node \d+ applied \d+`
)

var plantedCases = map[string]plantedCase{
	"B01a": {seeds: 64, cfg: DefaultConfig, want: staleRead, witness: true},
	"B01b": {seeds: 64, cfg: DefaultConfig, want: staleRead, witness: true},
	// An append acknowledged before its sync can be lost with the vote
	// or term next to it, and then two leaders write different entries.
	"B02":  {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|applied different entries`},
	"B03":  {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|applied different entries`},
	"B04a": {seeds: 256, cfg: DefaultConfig, want: `without noticing|` + refused},
	// B04c also lets a damaged record through recovery that a snapshot
	// sender's read then refuses (#287).
	"B04c": {seeds: 256, cfg: DefaultConfig, want: inventedState + `|without noticing|` + refused + `|` + badValue + `|the record of entry \d+ in segment \d+ does not verify`},
	// B04b cuts the last synced record, which can be a vote: the node then
	// votes twice in one term, and two leaders write different entries.
	"B04b": {seeds: 256, cfg: DefaultConfig, want: lostAppend + `|applied different entries|applied the same log to different states`},
	"B05":  {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	"B06a": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06b": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06c": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06d": {seeds: 32, cfg: DefaultConfig, want: `watch \d+ got an event at revision \d+ after one at`},
	"B07":  {seeds: 64, cfg: DefaultConfig, want: badWatch},
	"B08":  {seeds: 128, cfg: DefaultConfig, want: badWatch},
	"B09a": {seeds: 16, cfg: DefaultConfig, want: badState},
	"B09b": {seeds: 16, cfg: DefaultConfig, want: badState},
	"B10":  {seeds: 64, cfg: DefaultConfig, want: badState + `|applied the same log to different states|` + badWatch},
	"B11":  {seeds: 64, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	// Since a snapshot install starts the node again from its log (#238),
	// that start can be where the damaged record is found.
	"B12": {seeds: 256, cfg: DefaultConfig, want: lostAppend + `|` + refused + `|` + badValue + `|the record of entry \d+ in segment \d+ does not verify|damaged record followed by a valid one`},
	"B15": {seeds: 32, cfg: DefaultConfig, want: `no client sent it`},
	"B16": {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	// Followers forward their writes to the stalled leader, which stays
	// leader, so the liveness check may find the timed-out calls first.
	"B17": {seeds: 64, cfg: stalledLeaderConfig, want: `the stalled leader|liveness: client \d+ call`, extra: func(r *Result) error {
		if len(r.Silences) == 0 {
			return errors.New("the stalled leader never went silent")
		}
		return nil
	}},
	"B18": {seeds: 16, cfg: DefaultConfig, want: `holds \d+ watchers`},
	"B19": {seeds: 64, cfg: DefaultConfig, want: refused + `|not on disk|` + stuck},
	"B20": {seeds: 64, cfg: DefaultConfig, want: `holds entries from \d+ in memory`},
	"B21": {seeds: 64, cfg: DefaultConfig, want: inventedState + `|wrote different checkpoints|already exists|` + refused},
	// The install starts the node again from its log (#238), where the
	// unchecked snapshot's damaged entry can be what recovery refuses.
	"B22": {seeds: 256, cfg: DefaultConfig, want: inventedState + `|` + badValue + `|malformed|` + refused + `|installed a snapshot at \d+ from \d+, checkpoint \d+\n[^\n]*wal: corrupt log:`},
	"B23": {seeds: 64, cfg: DefaultConfig, want: inventedState + `|wrote different checkpoints|` + stuck},
	"B24": {seeds: 16, cfg: DefaultConfig, want: badValue},
	"B25": {seeds: 64, cfg: DefaultConfig, want: badState + `|` + badWatch},
	// The real driver's disk goroutine ends at an install, so a core that
	// goes on writes nothing more.
	"B27": {seeds: 64, cfg: DefaultConfig, want: inventedState + `|cold restart did not converge|` + stuck},
	// A node refuses a member change unless it has been connected to the
	// other voters for 5 s, so stage 1 completes during the faults in
	// some runs and after healing in others; either catch counts.
	"B28": {seeds: 16, cfg: membershipConfig, want: `membership: stage 1 acknowledged`},
	"B31": {seeds: 16, cfg: cacheConfig, want: badValue + `|` + badState},
	"B32": {seeds: 128, cfg: loneHeaderConfig, want: refused},
}

// openBugs are the failures of open bugs #357 and #361, which the default
// seeds hit in about one in twenty: a lost entry, term or vote, or a
// checkpoint that a restart removed on a root it read from the page
// cache. Such a seed catches nothing, unless the planted bug is caught by
// the same failure and the seed passes without it (failsUnplanted). Their
// fixes remove it.
var openBugs = regexp.MustCompile(`reported \d+ durable|held term \d+ and vote \d+ durable|load the checkpoint at \d+: file does not exist`)

// plantedElsewhere are the bugs whose own package's TestPlanted runs a
// fixed scenario that must catch it; the CI job runs those packages too.
var plantedElsewhere = map[string]string{
	"B13": "internal/node",
	"B14": "internal/peer",
	"B26": "internal/node",
	"B29": "internal/node",
	"B30": "internal/node",
}

// TestPlantedInventory requires a case for every planted bug.
func TestPlantedInventory(t *testing.T) {
	for name := range planted.Bugs {
		_, here := plantedCases[name]
		_, there := plantedElsewhere[name]
		if here == there {
			t.Errorf("planted bug %s (%s) needs exactly one case, here or in another package", name, planted.Bugs[name])
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
		err := check.Witnessed(h, mode, time.Minute, witnessSteps)
		if err == nil || !witnessFinding.MatchString(err.Error()) {
			t.Logf("the witness checker (%s) did not find the stale read on this seed: %v", mode, err)
			return false
		}
		t.Logf("the witness checker (%s): %v", mode, err)
	}
	return true
}

// plantedRun is what a seed of TestPlanted found: the failure that
// catches the bug, if any, and whether open bugs #357 and #361 give that
// failure too, so that it counts only if the seed passes without the
// planted bug (SIM_CONTROL).
type plantedRun struct {
	Finding string
	Open    bool
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
	if pkg, ok := plantedElsewhere[name]; ok {
		t.Skipf("%s is caught by TestPlanted in %s", name, pkg)
	}
	c, ok := plantedCases[name]
	if !ok {
		t.Fatalf("unknown planted bug %q", name)
	}
	want := regexp.MustCompile(c.want)
	control := os.Getenv("SIM_CONTROL") != ""
	planted.Active, activePlanted = name, name
	if control {
		planted.Active = ""
	}
	t.Cleanup(func() { planted.Active, activePlanted = "", "" })
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
		case openBugs.MatchString(err.Error()) && !want.MatchString(err.Error()):
			t.Logf("seed %d of %s hit an open bug instead, #357 or #361, and catches nothing: %v", seed, name, err)
			return plantedRun{}
		case !want.MatchString(err.Error()):
			t.Fatalf("%s failed on another check than the expected one (%s): %v", name, c.want, err)
		case c.witness && !witnessCatches(t, res):
			return plantedRun{}
		}
		save(t, replayCommand(t.Name(), seed), res)
		return plantedRun{Finding: err.Error(), Open: openBugs.MatchString(err.Error())}
	}, func(seed uint64, r plantedRun) bool {
		tried++
		if r.Finding == "" || control || r.Open && failsUnplanted(t, name, seed) {
			return false
		}
		caught = true
		t.Logf("%s (%s) caught by seed %d: %s", name, planted.Bugs[name], seed, r.Finding)
		t.Logf("replay: %s", replayCommand(fmt.Sprintf("%s/%d", t.Name(), seed), seed))
		return true
	}) {
		return
	}
	if !caught && !control {
		t.Fatalf("%s (%s) was not caught in %d seeds", name, planted.Bugs[name], tried)
	}
}

// failsUnplanted runs seed of TestPlanted again without planted bug name,
// as a gosim run of its own, and reports whether it fails there too: then
// an open bug gave the failure, which catches nothing. Under the race
// detector, where a seed does not replay, such a failure never counts.
func failsUnplanted(t *testing.T, name string, seed uint64) bool {
	t.Helper()
	if simrun.Race {
		return true
	}
	env := []string{fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1", "SIM_PLANTED=" + name, "SIM_CONTROL=1"}
	res := simrun.Do(t, simrun.Run{Test: "TestPlanted", Seed: seed, Env: env})
	var r plantedRun
	if err := json.Unmarshal(res.Artifacts["seed.json"], &r); err != nil {
		t.Fatalf("the control of seed %d left no result: %v", seed, err)
	}
	if r.Finding != "" {
		t.Logf("seed %d of %s fails without the bug too, and catches nothing: %s", seed, name, r.Finding)
	}
	return r.Finding != ""
}

// TestB22InstallRefusal checks that B22 counts as caught when the start
// after an install refuses the log, as seed 34 once ended (#302).
func TestB22InstallRefusal(t *testing.T) {
	err := "seed 34 at 16384 ms: node 3 stopped: core: installed a snapshot at 24 from 1, checkpoint 0\n" +
		"0000000000000001.wal at byte 706: wal: corrupt log: entry 0 follows entry 10"
	if !regexp.MustCompile(plantedCases["B22"].want).MatchString(err) {
		t.Fatalf("B22 does not count %q as caught", err)
	}
}

// TestB04cSnapshotRefusal checks that B04c counts as caught when a snapshot
// sender stops on the damaged record that the bug let through recovery,
// as seed 1 once ended (#287).
func TestB04cSnapshotRefusal(t *testing.T) {
	err := fmt.Errorf("seed 1 at 20806 ms: node 3 stopped: %w", &wal.RecordError{Entry: 8, Segment: 1, Reason: "does not verify"})
	if !regexp.MustCompile(plantedCases["B04c"].want).MatchString(err.Error()) {
		t.Fatalf("B04c does not count %q as caught", err)
	}
}
