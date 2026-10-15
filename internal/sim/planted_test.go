//go:build planted

package sim

import (
	"errors"
	"flag"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/cryptotest"

	"github.com/djosh34/tigerstore/internal/planted"
)

var plantedBug = flag.String("planted", "", "name of the planted bug TestPlanted switches on")

// plantedCase is how a planted bug must be caught: within seeds runs of
// cfg, by a failure whose message matches want, the assertion the
// semantics pre-work on #47 names for it. Any other failure fails the
// test, so a bug cannot pass on an unrelated check or a crash. extra adds
// an assertion of the case's own. Each cfg also runs in the plain tests
// (TestSeeds, TestStalledLeaderIsReplaced), where it must pass.
type plantedCase struct {
	seeds int
	cfg   func(seed uint64) Config
	want  string
	extra func(*Result) error
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
	// An acknowledged append that recovery or a cold restart lost.
	lostAppend = `reported \d+ durable|erasing node \d+`
	// A watch that broke the watch rules.
	badWatch = `watch \d+`
	// A read or the replayed log that disagrees with the model; not a
	// liveness failure, which names the call but not its node.
	badState = `call at \d+ ms on node \d+:|replaying the committed log`
	// A node applied a value that no client wrote.
	inventedState = `applied different entries|applied the same log to different states|differs from the final log|` + badState
	// Recovery refused a log it wrote itself.
	refused = `cannot recover its log`
	// A node read a value that is not the one it wrote, and stopped.
	badValue = `does not match its checksum|past the end of entry`
)

var plantedCases = map[string]plantedCase{
	"B01a": {seeds: 64, cfg: DefaultConfig, want: staleRead},
	"B01b": {seeds: 64, cfg: DefaultConfig, want: staleRead},
	"B02":  {seeds: 128, cfg: DefaultConfig, want: lostAppend},
	"B03":  {seeds: 128, cfg: DefaultConfig, want: lostAppend},
	"B04a": {seeds: 256, cfg: DefaultConfig, want: refused},
	"B04c": {seeds: 256, cfg: DefaultConfig, want: inventedState + `|without noticing|` + refused},
	// B04b cuts the last synced record, which can be a vote: the node then
	// votes twice in one term, and two leaders write different entries.
	"B04b": {seeds: 256, cfg: DefaultConfig, want: lostAppend + `|applied different entries`},
	"B05":  {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	"B06a": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06b": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06c": {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B06d": {seeds: 32, cfg: DefaultConfig, want: `watch \d+ got an event at revision \d+ after one at`},
	"B07":  {seeds: 64, cfg: DefaultConfig, want: badWatch},
	"B08":  {seeds: 32, cfg: DefaultConfig, want: badWatch},
	"B09a": {seeds: 16, cfg: DefaultConfig, want: badState},
	"B09b": {seeds: 16, cfg: DefaultConfig, want: badState},
	"B10":  {seeds: 64, cfg: DefaultConfig, want: badState + `|applied the same log to different states|` + badWatch},
	"B11":  {seeds: 64, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	"B12":  {seeds: 256, cfg: DefaultConfig, want: lostAppend + `|` + refused + `|` + badValue},
	"B15":  {seeds: 32, cfg: DefaultConfig, want: `no client sent it`},
	"B16":  {seeds: 128, cfg: DefaultConfig, want: lostAppend + `|` + refused},
	"B17": {seeds: 64, cfg: stalledLeaderConfig, want: `the stalled leader`, extra: func(r *Result) error {
		if len(r.StallStops) == 0 {
			return errors.New("the stalled leader never fenced itself")
		}
		return nil
	}},
	"B18": {seeds: 16, cfg: DefaultConfig, want: `unknown watch stream|holds \d+ watchers`},
	"B19": {seeds: 64, cfg: DefaultConfig, want: refused + `|not on disk`},
	"B20": {seeds: 64, cfg: DefaultConfig, want: `holds entries from \d+ in memory`},
	"B21": {seeds: 64, cfg: DefaultConfig, want: inventedState + `|wrote different checkpoints|already exists|` + refused},
	"B22": {seeds: 64, cfg: DefaultConfig, want: inventedState + `|` + badValue + `|malformed|` + refused},
	"B23": {seeds: 64, cfg: DefaultConfig, want: inventedState},
	"B24": {seeds: 16, cfg: DefaultConfig, want: badValue},
	"B25": {seeds: 64, cfg: DefaultConfig, want: badState + `|` + badWatch},
}

// plantedElsewhere are the bugs in code the simulator does not run. Each
// package's own TestPlanted runs a fixed scenario that must catch it; the
// CI job runs those packages too.
var plantedElsewhere = map[string]string{
	"B13": "internal/node",
	"B14": "internal/peer",
	"B26": "internal/node",
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
	ci, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*bug: \[(.*)\]$`).FindSubmatch(ci)
	if m == nil {
		t.Fatal("ci.yml has no planted bug matrix")
	}
	inCI := strings.Split(string(m[1]), ", ")
	for name := range planted.Bugs {
		if !slices.Contains(inCI, name) {
			t.Errorf("planted bug %s is not in the CI matrix", name)
		}
	}
	if len(inCI) != len(planted.Bugs) {
		t.Errorf("the CI matrix has %d bugs, the inventory %d", len(inCI), len(planted.Bugs))
	}
}

// TestPlanted switches on the bug named by -planted and fails unless a
// check catches it within its seed budget. A crash of the test binary is
// a failure too, not a catch.
func TestPlanted(t *testing.T) {
	name := *plantedBug
	if name == "" {
		t.Skip("no -planted bug selected")
	}
	if pkg, ok := plantedElsewhere[name]; ok {
		t.Skipf("%s is caught by TestPlanted in %s", name, pkg)
	}
	c, ok := plantedCases[name]
	if !ok {
		t.Fatalf("unknown planted bug %q", name)
	}
	want := regexp.MustCompile(c.want)
	planted.Active = name
	t.Cleanup(func() { planted.Active = "" })
	for seed := uint64(1); seed <= uint64(c.seeds); seed++ {
		caught := false
		if !t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cryptotest.SetGlobalRandom(t, seed)
			res, err := Run(c.cfg(seed))
			if err == nil {
				err = independent(res)
			}
			if err == nil && c.extra != nil {
				err = c.extra(res)
			}
			if err == nil {
				return
			}
			if !want.MatchString(err.Error()) {
				t.Fatalf("%s failed on another check than the expected one (%s): %v", name, c.want, err)
			}
			caught = true
			t.Logf("%s (%s) caught by seed %d: %v", name, planted.Bugs[name], seed, err)
		}) || caught {
			return
		}
	}
	t.Fatalf("%s (%s) was not caught within %d seeds", name, planted.Bugs[name], c.seeds)
}
