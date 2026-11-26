package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wallclock"
)

// The simulator's tests read their settings from the environment, which
// reaches them under gosim too, where they get no flags:
//   - SIM_SEED and SIM_SEEDS: the first seed of a test that runs a set of
//     seeds (simrun.Seeds), and how many. TestSeeds, TestLinearizable and
//     TestMembershipFaults run 16 from seed 1 by default.
//   - SIM_TWICE: how many seeds of the set scripts/sim.sh runs twice; each
//     must give the same trace, history and disk hashes both times.
//   - SIM_OUT: the directory where a failed run saves its trace, how it
//     left the nodes, its history, its disks and the command that runs it
//     again.
//   - SIM_PLANTED: the planted bug TestPlanted switches on.

// run runs one seed and fails the test if it breaks a rule.
func run(t *testing.T, cfg Config) *Result {
	t.Helper()
	res, err := runSeed(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// lastRun is the last run of each test, by its *testing.T.
var lastRun sync.Map

// runSeed runs one seed. If the test fails with no later run, the run
// explains itself (explain).
func runSeed(t *testing.T, cfg Config) (*Result, error) {
	t.Helper()
	return runWith(t, cfg, nil)
}

// runWith is runSeed with runInspect's inspect.
func runWith(t *testing.T, cfg Config, inspect func(*Sim) error) (*Result, error) {
	t.Helper()
	var res *Result
	var err error
	// An hour more than the run's length, which a run of hours needs.
	simrun.BubbleFor(t, time.Duration(cfg.length())+time.Hour, func(*testing.T) { res, err = runInspect(cfg, inspect) })
	if simrun.InGosim && res != nil {
		hashLines = append(hashLines, fmt.Sprintf("seed %d: %s", cfg.Seed, runHashes(res)))
		simrun.Artifact("hashes", []byte(strings.Join(hashLines, "\n")+"\n"))
		// The driver's double runs keep both runs' files, for when they
		// differ, once the seed's last simulation is over.
		if os.Getenv("SIM_KEEP") != "" {
			cmd := cfg.replay
			if cmd == "" {
				cmd = replayCommand(t.Name(), cfg.Seed)
			}
			// A long run's history takes gigabytes, which a run kept
			// only to compare with its second run leaves out.
			simrun.AfterSeed(func() {
				// A failed run is saved once, whole, as it is explained
				// (explain), within one budget: a save here too would
				// leave the files the second save leaves out (#806).
				if !t.Failed() {
					save(t, cmd, res, !res.long)
				}
			})
		}
	}
	me := new(int)
	lastRun.Store(t, me)
	t.Cleanup(func() {
		if last, _ := lastRun.Load(t); last == me {
			lastRun.Delete(t)
			if t.Failed() {
				explain(t, cfg, res)
			}
		}
	})
	return res, err
}

// runHashes is the run's hashes, taken once (Result.hashed): the first
// time comes before its checks, which drop a long run's history.
func runHashes(res *Result) string {
	if res.hashed == "" {
		res.hashed = hashes(res)
	}
	return res.hashed
}

// hashLines are the hashes of each run of the test, in order.
var hashLines []string

// hashes are the hashes of a run's trace, its history and each node's
// disk, now and right after its last crash. Under gosim, two runs of the
// same seed must give the same. Each is written straight into its hash,
// which a long run's history and disks need: built in memory first, a
// seed of 20,000 objects took 14.8 GiB there (#416).
func hashes(res *Result) string {
	sum := func(write func(io.Writer)) string {
		h := sha256.New()
		write(h)
		return hex.EncodeToString(h.Sum(nil)[:8])
	}
	var out strings.Builder
	fmt.Fprintf(&out, "trace %s history %s",
		sum(func(w io.Writer) { writeString(w, strings.Join(res.Trace, "\n")) }),
		sum(func(w io.Writer) { writeHistory(w, res) }))
	for _, n := range res.nodes {
		fmt.Fprintf(&out, " node%d %s", n.id, sum(func(w io.Writer) {
			file := func(name string, data []byte) {
				writef(w, "%s %d ", name, len(data))
				writeBytes(w, data)
				writeString(w, "\n")
			}
			n.disk.EachFile(file)
			writeString(w, "\n")
			for _, name := range slices.Sorted(maps.Keys(n.crashImage)) {
				file(name, n.crashImage[name])
			}
			writeString(w, "\n")
		}))
	}
	return out.String()
}

// writef, writeBytes and writeString write to a hash, whose writes never
// fail.
func writef(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func writeBytes(w io.Writer, b []byte) {
	_, _ = w.Write(b)
}

func writeString(w io.Writer, s string) {
	_, _ = io.WriteString(w, s)
}

// writeHistory writes every call and every watch response of a run, with
// its exact times, for its hash.
func writeHistory(w io.Writer, res *Result) {
	marshal := func(m proto.Message) {
		if m == nil {
			writeString(w, "nil\n")
			return
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			writef(w, "%T: %v\n", m, err)
			return
		}
		writef(w, "%T %d %x\n", m, len(b), b)
	}
	for _, r := range res.History {
		writef(w, "call %d %d %d %d %v %v\n", r.Client, r.Node, r.Call, r.Return, r.Err, r.Ambiguous)
		marshal(r.req)
		marshal(r.Resp)
	}
	for _, wr := range res.Watches {
		writef(w, "watch %q %q %v %v %d\n", wr.Key, wr.End, wr.PrevKV, wr.ProgressNotify, wr.Start)
		for _, r := range wr.Responses {
			writef(w, "at %d\n", r.Time)
			marshal(r.Resp)
		}
	}
}

// explainSim explains a scenario that drives s itself, as explain does a
// run: its trace, the nodes, their hashes, its saved files and the
// command that runs it again. It goes before s shuts down.
func explainSim(t *testing.T, cfg Config, s *Sim) {
	t.Helper()
	s.mu.Lock()
	var history []*Record
	var err error
	simrun.Aside(func() { history, err = s.records.records() })
	res := &Result{Clients: cfg.Clients + cfg.V3Clients, History: history, Stats: s.stats, nodes: s.nodes, records: s.records}
	s.mu.Unlock()
	if err != nil {
		t.Errorf("decoding the history: %v", err)
	}
	res.Report = s.report()
	s.mu.Lock()
	res.Trace = slices.Clone(s.trace)
	s.mu.Unlock()
	if simrun.InGosim {
		hashLines = append(hashLines, fmt.Sprintf("seed %d: %s", cfg.Seed, runHashes(res)))
		simrun.Artifact("hashes", []byte(strings.Join(hashLines, "\n")+"\n"))
	}
	explain(t, cfg, res)
}

// explain logs what a failed run did, how it left each node, its hashes
// and the command that runs its seed again, and saves the run (save).
func explain(t *testing.T, cfg Config, res *Result) {
	cmd := cfg.replay
	if cmd == "" {
		cmd = replayCommand(t.Name(), cfg.Seed)
	}
	if res == nil {
		t.Logf("replay: %s", cmd)
		return
	}
	// A long run's checks drop its history once they have their own; the
	// save decodes it again if the history fits its budget (runFiles).
	t.Logf("trace of seed %d:\n%s", cfg.Seed, strings.Join(res.Trace, "\n"))
	t.Logf("the nodes at the end of seed %d:\n%s", cfg.Seed, res.Report)
	t.Logf("hashes of seed %d: %s", cfg.Seed, runHashes(res))
	save(t, cmd, res, true)
	t.Logf("replay: %s", cmd)
}

// save keeps a run's files (runFiles), its history with history, in
// $SIM_OUT, under the test's name. Under gosim the driver writes them
// there.
func save(t *testing.T, cmd string, res *Result, history bool) {
	name := strings.ReplaceAll(t.Name(), "/", "_")
	if simrun.InGosim {
		for path, body := range runFiles(filepath.Join("$SIM_OUT", name), cmd, res, history) {
			simrun.Artifact(filepath.Join("sim-out", name, path), []byte(body))
		}
		return
	}
	out := os.Getenv("SIM_OUT")
	if out == "" {
		return
	}
	dir := filepath.Join(out, name)
	files := map[string][]byte{}
	for path, body := range runFiles(dir, cmd, res, history) {
		files[path] = []byte(body)
	}
	if err := simrun.WriteFiles(dir, files); err != nil {
		t.Logf("saving the run in %s: %v", dir, err)
		return
	}
	t.Logf("the run is saved in %s", dir)
}

// seedRange is the n seeds from first.
func seedRange(first uint64, n int) []uint64 {
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = first + uint64(i)
	}
	return seeds
}

// runFiles are a run's trace, node report, stats, hashes, history, with
// history, disks and replay command, by path, for a directory dir of
// their own.
func runFiles(dir, cmd string, res *Result, history bool) map[string]string {
	room := &saveRoom{left: math.MaxInt64}
	if simrun.InGosim {
		// The saves of one run under gosim share its budget: a kept run
		// that then fails is saved twice (runWith).
		if gosimRooms[dir] == nil {
			gosimRooms[dir] = &saveRoom{left: gosimSaveBudget}
		}
		room = gosimRooms[dir]
	}
	return runFilesWithin(dir, cmd, res, history, room)
}

// gosimRooms are the budgets of a gosim run's saves, by their directory.
var gosimRooms = map[string]*saveRoom{}

// runFilesWithin is runFiles within room's budget for the large files.
func runFilesWithin(dir, cmd string, res *Result, withHistory bool, room *saveRoom) map[string]string {
	room.out = nil
	files := map[string]string{
		"trace.txt":  strings.Join(res.Trace, "\n") + "\n",
		"nodes.txt":  res.Report,
		"replay.txt": cmd + "\n",
		"stats.txt":  fmt.Sprintf("%+v\n", res.Stats),
		"hashes.txt": runHashes(res) + "\n",
	}
	if simrun.WallLimitPassed() {
		// The run took too long: where each goroutine was, the nodes'
		// included, as the run's watchdog saw them, or now when a check
		// passed the limit after the run.
		stacks := res.stacks
		if stacks == nil {
			stacks = simrun.Stacks()
		}
		files["stacks.txt"] = string(stacks)
		files["replay.txt"] += "the run passed its limit of real time ($SIM_WALL); stacks.txt has every goroutine's stack\n"
	}
	if !withHistory {
		files["replay.txt"] += "no history.json and no calls.txt: a long run kept to compare with its second run leaves them out; the replay command plays the seed again\n"
		addDisks(files, res, room)
		room.note(files)
		return files
	}
	// Every call, as the clients saw it, also when the run failed before
	// its checks built the committed log that history.json needs. A long
	// run's history takes gigabytes, so it gets calls.txt only without
	// history.json. Each goes only within the budget, judged from the
	// record before the history is decoded again, which a long run's
	// checks dropped (saveBounds).
	callsMax, historyMax := res.saveBounds()
	wantCalls := !res.long || !res.logged
	var callsOK, historyOK bool
	if wantCalls {
		callsOK = room.take("calls.txt", callsMax)
		historyOK = res.logged && room.take("history.json", historyMax)
	} else {
		// A long run's calls go only in place of its history.
		if historyOK = room.take("history.json", historyMax); !historyOK {
			callsOK = room.take("calls.txt", callsMax)
		}
	}
	if (callsOK || historyOK) && res.History == nil {
		decoded, err := res.history()
		if err != nil {
			files["replay.txt"] += fmt.Sprintf("decoding the history again: %v\n", err)
		}
		res.History = decoded
	}
	var h *history.History
	var err error
	if historyOK {
		if h, err = res.Checkable(); err != nil {
			// No history.json after all: its room goes to the calls.
			room.give("history.json", historyMax)
			if !callsOK {
				callsOK = room.take("calls.txt", callsMax)
			}
		}
	}
	if callsOK {
		files["calls.txt"] = calls(res)
	}
	switch {
	case !res.logged && callsOK:
		files["replay.txt"] += "no history.json: the run failed before its checks built the committed log, which the checkers need; calls.txt has the calls\n"
	case !res.logged:
		files["replay.txt"] += "no history.json: the run failed before its checks built the committed log, which the checkers need\n"
	case !historyOK:
	case err != nil:
		files["replay.txt"] += fmt.Sprintf("no history.json: %v\n", err)
	default:
		// Not indented: a long run's history takes gigabytes as it is.
		b, err := json.Marshal(h)
		if err != nil {
			files["replay.txt"] += fmt.Sprintf("no history.json: %v\n", err)
			break
		}
		files["history.json"] = string(b) + "\n"
		checkers := "witness-log,witness-revisions"
		if check.ShortEnough(h) {
			checkers += ",linearizable"
		}
		files["replay.txt"] += "go run ./cmd/checkhistory -checkers " + checkers + " " + filepath.Join(dir, "history.json") + "\n"
	}
	addDisks(files, res, room)
	room.note(files)
	return files
}

// saveBounds are bounds on the bytes of a run's calls.txt and its
// history.json, judged from its record without decoding its history:
//   - a value's byte takes at most 6 in either: JSON escapes a control
//     byte as \u00XX, and a byte that is not UTF-8 becomes \ufffd; calls.txt
//     prints one as \xXX;
//   - the messages' other bytes take at most 24 each: a field's tag and
//     its smallest value are 2 bytes, and print as its name and the value
//     in decimal;
//   - a call's error, as often as it comes, at most 6 a byte;
//   - each message's own fields (client, node, times, a watch response's
//     time) at most 512;
//   - history.json holds the committed log's requests too, and a fork's
//     image's store (history.Start), at 24 a byte.
//
// Values are most of a bulk run; the messages most of a run of small
// values. TestSaveRunKeepsDisks checks both bounds against its run's
// files.
func (r *Result) saveBounds() (calls, history int64) {
	if r.records == nil {
		return 0, 0
	}
	values, messages, n, errs := r.records.sizes()
	calls = 6*values + 24*messages + 512*int64(n) + 6*errs
	log := int64(0)
	for _, m := range r.Log {
		log += int64(proto.Size(m))
	}
	// A fork's history starts from its image's store (history.Start):
	// its keys and values with 64 bytes a key for their fields, and 32 a
	// lease, counted as the log is.
	if st := r.start; st != nil {
		for _, kv := range st.KVs {
			log += int64(len(kv.Key)+len(kv.Value)) + 64
		}
		log += 32 * int64(len(st.Leases))
	}
	return calls, calls + 24*log
}

// gosimSaveBudget is how many bytes of a failed run's large files, its
// history and its disks, a run under gosim saves (#775). The gosim fork
// keeps a copy of every file it saves until the run ends, then hands them
// all to the driver as one JSON value, base64-encoded, which the driver
// decodes whole: a bulk run of 160 MB that failed took its runner from
// 2.8 to 15.7 GB in a minute and was shut down with its evidence. The
// replay command plays the seed again; raised here, it saves them all.
const gosimSaveBudget = 256 << 20

// saveRoom is what is left of a save's budget for the large files, the
// ones it saved, and out the ones the last save left out for it.
type saveRoom struct {
	left  int64
	saved map[string]bool
	out   []string
}

// take reports whether a file, or a set of them, of n bytes fits what is
// left, and takes it if it does. One saved already is saved again, the
// same, at no cost.
func (r *saveRoom) take(name string, n int64) bool {
	if r.saved[name] {
		return true
	}
	if r.saved == nil {
		r.saved = map[string]bool{}
	}
	if n > r.left {
		r.out = append(r.out, fmt.Sprintf("%s (%d MB)", name, n/1e6))
		return false
	}
	r.left -= n
	r.saved[name] = true
	return true
}

// give hands back the n bytes taken for a file that was not made after
// all.
func (r *saveRoom) give(name string, n int64) {
	if r.saved[name] {
		delete(r.saved, name)
		r.left += n
	}
}

// note says in replay.txt which files the budget left out.
func (r *saveRoom) note(files map[string]string) {
	if len(r.out) > 0 {
		files["replay.txt"] += fmt.Sprintf("left out, past the %d MiB a run under gosim saves (gosimSaveBudget in internal/sim/sim_test.go, which the replay saves them all with if raised): %s\n",
			gosimSaveBudget>>20, strings.Join(r.out, ", "))
	}
}

// addDisks adds each node's last disk calls and its files now and right
// after its last crash, and the disks a failed cold restart started from,
// with that restart's disk calls (#274).
func addDisks(files map[string]string, res *Result, room *saveRoom) {
	// A failed cold restart's disks first: when the restart is what
	// failed, they are its evidence.
	if c := res.cold; c != nil {
		for i, d := range c.disks {
			name := fmt.Sprintf("node%d", i+1)
			if i == c.erased {
				name += "-erased"
			}
			dir := filepath.Join("cold", fmt.Sprintf("copy-at-%dms", c.at/Millisecond), name)
			if room.take(dir, int64(d.Size())) {
				addFiles(files, dir, d.Files())
			}
		}
		for _, n := range c.nodes {
			files[filepath.Join("cold", fmt.Sprintf("node%d-calls.txt", n.id))] = strings.Join(n.diskLines(), "\n") + "\n"
		}
	}
	for _, n := range res.nodes {
		d := filepath.Join("disks", fmt.Sprintf("node%d", n.id))
		files[filepath.Join(d, "calls.txt")] = strings.Join(n.diskLines(), "\n") + "\n"
		if room.take(filepath.Join(d, "now"), int64(n.disk.Size())) {
			addFiles(files, filepath.Join(d, "now"), n.disk.Files())
		}
		image := 0
		for _, b := range n.crashImage {
			image += len(b)
		}
		if room.take(filepath.Join(d, "after-crash"), int64(image)) {
			addFiles(files, filepath.Join(d, "after-crash"), n.crashImage)
		}
	}
}

// calls is every call of a run, as the clients saw it.
func calls(res *Result) string {
	var b strings.Builder
	for _, r := range res.History {
		answer := fmt.Sprint(r.Resp)
		if r.Err != nil {
			answer = fmt.Sprintf("error %v, ambiguous %v", r.Err, r.Ambiguous)
		}
		fmt.Fprintf(&b, "client %d, node %d, %s to %s: %T %v -> %s\n", r.Client, r.Node+1, seconds(r.Call), seconds(r.Return), r.req, r.req, answer)
	}
	return b.String()
}

// addFiles puts each of a disk's files into files, under dir.
func addFiles(files map[string]string, dir string, disk map[string][]byte) {
	for name, b := range disk {
		files[filepath.Join(dir, name)] = string(b)
	}
}

// activePlanted is the planted bug TestPlanted switched on, if any, and
// activeControl set when TestPlanted runs its seeds without it.
var (
	activePlanted string
	activeControl bool
)

// replayCommand is the command that runs test name's run of seed again:
// scripts/sim.sh, or go test natively under the race detector. A subtest
// named after the seed is one of a set (simrun.Seeds, simrun.Each).
func replayCommand(name string, seed uint64) string {
	parts := simrun.Top(strings.Split(name, "/"))
	var env []string
	if len(parts) > 1 && parts[1] == strconv.FormatUint(seed, 10) {
		env = append(env, fmt.Sprintf("SIM_SEED=%d", seed), "SIM_SEEDS=1")
	}
	if activePlanted != "" {
		env = append(env, "SIM_PLANTED="+activePlanted)
	}
	if activeControl {
		env = append(env, "SIM_CONTROL=1")
	}
	pattern := simrun.Pattern(name)
	if !simrun.Race {
		return simrun.Replay("./internal/sim", pattern, env)
	}
	cmd := strings.Join(append(env, "go test -race -v"), " ")
	if activePlanted != "" {
		cmd += " -tags planted"
	}
	return cmd + fmt.Sprintf(" -count=1 ./internal/sim/ -run '%s'", pattern)
}

// TestSeeds runs every seed on wl nodes (Config.wl) with the checkers,
// and every fault main's runs (wlFaultsConfig); 7 seeds in 8 form their
// cluster first (formsFirst). A failing seed prints the command that runs
// it again; it goes in TestWLFormsAndRunsTheFaults, or in
// TestWLRunsTheFaults if it formed under the faults.
func TestSeeds(t *testing.T) {
	var total Stats
	var took effects
	ran := 0
	if !simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) Stats {
		a := runWLFaults(t, seed, false)
		st := a.Stats
		if st.Compactions == 0 || st.Revokes == 0 || st.OK < 200 || len(a.Log) < 50 {
			t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
		}
		return st
	}, func(seed uint64, st Stats) bool {
		took.add(seed, faultsTaken(st))
		ran++
		total.Fenced += st.Fenced
		total.Silences += st.Silences
		total.Asymmetric += st.Asymmetric
		total.RecoveryCrashes += st.RecoveryCrashes
		total.ErasedAtRoot += st.ErasedAtRoot
		total.ErasedAtRemoval += st.ErasedAtRemoval
		total.Disk.TornWrites += st.Disk.TornWrites
		total.Disk.Reordered += st.Disk.Reordered
		total.TornRefusals += st.TornRefusals
		total.OutOfSpace += st.OutOfSpace + st.NoRoom
		return false
	}) {
		return
	}
	// Random faults can end a disk stall before the leader goes silent
	// (#218), so each shard also runs the deliberate scenario.
	if ran >= 64 {
		t.Run("TestWLStalledLeaderIsReplaced", TestWLStalledLeaderIsReplaced)
		// wl's log has one write in flight, synced as it is written, and
		// during the faults a node writes no checkpoint, so no crash of a
		// random seed keeps a later write and loses an earlier one; the
		// scenario crashes nodes partway through checkpoints of several
		// writes until one does.
		t.Run("TestWLCrashKeepsALaterCheckpointWrite", func(t *testing.T) { total.Disk.Reordered += runCheckpointCrash(t) })
	}
	// Corruption, a link 10 times slower than its other direction and the
	// other common events are rare in one run, but a set of seeds must see
	// each. Main's set also needed packs of its cleaner and lease
	// checkpoints in the log. wl has neither: it cuts its log behind a
	// checkpoint instead of cleaning it, which the copy at a removal
	// (ErasedAtRemoval) needs, and a new leader starts every lease's TTL
	// over, as etcd does without lease checkpoints. During the faults a wl
	// node's log is a few hundred bytes in one segment with no checkpoint,
	// so the damage drawn mostly finds no place: it then flips a record of
	// the log that a record that checks follows (flipWLRecord), which
	// fences the node. The crashes that keep a later write come from the
	// checkpoint scenario above.
	if ran >= 64 && !t.Failed() && (total.Fenced == 0 || total.Asymmetric == 0 ||
		total.RecoveryCrashes == 0 || total.ErasedAtRoot == 0 || total.ErasedAtRemoval == 0 ||
		total.Disk.Reordered == 0 || total.OutOfSpace == 0) {
		t.Fatalf("%d seeds: %d fenced nodes, %d asymmetric links, %d crashes in recovery, %d disks erased after a first checkpoint, %d after a removal, %d crashes that kept a write and lost an earlier one, %d nodes out of space, stopped or with a checkpoint held; each must happen",
			ran, total.Fenced, total.Asymmetric, total.RecoveryCrashes, total.ErasedAtRoot, total.ErasedAtRemoval, total.Disk.Reordered, total.OutOfSpace)
	}
	// A crash tears a write in about 1 seed in 300, so 400 seeds have none
	// about one time in four; a set of 2000, as in the seed campaign,
	// holds about 7 (#378).
	if ran >= 2000 && !t.Failed() && total.Disk.TornWrites == 0 {
		t.Fatalf("%d seeds: no write a crash tore; a set of 2000 seeds or more must have one", ran)
	}
	t.Logf("%d seeds: %d writes a crash tore, %d crashes kept a write and lost an earlier one, %d nodes refused their torn crash image",
		ran, total.Disk.TornWrites, total.Disk.Reordered, total.TornRefusals)
	// A stalled leader that goes silent is rarer: about 0.9% of seeds had
	// one when it handed over instead (#218). Over 2000 seeds even 0.7%
	// leaves less than one chance in a million of none. CI's sets of 200
	// do not reach that, the seed campaign does.
	if ran >= 2000 && !t.Failed() && total.Silences == 0 {
		t.Fatalf("%d seeds: no stalled leader went silent; one must", ran)
	}
	t.Logf("%d seeds: %d stalled leaders went silent", ran, total.Silences)
	took.check(t)
}

// faultsTaken says which of the faults of a default run took effect in a
// run with stats st.
func faultsTaken(st Stats) map[string]bool {
	return map[string]bool{
		"crash": st.Crashes > 0, "link change": st.LinkChanges > 0, "two cold restarts": st.ColdRestarts >= 2,
		"disk fault": st.DiskFaults > 0, "pause": st.Pauses > 0, "link stall": st.LinkStalls > 0,
		"half-open link": st.HalfOpens > 0, "gray node": st.GrayNodes > 0, "gray client": st.GrayStreams > 0,
		"kill": st.Kills > 0 || simrun.Race, "clientv3 call": st.V3OK > 0, "link fault": slices.ContainsFunc(st.Netem[:], func(n int) bool { return n > 0 }),
		"connection reset": st.Resets > 0, "slow watcher": st.SlowWatchers > 0,
	}
}

// effects counts, over a set of runs, the runs in which each fault took
// effect. A fault that took effect in too few of them fails the test,
// not the run it missed, and names the seeds it missed. With all set,
// every run must show each fault.
type effects struct {
	runs   int
	all    bool
	took   map[string]int
	missed map[string][]uint64
}

// add counts the run of seed, with whether each fault took effect in it.
func (e *effects) add(seed uint64, took map[string]bool) {
	if e.took == nil {
		e.took, e.missed = map[string]int{}, map[string][]uint64{}
	}
	e.runs++
	for name, ok := range took {
		n := e.took[name]
		if ok {
			n++
		} else {
			e.missed[name] = append(e.missed[name], seed)
		}
		e.took[name] = n
	}
}

// check fails t unless each fault took effect in atLeast of the seeds,
// or in all of them, and logs the counts. A failure names up to five
// seeds that missed the fault, with the command that runs each again.
func (e *effects) check(t *testing.T) {
	t.Helper()
	least := atLeast(e.runs)
	if e.all {
		least = e.runs
	}
	var counts []string
	for _, name := range slices.Sorted(maps.Keys(e.took)) {
		n := e.took[name]
		counts = append(counts, fmt.Sprintf("%s %d", name, n))
		if n >= least {
			continue
		}
		missed := e.missed[name][:min(5, len(e.missed[name]))]
		t.Errorf("%s took effect in %d of %d seeds, want %d at least; it missed seeds %v", name, n, e.runs, least, missed)
		for _, seed := range missed {
			t.Logf("replay: %s", replayCommand(t.Name()+"/"+strconv.FormatUint(seed, 10), seed))
		}
	}
	t.Logf("of %d seeds, the faults took effect in: %s", e.runs, strings.Join(counts, ", "))
}

// checkers runs the independent checkers of package check on a run:
// durability, revisions and watches. Linearizability runs on the smaller
// histories of TestLinearizable.
func checkers(t *testing.T, res *Result) {
	t.Helper()
	if err := independentWithSteps(res, func(steps int64, cpu, wall time.Duration) {
		t.Logf("revision witness used %d weighted steps (budget %d, native CPU %s, search wall %s, %.2f steps/s)",
			steps, witnessStepLimit, cpu, wall, float64(steps)/wall.Seconds())
	}, witnessStepLimit); err != nil {
		t.Fatal(err)
	}
}

// Under gosim the clock stands still while a check computes, so weighted
// steps are the deterministic bound; wallStop is only a backstop. #872
// charges ordinary inner work at 1/1,024 and cheap capacity-vector items
// and sort estimates at 1/16,384; index work stays at 1/256 and operation
// turns at one. Stop polls still tick every 64 inner items, independently
// of those weights. After #876's retained eligibility intersection, no-cap
// measurements against base 37e51817 (maximum of three runs, ratios
// rounded up to four decimals):
//
//	history                     base  weighted   ratio
//	planted-B01a-1                10        10  1.0000
//	seed-5058843                1479      1662  1.1238
//	seed-5068434                 107       114  1.0655
//	seed-59113215                272       293  1.0773
//	seed-77148                   777       841  1.0824
//	seed-888737                  141       156  1.1064
//	witness-425                23712     25426  1.0723
//	witness-control-80-201      9764     12296  1.2594
//	witness-control-320-1529   15881    110320  6.9467
//	81000 same-revision grants 87330     89394  1.0237
//	256 gaps, 4096 expiries    13271     62819  4.7336
//
// Round the worst ratio, 110,320 / 15,881, up to seven: the old
// 50,000,000 * 7 = 350,000,000 preserves every measured margin; ordinary
// checks get 360,000,000, a little above that floor. Long-seed revision
// checks instead get 180,000,000: run 45803627951 used 219,708,153 steps
// in about 149 search minutes and stopped at 68% of seed 1. Its measured
// rate (about 24,600 steps/s) puts 180M near 122 search minutes, leaving
// the six-minute simulation and more than 15% margin before the 160-minute
// wall backstop. That budget, not wall time, must stop long searches.
// #876 intersects due sets with retained revision eligibility to avoid
// trials of future expiries, but seed 1 still exhausts its deterministic
// cap and remains a known timeout. Budgets are unchanged; its remaining
// search work needs a separate ruling. See longSeedWallLimit for sizing.
const (
	witnessStepLimit     int64 = 360_000_000
	longWitnessStepLimit int64 = 180_000_000
)

var (
	witnessSteps   = check.MaxSteps(witnessStepLimit)
	porcupineSteps = check.MaxSteps(1_000_000)
	wallStop       = check.StopWhen(simrun.WallLimitPassed)
)

// independent runs the durability, revision and watch checkers of package
// check on a run.
func independent(res *Result) error {
	return independentWithSteps(res, nil, witnessStepLimit)
}

// independentWithSteps reports the revision witness's weighted count before
// returning its verdict, including a timeout. Long runs log it for sizing
// their deterministic budget without relying on the real-time backstop.
func independentWithSteps(res *Result, report func(int64, time.Duration, time.Duration), budget int64) error {
	// A long run's checks drop its history as they convert it: they need
	// only their own copy, and a report decodes it again (#500). Its
	// hashes, which cover the history, are taken before.
	if res.long {
		runHashes(res)
	}
	h, err := res.checkable(res.long)
	if err != nil {
		return err
	}
	var errs []error
	if err := check.Durable(h); err != nil {
		errs = append(errs, fmt.Errorf("durability: %w", err))
	}
	if err := check.Revisions(h); err != nil {
		errs = append(errs, fmt.Errorf("revisions: %w", err))
	}
	if err := check.Watches(h); err != nil {
		errs = append(errs, fmt.Errorf("watches: %w", err))
	}
	for _, mode := range []check.WitnessMode{check.FromLog, check.FromRevisions} {
		var steps int64
		var cpuFrom time.Duration
		var wallFrom int64
		var cpuErr error
		measure := mode == check.FromRevisions && report != nil
		if measure {
			cpuFrom, cpuErr = simrun.CPUTime()
			wallFrom = wallclock.Now()
		}
		err := check.Witnessed(h, mode, time.Minute, check.MaxSteps(budget), wallStop, check.StepsUsed(&steps))
		if measure {
			// Timing is diagnostic: a failed counter must not change a
			// check's verdict. Zero means the counter was unavailable.
			var elapsed time.Duration
			if cpuTo, err := simrun.CPUTime(); cpuErr == nil && err == nil {
				elapsed = max(0, cpuTo-cpuFrom)
			}
			report(steps, elapsed, time.Duration(max(0, wallclock.Now()-wallFrom)))
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("witness: %w", err))
		}
	}
	return errors.Join(errs...)
}

// agree runs the witness checker in both modes on a run's history, and
// Porcupine too if the history is short enough for it
// (check.ShortEnough). All of them must pass; a history that one passes
// and another fails is a checker bug.
//
// Porcupine gets the history without the ambiguous writes the committed
// log shows never took effect, as chaos and pastram give it: each one it
// must keep open to the end of the history, and a few dozen made its
// search run past its steps on histories that are linearizable (#729).
// Leaving them out can only make the check stricter.
func agree(h *history.History) error {
	names := []string{"witness from the log", "witness from the revisions"}
	errs := []error{check.Witnessed(h, check.FromLog, time.Minute, witnessSteps, wallStop), check.Witnessed(h, check.FromRevisions, time.Minute, witnessSteps, wallStop)}
	if check.ShortEnough(h) {
		names = append(names, "linearizability")
		lin, _, err := h.WithoutLostWrites()
		if err == nil {
			err = check.Linearizable(lin, 3*time.Minute, porcupineSteps, wallStop)
		}
		errs = append(errs, err)
	}
	failed := 0
	var verdicts []string
	for i, err := range errs {
		v := "passes"
		if err != nil {
			failed++
			v = err.Error()
		}
		verdicts = append(verdicts, names[i]+" "+v)
	}
	switch failed {
	case 0:
		return nil
	case len(errs):
		return fmt.Errorf("%s: %w", strings.Join(names, ", "), errors.Join(errs...))
	}
	return fmt.Errorf("the checkers disagree: %s", strings.Join(verdicts, "; "))
}

// linearizableConfig is a short run whose history Porcupine can check:
// 5 virtual seconds with the same faults and 4 clients, one of them a
// client of etcd's Go client, then 8 s of healing, without leases.
func linearizableConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Duration = 5 * Second
	cfg.Heal = 8 * Second
	cfg.Clients, cfg.V3Clients = 3, 1
	cfg.NoLeases = true
	return cfg
}

// TestFormedClusterAnswers runs seeds that form their cluster first, with
// no faults after it, and requires every call to get a definite answer,
// within 500 ms from 2 s on: the run starts on the formed disks with
// nothing held from the formation. The nodes keep the core's default
// window: with the simulator's tiny one, a follower a few entries behind,
// as after an election, gets a snapshot, whose install starts its core
// again and ends the calls it held. Seed 3 then timed out a call while a
// node fetched one snapshot after another (#518), and seed 1 got "server
// stopped" under the race detector.
func TestFormedClusterAnswers(t *testing.T) {
	simrun.Each(t, []uint64{1, 2, 3}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.Window = true
		cfg.Duration = 10 * Second
		if !formsFirst(cfg) {
			t.Fatalf("seed %d forms its cluster under the faults; this test needs one that forms it first", seed)
		}
		res := run(t, cfg)
		slowest := int64(0)
		for _, r := range res.History {
			if r.Ambiguous {
				t.Fatalf("seed %d: call at %s on node %d got no definite answer with no faults: %v", seed, seconds(r.Call), r.Node+1, r.Err)
			}
			if r.Call >= 2*Second {
				slowest = max(slowest, r.Return-r.Call)
			}
		}
		if slowest > 500*Millisecond {
			t.Fatalf("seed %d: a call after 2 s took %s with no faults", seed, seconds(slowest))
		}
		t.Logf("seed %d: %d calls, the slowest after 2 s %s", seed, len(res.History), seconds(slowest))
		return struct{}{}
	}, nil)
}

// TestWallLimit runs seeds whose limit of real time is 1 ms: each run
// must fail as past its limit, and its saved files must have every
// goroutine's stack, the nodes' included, next to its trace, calls and
// disks (#499). Seed 1 forms its cluster under the faults, so the limit
// passes in the run, and seed 2 forms it first, so it passes in the
// formation, which has a watchdog of its own.
func TestWallLimit(t *testing.T) {
	if simrun.Race {
		t.Skip("natively go test's -timeout bounds a run")
	}
	if !simrun.InGosim {
		t.Setenv("SIM_WALL", "1ms")
	}
	simrun.Each(t, []uint64{1, 2}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.FormUnderFaults = seed == 1
		if !cfg.FormUnderFaults && !formsFirst(cfg) {
			t.Fatalf("seed %d forms its cluster under the faults; this test needs one that forms it first", seed)
		}
		res, err := runSeed(t, cfg)
		if !errors.Is(err, errWallLimit) {
			t.Fatalf("seed %d: a run past its limit of real time ended with %v", seed, err)
		}
		files := runFiles(fmt.Sprintf("$SIM_OUT/TestWallLimit/%d", seed), replayCommand(t.Name(), seed), res, true)
		// The nodes' own goroutines, as their loops'.
		if !strings.Contains(files["stacks.txt"], "tigerstore/internal_/wl.(*loop)") {
			t.Fatalf("seed %d: the saved stacks have none of the nodes' goroutines:\n%s", seed, files["stacks.txt"])
		}
		for _, name := range []string{"trace.txt", "nodes.txt", "replay.txt", "disks/node1/calls.txt"} {
			if files[name] == "" {
				t.Errorf("seed %d: the saved run has no %s", seed, name)
			}
		}
		// It may end before any call returned.
		if _, ok := files["calls.txt"]; !ok {
			t.Errorf("seed %d: the saved run has no calls.txt", seed)
		}
		return struct{}{}
	}, nil)
}

// TestLinearizable checks short runs of wl nodes with Porcupine and the
// witness checker, which must agree. Not finishing in time is a failure. Nearly
// every run must be short enough for Porcupine (check.ShortEnough).
func TestLinearizable(t *testing.T) {
	n, short := 0, 0
	if !simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) bool {
		res := run(t, wlLinearizableConfig(seed))
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := agree(h); err != nil {
			t.Fatal(err)
		}
		t.Logf("%d ops checked in %v", len(h.Ops), time.Since(start))
		return check.ShortEnough(h)
	}, func(_ uint64, ok bool) bool {
		n++
		if ok {
			short++
		}
		return false
	}) {
		return
	}
	if least := atLeast(n); !t.Failed() && short < least {
		t.Fatalf("Porcupine ran on %d of %d histories, want %d at least: the others were longer than %d operations", short, n, least, check.ShortOps)
	}
}

// atLeast is how many of n runs must show something that most runs show:
// 95% of them, rounded down.
func atLeast(n int) int { return n * 95 / 100 }

// TestStalledLeaderIsReplaced stalls the leader's disk for good at 5 s,
// with no other fault. The leader would go on heartbeating, so it must go
// silent for the others to elect another (#233): core.StallTicks
// election timeouts after the oldest append it has not persisted, which
// is the first one the stall holds, one queued before the stall or the
// first after it. It must go silent by then, plus two ticks, as the core
// counts ticks, or step down first: once the stalled disk has filled the
// node's queues, its committer takes no more messages, the followers'
// answers included, and CheckQuorum steps it down after one election
// timeout without them, which the race detector's scheduling reached
// (#457), and gosim's seed 54 does. Every call to a healthy node that
// starts 10 election timeouts after the stall must succeed. A leader can
// propose nothing for a while after the stall, as when it serves a
// snapshot (#260).
func TestStalledLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	simrun.Each(t, []uint64{1, 2, 3, 54}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.StallLeaderAt = 5 * Second
		cfg.LiveFrom = cfg.StallLeaderAt + 10*election
		cfg.Duration = 25 * Second
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
		limit := res.HeldAppendAt + core.StallTicks*election + 2*TickTime
		if at < cfg.StallLeaderAt || at > limit {
			t.Fatalf("seed %d: the stalled leader %s at %d ms, want between the stall at %d ms and %d ms, %d election timeouts and two ticks after its oldest held append at %d ms",
				seed, what, at/Millisecond, cfg.StallLeaderAt/Millisecond, limit/Millisecond, core.StallTicks, res.HeldAppendAt/Millisecond)
		}
		checkers(t, res)
		t.Logf("seed %d: the stalled leader %s at %d ms, deadline %d ms after its oldest held append at %d ms", seed, what, at/Millisecond, limit/Millisecond, res.HeldAppendAt/Millisecond)
		return struct{}{}
	}, nil)
}

// TestAllLinkMasks holds each of the 64 sets of directed links between the
// three nodes for 3 virtual seconds, from 2 s on, with no other fault,
// then heals. Every rule and checker must hold, and the cluster must serve
// again 2 s after healing.
func TestAllLinkMasks(t *testing.T) {
	simrun.Each(t, seedRange(1000, 64), func(t *testing.T, seed uint64) struct{} {
		mask := seed - 1000
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{HoldMask: mask, HoldMaskAt: 2 * Second}
		cfg.Duration = 5 * Second
		cfg.Heal = 8 * Second
		cfg.Clients = 4
		cfg.NoLeases = true
		res := run(t, cfg)
		// The mask went in at 2 s and, with no other link fault, held
		// until healing at 5 s.
		if res.Stats.LinkChanges != 1 {
			t.Fatalf("mask %06b: %d link changes, want the one", mask, res.Stats.LinkChanges)
		}
		checkers(t, res)
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := agree(h); err != nil {
			t.Fatalf("mask %06b: %v", mask, err)
		}
		return struct{}{}
	}, nil)
}

// TestSwitchingLinks switches to a random set of directed links about
// every 300 ms for 10 virtual seconds, then heals. Two of the three seeds
// at least must switch 10 times.
func TestSwitchingLinks(t *testing.T) {
	var took effects
	if !simrun.Each(t, []uint64{2001, 2002, 2003}, func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{SwitchEvery: 300 * Millisecond}
		cfg.Duration = 10 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.LinkChanges
	}, func(seed uint64, changes int) bool {
		took.add(seed, map[string]bool{"ten switches": changes >= 10})
		return false
	}) {
		return
	}
	took.check(t)
}

// faultCase is a fault alone, with the count that shows it took effect.
type faultCase struct {
	name   string
	faults Faults
	// cfg, if set, changes the run's configuration further.
	cfg   func(*Config)
	count func(Stats) int
}

// faultCases are the cases of TestEachFaultTakesEffect.
func faultCases() []faultCase {
	crashes := Faults{CrashEvery: Second, MaxDown: Second}
	// Crashes while directory operations are not synced yet: inside
	// recovery, which removes and renames files, and at a checkpoint or
	// removal step.
	unsynced := Faults{CrashEvery: 500 * Millisecond, MaxDown: 500 * Millisecond, RecoveryCrash: true, StepCrash: true}
	// The damage needs sealed segments, which take a few seconds of
	// writes.
	long := func(c *Config) { c.Duration = 16 * Second }
	longer := func(c *Config) { c.Duration = 24 * Second }
	// A checkpoint, which a snapshot sends and which cuts the log
	// behind it, every DefaultConfig's disk budget of log (startWL):
	// without one, a follower held down catches up from the log, and
	// nothing names a value before the log's start.
	ckpts := func(c *Config) { c.Faults.DiskBudget = DefaultConfig(c.Seed).Faults.DiskBudget }
	damage := func(k damageKind, count func(Stats) int) faultCase {
		return faultCase{k.String(), Faults{CrashEvery: Second, MaxDown: Second, Corrupt: true}, func(c *Config) {
			c.damageKinds = []damageKind{k}
			// Damage goes in at a crash, and the crashes stop at healing,
			// Duration in: a segment it may damage needs a log of four
			// segments by then, which a wl cluster under a crash a
			// second may not have written in long()'s 16 s (seeds 3241,
			// 3242, 3261 and 3262 on 5d361913, whose logs stalled in
			// their first segments). So they crash for 24 s, and run 16 s
			// healed, 40 s in all; coverage on 7d41b46b passed with it.
			c.Duration, c.Heal = 24*Second, 16*Second
			if k == flippedRoot {
				// The other kinds need a log of several segments, which
				// a checkpoint would cut.
				ckpts(c)
			}
		}, count}
	}
	cases := []faultCase{
		{"link stall", Faults{LinkStallEvery: Second}, nil, func(st Stats) int { return st.LinkStalls }},
		{"pause", Faults{PauseEvery: Second}, nil, func(st Stats) int { return st.Pauses }},
		{"half-open link", Faults{HalfOpenEvery: Second}, nil, func(st Stats) int { return st.HalfOpens }},
		{"link delays", Faults{SwitchEvery: 200 * Millisecond, MaxDelay: 5 * Millisecond}, nil, func(st Stats) int { return st.Asymmetric }},
		{"gray node", Faults{GrayEvery: Second}, nil, func(st Stats) int { return st.GrayNodes }},
		{"gray client", Faults{GrayClient: true}, nil, func(st Stats) int { return st.GrayStreams }},
		{"crash in recovery", Faults{CrashEvery: 2 * Second, MaxDown: Second, RecoveryCrash: true}, nil, func(st Stats) int { return st.RecoveryCrashes }},
		{"follower held down", Faults{HoldDown: true}, ckpts, func(st Stats) int { return st.Snapshots }},
		{"damaged snapshot", Faults{}, func(c *Config) { *c = wlDamagedSnapshotConfig(c.Seed) }, func(st Stats) int { return st.SnapshotFlips }},
		{"crash during a fetch", Faults{HoldDown: true, FetchCrash: true}, ckpts, func(st Stats) int { return st.FetchCrashes }},
		{"crash at a checkpoint step", Faults{StepCrash: true}, nil, func(st Stats) int { return st.StepCrashes }},
		{"rotten value", Faults{Rot: true}, ckpts, func(st Stats) int { return st.Rots }},
		{"kill", crashes, nil, func(st Stats) int { return st.Kills }},
		// One kill in three, of half the crashes, has a power loss after
		// it: 24 s make about 12 kills, and a seed without a loss rare
		// (seed 3131: 5 kills in 16 s, none followed by one).
		{"power loss after a kill", crashes, longer, func(st Stats) int { return st.KillLosses }},
		{"lost directory operation", unsynced, long, func(st Stats) int { return st.Disk.LostDirOps }},
		// One disk fault in four is a failed sync: twice the rate makes a
		// seed without one rare.
		{"failed sync", Faults{DiskErrorEvery: 500 * Millisecond}, nil, func(st Stats) int { return st.Disk.FailedSyncs }},
		{"EIO", Faults{DiskErrorEvery: Second}, nil, func(st Stats) int { return st.IOErrors }},
		{"file system down", Faults{FSDownEvery: Second}, nil, func(st Stats) int { return min(st.FSDowns, st.DiskFaults) }},
		{"read-only file system", Faults{FSDownEvery: Second}, nil, func(st Stats) int { return st.ReadOnlyStops }},
		{"client link down", Faults{LinkEvery: Second}, nil, func(st Stats) int { return st.ClientLinksDown }},
		// A small budget checkpoints often (Config.CheckpointLog): a
		// node then needs room while its disk is full, which its log's
		// ring, made once, does not. It stops on the write, or holds the
		// checkpoint with NOSPACE raised and stays up (#769).
		{"full disk", Faults{DiskFullEvery: Second, DiskBudget: 32 << 10}, nil, func(st Stats) int { return st.OutOfSpace + st.NoRoom }},
		{"the log cut within the budget", Faults{DiskBudget: 32 << 10, DiskCapacity: 128 << 10}, nil, func(st Stats) int { return st.LogCuts }},
		damage(flippedRoot, func(st Stats) int { return min(st.Damage[flippedRoot], st.Fenced) }),
		damage(misdirected, func(st Stats) int { return min(st.Damage[misdirected], st.Fenced) }),
		damage(lostWrite, func(st Stats) int { return min(st.Damage[lostWrite], st.Fenced) }),
		damage(badSector, func(st Stats) int { return min(st.Damage[badSector], st.Fenced, st.Disk.BadReads) }),
		damage(misread, func(st Stats) int { return min(st.Damage[misread], st.MisreadRefusals) }),
		{"connection reset", Faults{ResetEvery: Second}, nil, func(st Stats) int { return st.Resets }},
		{"full send buffer", Faults{LinkStallEvery: 500 * Millisecond}, nil, func(st Stats) int { return st.BlockedWrites }},
		{"slow watcher", Faults{SlowWatcherEvery: Second}, nil, func(st Stats) int { return st.SlowWatchers }},
		// A read that must not wait on the disk reads a value of the log or
		// a checkpoint: a watch that catches up, or a range at a past
		// revision.
		{"value out of the page cache", Faults{Uncached: true, SlowWatcherEvery: Second}, ckpts, func(st Stats) int { return st.Uncached }},
	}
	for i := range numNetem {
		k := netemKind(i)
		cases = append(cases, faultCase{
			k.String(),
			Faults{NetemEvery: 300 * Millisecond},
			func(c *Config) { c.netemKinds = []netemKind{k} }, func(st Stats) int { return st.Netem[k] },
		})
	}
	return cases
}

// TestEachFaultTakesEffect runs each fault alone for 8 virtual seconds,
// on three seeds, and requires that it happened in two of them at least.
// The seeds of case i are 3000+10i to 3002+10i, each a gosim run of its
// own. A follower held down for up to 4 s needs about a second after its
// restart before its first fetch, which the stream faults damage only
// before healing.
func TestEachFaultTakesEffect(t *testing.T) {
	if simrun.Race {
		// Under the race detector the cases take half an hour.
		t.Skip("the faults' effects are counted under gosim")
	}
	cases := faultCases()
	var seeds []uint64
	for i := range cases {
		seeds = append(seeds, seedRange(3000+10*uint64(i), 3)...)
	}
	took := make([]effects, len(cases))
	if !simrun.Each(t, seeds, func(t *testing.T, seed uint64) bool {
		c := cases[(seed-3000)/10]
		cfg := DefaultConfig(seed)
		cfg.Faults = c.faults
		cfg.Duration = 8 * Second
		cfg.Heal = 8 * Second
		if c.cfg != nil {
			c.cfg(&cfg)
		}
		res := run(t, cfg)
		checkers(t, res)
		t.Logf("%s: %+v", c.name, res.Stats)
		return c.count(res.Stats) > 0
	}, func(seed uint64, effect bool) bool {
		i := (seed - 3000) / 10
		took[i].add(seed, map[string]bool{cases[i].name: effect})
		return false
	}) {
		return
	}
	for i := range cases {
		took[i].check(t)
	}
}

// TestPowerLossAfterKill makes every crash a kill, which keeps the page
// cache, followed by a power loss 0.1 to 3 s later, while the node is
// down or after it started again. What the restarted node read and held
// durable must survive the power loss: before #361's fix, recovery did
// not sync what it read from the page cache.
func TestPowerLossAfterKill(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3600, 16), func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{CrashEvery: Second, MaxDown: Second}
		cfg.killLosses = true
		cfg.Duration = 10 * Second
		cfg.Heal = 8 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.KillLosses
	}, func(seed uint64, losses int) bool {
		took.add(seed, map[string]bool{"power loss after a kill": losses > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// TestFailedSyncThenRestart makes every disk fault a failed sync, after
// which the node starts again with its page cache kept, where the bytes
// the sync did not write would stay readable, and no later sync writes
// them. The failed sync's process must cut them off: with segments of 256
// bytes, which end every few writes, a power loss later would find a hole
// in a sealed segment (#364).
func TestFailedSyncThenRestart(t *testing.T) {
	var took effects
	if !simrun.Each(t, seedRange(3800, 8), func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{DiskErrorEvery: 500 * Millisecond, CrashEvery: 2 * Second, MaxDown: Second}
		cfg.syncFaultExits = true
		cfg.SegmentBytes = 256
		cfg.Duration = 10 * Second
		cfg.Heal = 8 * Second
		res := run(t, cfg)
		checkers(t, res)
		return res.Stats.Disk.FailedSyncs
	}, func(seed uint64, n int) bool {
		took.add(seed, map[string]bool{"failed sync": n > 0})
		return false
	}) {
		return
	}
	took.check(t)
}

// damagedSnapshotConfig holds a follower down until it fetches a snapshot,
// and damages the first stream, for 8 virtual seconds and 8 of healing:
// the case "damaged snapshot" of TestEachFaultTakesEffect, and B22's.
func damagedSnapshotConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{HoldDown: true, FlipSnapshot: true}
	cfg.Duration = 8 * Second
	cfg.Heal = 8 * Second
	return cfg
}

// TestHalfOpenLink makes the link from the leader to a follower half-open
// at 5 s, with no other fault, until the peer write timeout fails it 5 s
// later. The leader and the other follower are a quorum, so every call to
// them must succeed and take under a second; the cut-off follower must
// serve again 2 s after the connection fails. A call that a node answers
// with "server stopped" as it starts again from its log after a snapshot
// install is left out, as the liveness rule does: the simulator's window
// of a few entries (tinyLimits) makes a follower that falls a few entries
// behind install a snapshot, which under 12 clients happens often (#266).
func TestHalfOpenLink(t *testing.T) {
	simrun.Each(t, []uint64{4001, 4002, 4003}, func(t *testing.T, seed uint64) struct{} {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		// Calls to the cut-off follower wait up to the client timeout for a
		// leader (#105). With clients that pick a node per call, all of
		// them could wait there at once and leave the healthy pair idle
		// (#279), so four stay on each node.
		cfg.Clients, cfg.PinClients = 12, true
		cfg.HalfOpenAt = 5 * Second
		cfg.LiveFrom = cfg.HalfOpenAt + halfOpenTimeout + 2*Second
		cfg.Duration = 15 * Second
		res := run(t, cfg)
		if len(res.halfOpens) != 1 {
			t.Fatalf("seed %d: %d half-open links, want 1", seed, len(res.halfOpens))
		}
		h := res.halfOpens[0]
		healthy := 0
		for _, r := range res.History {
			if r.Call < h.start || r.Call >= h.end || r.Node == h.to || errors.Is(r.Err, rpctypes.ErrGRPCStopped) {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed during the half-open link: %v", seed, r.Node+1, r.Call/Millisecond, r.Err)
			}
			if r.Return-r.Call >= Second {
				t.Fatalf("seed %d: call to node %d at %d ms took %d ms", seed, r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
			}
			healthy++
		}
		if healthy < 20 {
			t.Fatalf("seed %d: only %d calls to the healthy pair during the half-open link", seed, healthy)
		}
		return struct{}{}
	}, nil)
}

// TestDiskFaultAfterHealing cuts every link before the first election, so
// no node appends during the faults: the guaranteed disk fault must fire
// after healing, and the liveness rule must still hold.
func TestDiskFaultAfterHealing(t *testing.T) {
	if simrun.Drive(t, 6001) {
		return
	}
	cfg := DefaultConfig(6001)
	cfg.Faults = Faults{DiskErrorEvery: 1000 * Second, HoldMask: 0, HoldMaskAt: 1}
	cfg.Duration = 5 * Second
	cfg.Heal = 8 * Second
	res := run(t, cfg)
	if res.Stats.DiskFaults != 1 {
		t.Fatalf("%d disk faults, want the guaranteed one", res.Stats.DiskFaults)
	}
	checkers(t, res)
}

// TestGrayClient runs only the gray client for 5 s: it reconnects every
// 50 to 300 ms and opens 10 watches each time, and never reads. Every
// call of the other clients during that time must succeed in under a
// second.
func TestGrayClient(t *testing.T) {
	if simrun.Drive(t, 7001) {
		return
	}
	cfg := DefaultConfig(7001)
	cfg.Faults = Faults{GrayClient: true}
	cfg.Duration = 5 * Second
	res := run(t, cfg)
	if res.Stats.GrayStreams < 10 {
		t.Fatalf("the gray client opened only %d streams", res.Stats.GrayStreams)
	}
	calls := 0
	for _, r := range res.History {
		if r.Call >= cfg.Duration || r.Call < Second {
			continue
		}
		if errors.Is(r.Err, rpctypes.ErrGRPCStopped) || res.nodes[r.Node].hungOnReload(r) {
			// With no other fault, only a start from the log after a
			// snapshot install, which the run's tiny window makes
			// frequent, answers that, or makes clientv3 try a read
			// again, which can time out (#365).
			continue
		}
		if r.Err != nil && !isRejection(r.Err) {
			t.Fatalf("call to node %d at %d ms failed beside the gray client: %v", r.Node+1, r.Call/Millisecond, r.Err)
		}
		// A node that keeps starting again from its log, as the tiny
		// window makes a follower do that falls behind, as under the race
		// detector, holds a call that it answers in the end (#381).
		if took := r.Return - r.Call; took >= Second && !during(res.nodes[r.Node].fromLog, r.Call, r.Return) {
			t.Fatalf("call to node %d at %d ms took %d ms beside the gray client", r.Node+1, r.Call/Millisecond, took/Millisecond)
		}
		calls++
	}
	if calls < 100 {
		t.Fatalf("only %d calls beside the gray client", calls)
	}
}

// TestEndWaitsForCatchUp is #269's case: a follower, held back from the
// others until the end, misses what committed while it was down, so the
// end finds it behind them (LagAtEnd). The run must let it catch up,
// then pass every check, as strict as ever. Every seed must end with it
// behind (#745).
func TestEndWaitsForCatchUp(t *testing.T) {
	lagged := 0
	if !simrun.Each(t, []uint64{7001, 7002, 7003, 7004}, func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.LagAtEnd = true
		res := run(t, cfg)
		checkers(t, res)
		t.Logf("seed %d: the nodes ran %d s more after the end", seed, res.Stats.Settles)
		return res.Stats.Settles
	}, func(_ uint64, settles int) bool {
		if settles > 0 {
			lagged++
		}
		return false
	}) {
		return
	}
	if !simrun.One() && !t.Failed() && lagged < 4 {
		t.Fatalf("%d of 4 seeds ended with a node behind; every seed must", lagged)
	}
}

// TestSaveRunKeepsDisks checks that a saved run has each node's disk
// calls and files, and the files of a crashed node right after its crash
// (#274).
func TestSaveRunKeepsDisks(t *testing.T) {
	if simrun.Drive(t, 11) {
		return
	}
	cfg := DefaultConfig(11)
	cfg.Duration = 6 * Second
	res := run(t, cfg)
	files := runFiles("out", "replay", res, true)
	calls := files[filepath.Join("disks", "node1", "calls.txt")]
	if !strings.Contains(calls, " ms sync wal/") || !strings.Contains(calls, " ms write wal/") {
		t.Fatalf("node 1's disk calls: %.200q", calls)
	}
	saved := func(dir string) bool {
		for path := range files {
			if strings.HasPrefix(path, dir+string(filepath.Separator)) {
				return true
			}
		}
		return false
	}
	if !saved(filepath.Join("disks", "node1", "now")) {
		t.Fatal("node 1's segments are not saved")
	}
	crashed := 0
	for _, n := range res.nodes {
		if n.crashImage != nil {
			crashed++
			if !saved(filepath.Join("disks", fmt.Sprintf("node%d", n.id), "after-crash")) {
				t.Fatalf("node %d crashed, but its files after the crash are not saved", n.id)
			}
		}
	}
	if crashed == 0 {
		t.Fatal("no node crashed")
	}
	// The bounds the budget judges the history by hold.
	callsMax, historyMax := res.saveBounds()
	if _, ok := files["history.json"]; !ok {
		t.Fatal("the run's history.json is not saved")
	}
	if c, h := int64(len(files["calls.txt"])), int64(len(files["history.json"])); c == 0 || c > callsMax || h > historyMax {
		t.Fatalf("calls.txt has %d bytes and history.json %d, past their bounds of %d and %d", c, h, callsMax, historyMax)
	}
	disks := func(files map[string]string) (in, out []string) {
		for _, n := range res.nodes {
			for _, dir := range []string{"now", "after-crash"} {
				if dir == "after-crash" && n.crashImage == nil {
					continue
				}
				d := filepath.Join("disks", fmt.Sprintf("node%d", n.id), dir)
				saved := false
				for path := range files {
					saved = saved || strings.HasPrefix(path, d+string(filepath.Separator))
				}
				if saved {
					in = append(in, d)
				} else {
					out = append(out, d)
				}
			}
		}
		return in, out
	}
	// Within a budget too small for any large file (#775), the history,
	// the calls and every disk are left out, and replay.txt names each;
	// the small files stay.
	files = runFilesWithin("out", "replay", res, true, &saveRoom{left: 1})
	for _, name := range []string{"history.json", "calls.txt"} {
		if _, ok := files[name]; ok {
			t.Errorf("%s saved past the budget", name)
		}
	}
	for _, name := range []string{"trace.txt", "nodes.txt", "hashes.txt", filepath.Join("disks", "node1", "calls.txt")} {
		if files[name] == "" {
			t.Errorf("no %s within the budget", name)
		}
	}
	in, out := disks(files)
	if len(in) > 0 {
		t.Errorf("disks %v saved past the budget", in)
	}
	for _, name := range append([]string{"history.json", "calls.txt"}, out...) {
		if !strings.Contains(files["replay.txt"], name+" (") {
			t.Errorf("replay.txt does not name %s, left out: %q", name, files["replay.txt"])
		}
	}
	// Within room for the history, the calls and node 1's disk now, those
	// are saved and the other disks left out.
	files = runFilesWithin("out", "replay", res, true, &saveRoom{left: callsMax + historyMax + int64(res.nodes[0].disk.Size())})
	if files["history.json"] == "" || files["calls.txt"] == "" {
		t.Error("the history or the calls left out within room for both")
	}
	in, out = disks(files)
	if !slices.Equal(in, []string{filepath.Join("disks", "node1", "now")}) || len(out) == 0 {
		t.Errorf("within room for node 1's disk now, saved %v and left out %v", in, out)
	}
	for _, name := range out {
		if !strings.Contains(files["replay.txt"], name+" (") {
			t.Errorf("replay.txt does not name %s, left out: %q", name, files["replay.txt"])
		}
	}
}

// TestFiveVoters runs DefaultConfig's faults on a cluster of five voters,
// every checker on it: a cluster of five may lose two voters for good
// and keep its quorum.
func TestFiveVoters(t *testing.T) {
	sizeSeed(t, 1, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Nodes = 5
		return cfg
	})
}

// TestOneVoter runs DefaultConfig's faults on a cluster of one voter,
// every checker on it: it keeps its only disk whole, and has no links
// between nodes to fault.
func TestOneVoter(t *testing.T) {
	sizeSeed(t, 2, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Nodes = 1
		return cfg
	})
}

// TestOneVoterMembership runs the membership work on a cluster of one
// voter, every checker on it: the member it adds is the cluster's first
// link between nodes.
func TestOneVoterMembership(t *testing.T) {
	sizeSeed(t, 4, func(seed uint64) Config {
		cfg := wlMembershipConfig(seed)
		cfg.Nodes = 1
		return cfg
	})
}

// sizeSeed runs the config of seed with every checker, and requires it to
// have done enough to test anything.
func sizeSeed(t *testing.T, seed uint64, config func(uint64) Config) {
	simrun.Each(t, []uint64{seed}, func(t *testing.T, seed uint64) Stats {
		cfg := config(seed)
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		if st.OK < 50 || len(res.Log) < 20 {
			t.Fatalf("seed %d, %d voters, did too little to test anything: %+v, %d committed writes", seed, cfg.Nodes, st, len(res.Log))
		}
		t.Logf("seed %d, %d voters: %d ok, %d failed, %d ambiguous, %d fenced, %d cold restarts, membership changes %v",
			seed, cfg.Nodes, st.OK, st.Failed, st.Ambiguous, st.Fenced, st.ColdRestarts, st.MembershipChanges)
		return st
	}, nil)
}
