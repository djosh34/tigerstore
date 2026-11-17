package wl

import (
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// The invariant of reads held across checkpoints (ckpt.go, above
// scanKeys), checked against a model: the store's state at each revision
// as a map of key to value, and the puts each revision made, a put its own
// Txn deleted included. Each case is a seeded run of random puts, deletes,
// Txns of several keys or that put and delete one key, compactions and
// partial trims, records let go, holds of the three kinds taken and
// released (a large range, a large watch, a reply waiting for the
// reserve, through holdTxn and endHeld), and checkpoints, in the middle of
// which writes and holds come, each followed by the log's cut and a load.
// After every step every hold reads what it read when it was taken, and
// reads at the compacted and the current revision answer as the model
// does. $WL_MODEL_SEEDS runs more cases, $WL_MODEL_SEED one.
func TestHeldReadsSurviveCheckpointsAsAModelSays(t *testing.T) {
	seeds := []int64{}
	if s := os.Getenv("WL_MODEL_SEED"); s != "" {
		seed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, seed)
	} else {
		count := 300
		if s := os.Getenv("WL_MODEL_SEEDS"); s != "" {
			var err error
			if count, err = strconv.Atoi(s); err != nil {
				t.Fatal(err)
			}
		}
		for i := range count {
			seeds = append(seeds, int64(i+1))
		}
	}
	for _, seed := range seeds {
		if failed := runHeldModel(t, seed); failed != "" {
			t.Fatalf("seed %d (WL_MODEL_SEED=%d): %s", seed, seed, failed)
		}
	}
}

// heldModelKeys are the keys a case writes.
var heldModelKeys = []string{"/a", "/b", "/c", "/d", "/e"}

// modelHold is a hold a case took, and what its holder read when it took
// it: name is what each read is, want its answer.
type modelHold struct {
	kind  string // range, watch or reply
	rev   int64
	hr    *heldReply
	reads []modelRead
}

type modelRead struct {
	key    string
	rev    int64
	put    bool // the put at rev (putAt), not the state a read at rev sees
	want   string
	record string // complete record captured when held: metadata must remain identical too
}

type heldModel struct {
	t          *testing.T
	rng        *rand.Rand
	n          *node
	l          *memLog
	c          *client
	sid        uint32
	states     map[int64]map[string]string // the state after each revision
	puts       map[int64]map[string]string // the puts each revision made
	holds      []*modelHold
	failed     string
	cut        int64
	index      uint64
	inCkpt     bool
	installing bool
}

func runHeldModel(t *testing.T, seed int64) (failed string) {
	n, l := ckptNode(t)
	st := n.st
	defer func() { _ = n.st.close() }()
	takeEveryValue(t)
	// A cut short of the checkpoint before's place too, which leaves
	// values in the log for the next checkpoint to take in, as the loop's
	// checkpoints do (the invariant re-check, Opus).
	knobs := rand.New(rand.NewSource(seed + 1<<32))
	ckptCutShare = []int64{1, 4, 16, 64}[knobs.Intn(4)]
	ckptCutUnit = []int64{1, 8, 64}[knobs.Intn(3)]
	oldScan := ckptScanStates
	ckptScanStates = 1 // force holds and writes between keys, not just before/after a whole scan
	defer func() { ckptScanStates = oldScan }()
	n.ckptKeys = 1 // force several encoding turns too
	n.sizes, n.connOut = sizeFor(256<<20), 64<<10
	c, _ := flowClient(t)
	c.node = n
	m := &heldModel{
		t: t, rng: rand.New(rand.NewSource(seed)), n: n, l: l, c: c, sid: 3,
		states: map[int64]map[string]string{st.rev: {}}, puts: map[int64]map[string]string{}, index: 10,
	}
	for step := 0; step < 120 && m.failed == ""; step++ {
		m.step(step, true)
	}
	if m.failed == "" {
		// Reach every remaining compaction boundary, and consecutive replacements
		// with no intervening writes, not only whatever the random walk selected.
		for rev := m.low(); rev <= n.st.rev && m.failed == ""; rev++ {
			n.st.compact(rev)
			n.st.trimSteps(1 << 20)
			m.check(120)
		}
		m.installing = true // suppress interleaved writes, not the existing holds
		m.checkpoint(121)
		if m.failed == "" {
			m.checkpoint(122)
		}
		m.installing = false
	}
	if n.ckpt != nil {
		n.abortCkpt() // a failure in its middle stopped it
	}
	return m.failed
}

// fail records the case's first failure; the case stops at it.
func (m *heldModel) fail(format string, args ...any) {
	if m.failed == "" {
		m.failed = fmt.Sprintf(format, args...)
	}
}

func (m *heldModel) id(key string) uint32 {
	id, err := m.n.st.intern([]byte(key))
	if err != nil {
		m.fail("intern %s: %v", key, err)
		return 0
	}
	return id
}

// read is what read r gives now.
func (m *heldModel) read(r modelRead) (string, error) {
	rec, err := m.readRecord(r)
	return string(recValue(rec)), err
}

func (m *heldModel) readRecord(r modelRead) ([]byte, error) {
	st := m.n.st
	id, ok := st.find([]byte(r.key))
	if !ok {
		return nil, nil
	}
	var rec []byte
	var err error
	if r.put {
		rec, err = st.putAt(id, r.rev)
	} else {
		rec, err = st.at(id, r.rev)
	}
	return rec, err
}

// write applies one revision of puts and deletes, in order, to the store
// and the model.
func (m *heldModel) write(ops []struct {
	key string
	del bool
},
) {
	st := m.n.st
	prev := m.states[st.rev]
	st.rev++
	rev := st.rev
	next := maps.Clone(prev)
	m.puts[rev] = map[string]string{}
	for _, op := range ops {
		id := m.id(op.key)
		if op.del {
			if _, _, err := st.del(id, rev, false); err != nil {
				m.fail("delete %s at %d: %v", op.key, rev, err)
				return
			}
			delete(next, op.key)
			continue
		}
		v := fmt.Sprintf("%s@%d", op.key, rev)
		if _, err := st.put(id, []byte(v), 0, rev, m.l.add(v), false); err != nil {
			m.fail("put %s at %d: %v", op.key, rev, err)
			return
		}
		next[op.key] = v
		m.puts[rev][op.key] = v
	}
	st.endApply()
	m.states[rev] = next
}

func (m *heldModel) key() string { return heldModelKeys[m.rng.Intn(len(heldModelKeys))] }

// low is the least revision a read may ask for: the compacted one.
func (m *heldModel) low() int64 { return max(m.n.st.compactRev, 1) }

func (m *heldModel) step(step int, mayCkpt bool) {
	if m.failed != "" {
		return
	}
	st := m.n.st
	type op = struct {
		key string
		del bool
	}
	switch x := m.rng.Intn(100); {
	case x < 8:
		m.nested()
	case x < 25:
		m.write([]op{{key: m.key()}})
	case x < 33:
		m.write([]op{{key: m.key(), del: true}})
	case x < 41:
		a, b := m.key(), m.key()
		ops := []op{{key: a}}
		if b != a {
			ops = append(ops, op{key: b, del: m.rng.Intn(2) == 0})
		}
		m.write(ops)
	case x < 49:
		// A Txn that puts a key and deletes it, whose reply, the delete's
		// prev_kv, may wait for the reserve.
		k := m.key()
		m.write([]op{{key: k}, {key: k, del: true}})
		if m.rng.Intn(2) == 0 {
			m.holdPrevKV(k)
		}
	case x < 55:
		lo := m.low()
		st.compact(lo + m.rng.Int63n(st.rev-lo+1))
	case x < 73:
		m.hold()
	case x < 84:
		if len(m.holds) > 0 {
			i := m.rng.Intn(len(m.holds))
			m.release(m.holds[i])
			m.holds = slices.Delete(m.holds, i, i+1)
		}
	case x < 92:
		if mayCkpt && !m.inCkpt {
			m.checkpoint(step)
		}
	case x < 95:
		if mayCkpt && !m.inCkpt {
			m.install(step)
			st = m.n.st
		}
	default:
		dropRecords(st)
	}
	if m.rng.Intn(4) == 0 {
		st.trimSteps(1 << 20)
	} else {
		st.trimSteps(m.rng.Intn(3))
	}
	m.check(step)
}

// hold takes a hold of a random kind, and records what its holder reads.
func (m *heldModel) hold() {
	st := m.n.st
	lo := m.low()
	r := lo + m.rng.Int63n(st.rev-lo+1)
	h := &modelHold{rev: r}
	switch m.rng.Intn(3) {
	case 0:
		h.kind = "range"
		for _, k := range heldModelKeys {
			h.reads = append(h.reads, modelRead{key: k, rev: r, want: m.states[r][k]})
		}
		if m.taken(h) {
			st.holdRead(r)
		} else {
			return
		}
	case 1:
		if r < 2 {
			return
		}
		h.kind = "watch"
		// Its events: the puts at r, a put its Txn deleted too, and the
		// value before each change at r.
		for k, v := range m.puts[r] {
			read := modelRead{key: k, rev: r, put: true, want: v}
			if got, err := m.read(read); err == nil && got == "" && r <= st.compactRev {
				continue // trimmed at the compacted revision: etcd sends the deletion alone
			}
			h.reads = append(h.reads, read)
		}
		for _, k := range heldModelKeys {
			if m.states[r][k] == m.states[r-1][k] && m.puts[r][k] == "" {
				continue
			}
			read := modelRead{key: k, rev: r - 1, want: m.states[r-1][k]}
			if got, err := m.read(read); err == nil && got != read.want && r-1 < st.compactRev {
				return // its value before is compacted: the watch is canceled
			}
			h.reads = append(h.reads, read)
		}
		if !m.taken(h) {
			return
		}
		st.holdRead(r - 1)
		st.holdRead(r)
	default:
		// A Txn's range at r, its reply waiting for the reserve.
		h.kind = "reply"
		var kvs [][]byte
		for _, k := range heldModelKeys {
			if m.states[r][k] == "" || m.rng.Intn(2) == 0 {
				continue
			}
			id, _ := st.find([]byte(k))
			rec, err := st.at(id, r)
			if err != nil || rec == nil {
				m.fail("a reply's read of %s at %d: %q, %v; the model has %q", k, r, recValue(rec), err, m.states[r][k])
				return
			}
			kvs = append(kvs, rec)
			kv := parseKV(rec)
			h.reads = append(h.reads, modelRead{key: k, rev: kv.mod, put: true, want: m.states[r][k]})
		}
		if len(kvs) == 0 || !m.taken(h) {
			return
		}
		m.sid += 2
		before := len(m.n.heldReplies)
		m.n.holdTxn(m.c, m.c.newStream(m.sid, kTxn), true, []opRes{{kind: kRange, rev: r, kvs1: len(kvs), count: int64(len(kvs))}}, 0, 1, kvs)
		h.hr = m.n.heldReplies[before]
	}
	m.holds = append(m.holds, h)
}

// holdPrevKV holds the reply of the Txn just applied, which put key and
// deleted it with prev_kv: its record is that put, which the delete at
// the same revision superseded.
func (m *heldModel) holdPrevKV(key string) {
	st := m.n.st
	rev := st.rev
	id, _ := st.find([]byte(key))
	rec, err := st.putAt(id, rev)
	if err != nil || rec == nil {
		m.fail("the prev_kv of %s at %d: %q, %v", key, rev, recValue(rec), err)
		return
	}
	h := &modelHold{kind: "reply", rev: rev, reads: []modelRead{{key: key, rev: rev, put: true, want: m.puts[rev][key]}}}
	if !m.taken(h) {
		return
	}
	m.sid += 2
	before := len(m.n.heldReplies)
	m.n.holdTxn(m.c, m.c.newStream(m.sid, kTxn), true, []opRes{{kind: kDel, rev: rev, kvs1: 1, count: 1}}, 0, 1, [][]byte{rec})
	h.hr = m.n.heldReplies[before]
	m.holds = append(m.holds, h)
}

// taken reports whether h's reads give, as it is taken, what the model
// says.
func (m *heldModel) taken(h *modelHold) bool {
	for i, r := range h.reads {
		if got, err := m.read(r); err != nil || got != r.want {
			m.fail("a %s hold at %d, taken: %s at %d (put %v) reads %q, %v; want %q", h.kind, h.rev, r.key, r.rev, r.put, got, err, r.want)
			return false
		}
		rec, err := m.readRecord(r)
		if err != nil {
			m.fail("capture held record: %v", err)
			return false
		}
		h.reads[i].record = string(rec)
	}
	return true
}

func (m *heldModel) release(h *modelHold) {
	st := m.n.st
	switch h.kind {
	case "range":
		st.releaseRead(h.rev)
	case "watch":
		st.releaseRead(h.rev - 1)
		st.releaseRead(h.rev)
	default:
		m.n.endHeld(h.hr)
	}
}

// check checks every hold's reads, and the reads at the compacted and the
// current revision, against the model.
func (m *heldModel) check(step int) {
	m.checkIndexes(step)
	st := m.n.st
	// With no hold left, nothing holds on: the hold maps let go, and the
	// change index starts where trimming keeps (the invariant re-check,
	// Opus).
	if len(m.holds) == 0 && m.n.ckpt == nil {
		if st.heldStates != nil || st.readHolds != nil || st.trimHolds != nil {
			m.fail("step %d: no hold, but held states %v, read holds %v, trim holds %v", step, st.heldStates, st.readHolds, st.trimHolds)
			return
		}
		if len(st.changes) > 0 && st.changes[0].rev < st.trimRev() {
			m.fail("step %d: no hold, but the change index starts at %d, before %d", step, st.changes[0].rev, st.trimRev())
			return
		}
	}
	for _, h := range m.holds {
		for _, r := range h.reads {
			if rec, err := m.readRecord(r); err == nil && string(rec) != r.record && string(recValue(rec)) == r.want {
				m.fail("step %d: %s hold at %d: %s record metadata changed", step, h.kind, h.rev, r.key)
				return
			}
			if got, err := m.read(r); err != nil || got != r.want {
				m.fail("step %d: a %s hold at %d: %s at %d (put %v) reads %q, %v; want %q", step, h.kind, h.rev, r.key, r.rev, r.put, got, err, r.want)
				return
			}
		}
	}
	for rev := m.low(); rev <= st.rev; rev++ {
		if rev < 1 {
			continue
		}
		for _, k := range heldModelKeys {
			r := modelRead{key: k, rev: rev, want: m.states[rev][k]}
			if got, err := m.read(r); err != nil || got != r.want {
				m.fail("step %d: %s at %d reads %q, %v; want %q (compacted at %d)", step, k, rev, got, err, r.want, st.compactRev)
				return
			}
		}
	}
}

// checkpoint writes a checkpoint, with steps in the middle of it, cuts the
// log behind it, and loads it.
func (m *heldModel) checkpoint(step int) {
	n, st := m.n, m.n.st
	m.inCkpt = true
	defer func() { m.inCkpt = false }()
	m.index++
	index := m.index
	want, rev := st.stateHash(), st.rev
	// The checkpoint before's place is where its log began (ckptDone), as
	// in the loop: set to the log's end, no value was ever left in it.
	if err := n.startCkpt(index, 1, int64(len(m.l.b))); err != nil {
		m.fail("start checkpoint %d: %v", index, err)
		return
	}
	for n.ckpt != nil {
		if !m.installing {
			m.hold() // every scan/encoding boundary offers a new hold
			if m.rng.Intn(3) == 0 {
				m.step(step, false)
			}
		}
		if n.ckpt == nil || m.failed != "" {
			return
		}
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			m.fail("checkpoint %d: %v", index, err)
			return
		}
	}
	m.cut = max(m.cut, n.ckptCut)
	st.log = cutLog{m.l, m.cut}
	dropRecords(st) // do not let cached live records mask a missing relocation
	m.check(step)
	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(index)))
	if err != nil {
		m.fail("open checkpoint %d: %v", index, err)
		return
	}
	defer f.Close() //nolint:errcheck // read only
	loaded, err := newStore()
	if err != nil {
		m.fail("%v", err)
		return
	}
	defer func() { _ = loaded.close() }()
	loaded.log = st.log
	if _, err := loadCkpt(f, n.ckptGen, loaded); err != nil {
		m.fail("load checkpoint %d: %v", index, err)
		return
	}
	if loaded.stateHash() != want {
		m.fail("checkpoint %d at %d loads another state than its store's", index, rev)
		return
	}
	for r := max(loaded.compactRev, 1); r <= rev; r++ {
		for _, k := range heldModelKeys {
			var got string
			if id, ok := loaded.find([]byte(k)); ok {
				rec, err := loaded.at(id, r)
				if err != nil {
					m.fail("checkpoint %d: %s at %d: %v", index, k, rev, err)
					return
				}
				if rec != nil {
					got = string(recValue(rec))
				}
			}
			if got != m.states[r][k] {
				m.fail("checkpoint %d loaded: %s at %d reads %q, want %q", index, k, r, got, m.states[r][k])
				return
			}
		}
	}
}
