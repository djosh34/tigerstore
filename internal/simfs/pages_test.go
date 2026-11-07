package simfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simfs/internal/ref"
	"github.com/djosh34/tigerstore/internal/wal"
)

// pair is a simfs disk and the reference disk it must match, with an
// open handle on each of their files.
type pair struct {
	t          *testing.T
	disk       *simfs.Disk
	model      *ref.Disk
	trace      []string
	modelTrace []string
	handles    map[string][2]wal.File
	// kept are copies taken earlier, which no call changes since: each
	// must keep matching the model's copy while the disks they came from
	// change.
	kept [][2]any
}

func newPair(t *testing.T) *pair {
	p := &pair{t: t, disk: simfs.New(), model: ref.New()}
	p.setup()
	return p
}

// memStore is a Store in memory that counts what it holds and fails
// every call once fail is set.
type memStore struct {
	pages map[int64][]byte
	fail  error
}

func (m *memStore) Put(_ any, p []byte) (int64, error) {
	if m.fail != nil {
		return 0, m.fail
	}
	if m.pages == nil {
		m.pages = map[int64][]byte{}
	}
	at := int64(len(m.pages)) * simfs.PageBytes
	m.pages[at] = slices.Clone(p)
	return at, nil
}

func (m *memStore) Get(off int64, p []byte) error {
	if m.fail != nil {
		return m.fail
	}
	b, ok := m.pages[off]
	if !ok {
		return fmt.Errorf("no page at %d", off)
	}
	copy(p, b)
	return nil
}

// setup traces both disks, sets their capacity and opens a handle on each
// file.
func (p *pair) setup() {
	p.disk.Trace = func(format string, args ...any) { p.trace = append(p.trace, fmt.Sprintf(format, args...)) }
	p.model.Trace = func(format string, args ...any) { p.modelTrace = append(p.modelTrace, fmt.Sprintf(format, args...)) }
	p.disk.Capacity, p.model.Capacity = 1<<20, 1<<20
	p.handles = map[string][2]wal.File{}
	names, err := p.disk.List()
	if err != nil {
		p.t.Fatal(err)
	}
	for _, name := range names {
		a, errA := p.disk.Open(name)
		b, errB := p.model.Open(name)
		if errA != nil || errB != nil {
			p.t.Fatalf("open %s: %v, the model: %v", name, errA, errB)
		}
		p.handles[name] = [2]wal.File{a, b}
	}
}

// rngs returns two random sources that draw the same numbers.
func rngs(rng *rand.Rand) (*rand.Rand, *rand.Rand) {
	a, b := rng.Uint64(), rng.Uint64()
	return rand.New(rand.NewPCG(a, b)), rand.New(rand.NewPCG(a, b))
}

// same fails unless the disk and the model agree on everything they show.
func (p *pair) same(step int, what string) {
	p.t.Helper()
	fail := func(format string, args ...any) {
		p.t.Helper()
		p.t.Fatalf("step %d (%s): "+format, append([]any{step, what}, args...)...)
	}
	if err := sameFiles(p.disk, p.model); err != "" {
		fail("%s", err)
	}
	for i, k := range p.kept {
		if err := sameFiles(k[0].(*simfs.Disk), k[1].(*ref.Disk)); err != "" {
			fail("copy %d: %s", i, err)
		}
	}
	for _, name := range p.model.DurableNames() {
		if a, b := p.disk.Unsynced(name), p.model.Unsynced(name); a != b {
			fail("%s unsynced %v, the model %v", name, a, b)
		}
	}
	if a, b := p.disk.Bytes(), p.model.Bytes(); a != b {
		fail("%d bytes, the model %d", a, b)
	}
	if a, b := fmt.Sprint(p.disk.Counts), fmt.Sprint(p.model.Counts); a != b {
		fail("counts %s, the model's %s", a, b)
	}
	if !slices.Equal(p.trace, p.modelTrace) {
		fail("trace %q, the model's %q", p.trace, p.modelTrace)
	}
}

// sameFiles says how disk's files, synced and not, differ from the
// model's, or "" if they do not.
func sameFiles(disk *simfs.Disk, model *ref.Disk) string {
	diff := func(what string, a, b []byte) string {
		if bytes.Equal(a, b) {
			return ""
		}
		at := 0
		for at < min(len(a), len(b)) && a[at] == b[at] {
			at++
		}
		return fmt.Sprintf("%s holds %d bytes, the model's %d, and differs from byte %d", what, len(a), len(b), at)
	}
	a, b := disk.Files(), model.Files()
	if !slices.Equal(slices.Sorted(maps.Keys(a)), slices.Sorted(maps.Keys(b))) {
		return fmt.Sprintf("the files are %v, the model's %v", slices.Sorted(maps.Keys(a)), slices.Sorted(maps.Keys(b)))
	}
	for _, name := range slices.Sorted(maps.Keys(b)) {
		if d := diff("file "+name, a[name], b[name]); d != "" {
			return d
		}
	}
	if a, b := disk.DurableNames(), model.DurableNames(); !slices.Equal(a, b) {
		return fmt.Sprintf("durable names %v, the model's %v", a, b)
	}
	for _, name := range model.DurableNames() {
		if d := diff("the synced "+name, disk.DurableBytes(name), model.DurableBytes(name)); d != "" {
			return d
		}
	}
	return ""
}

// sentinels pairs each error a disk call can give with the model's: an
// error of simfs and one of the model are the same if they match the same
// pair, by errors.Is.
var sentinels = [][2]error{
	{simfs.ErrNoSpace, ref.ErrNoSpace},
	{simfs.ErrIO, ref.ErrIO},
	{simfs.ErrReadOnly, ref.ErrReadOnly},
	{simfs.ErrBadSector, ref.ErrBadSector},
	{fs.ErrExist, fs.ErrExist},
	{fs.ErrNotExist, fs.ErrNotExist},
	{io.EOF, io.EOF},
}

// sameErr says whether err of simfs and the model's are the same: both
// nil, or each matches the same sentinels and at least one.
func sameErr(err, model error) bool {
	if err == nil || model == nil {
		return err == nil && model == nil
	}
	matched := false
	for _, s := range sentinels {
		a, b := errors.Is(err, s[0]), errors.Is(model, s[1])
		if a != b {
			return false
		}
		matched = matched || a
	}
	return matched
}

// both runs a call on the disk and on the model and fails unless they
// give the same result and the same error.
func (p *pair) both(step int, what string, a, b func() (any, error)) {
	p.t.Helper()
	ra, errA := a()
	rb, errB := b()
	if fmt.Sprint(ra) != fmt.Sprint(rb) || !sameErr(errA, errB) {
		p.t.Fatalf("step %d (%s): %v, %v; the model %v, %v", step, what, ra, errA, rb, errB)
	}
}

// TestPagesMatchTheModel runs random calls on a disk whose files keep
// their bytes in shared pages and on simfs as it was before (package
// ref), with the same random draws, and checks after every call that
// both hold the same files, synced and not, report the same and trace
// the same.
func TestPagesMatchTheModel(t *testing.T) {
	seeds := uint64(40)
	if testing.Short() {
		seeds = 8
	}
	for seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) { pagesMatch(t, seed) })
	}
}

func pagesMatch(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 500))
	p := newPair(t)
	// Half the seeds keep the synced pages in a Store, behind a cache of
	// 2 pages, so that pages leave memory and are read back all the time.
	store := &memStore{}
	if seed%2 == 1 {
		p.disk.KeepIn(store, 2)
		defer func() {
			if len(store.pages) == 0 {
				t.Error("no page went to the store")
			}
			if err := p.disk.StoreErr(); err != nil {
				t.Error(err)
			}
		}()
	}
	names := []string{"a.wal", "b.wal", "c.ckpt", "d"}
	pick := func() string { return names[rng.IntN(len(names))] }
	// size is a file's size now, from the model.
	size := func(name string) int { return len(p.model.Files()[name]) }
	for step := range 600 {
		var what string
		switch k := rng.IntN(100); {
		case k < 6:
			name := pick()
			what = "create " + name
			var ha, hb wal.File
			p.both(step, what, func() (any, error) {
				h, err := p.disk.Create(name)
				ha = h
				return nil, err
			}, func() (any, error) {
				h, err := p.model.Create(name)
				hb = h
				return nil, err
			})
			if ha != nil {
				p.handles[name] = [2]wal.File{ha, hb}
			}
		case k < 40:
			name := pick()
			h, ok := p.handles[name]
			if !ok {
				continue
			}
			// Writes that cross pages, in place, appended and past the end.
			n := rng.IntN(3 * simfs.PageBytes)
			off := rng.IntN(size(name) + simfs.PageBytes + 1)
			if rng.IntN(3) == 0 {
				off = size(name)
			}
			b := make([]byte, n)
			for i := range b {
				b[i] = byte(rng.Uint32())
			}
			if rng.IntN(20) == 0 {
				a, m := rngs(rng)
				p.disk.ShortWrite(a)
				p.model.ShortWrite(m)
			}
			what = fmt.Sprintf("write %s at %d: %d bytes", name, off, n)
			p.both(step, what, func() (any, error) { return h[0].WriteAt(b, int64(off)) }, func() (any, error) { return h[1].WriteAt(b, int64(off)) })
		case k < 48:
			name := pick()
			h, ok := p.handles[name]
			if !ok {
				continue
			}
			to := rng.IntN(size(name) + 2)
			what = fmt.Sprintf("truncate %s to %d", name, to)
			p.both(step, what, func() (any, error) { return nil, h[0].Truncate(int64(to)) }, func() (any, error) { return nil, h[1].Truncate(int64(to)) })
		case k < 62:
			name := pick()
			h, ok := p.handles[name]
			if !ok {
				continue
			}
			what = "sync " + name
			if rng.IntN(5) == 0 {
				what = "failed sync " + name
				a, m := rngs(rng)
				p.disk.FailSync(a)
				p.model.FailSync(m)
			}
			p.both(step, what, func() (any, error) { return nil, h[0].Sync() }, func() (any, error) { return nil, h[1].Sync() })
		case k < 66:
			what = "sync the directory"
			p.both(step, what, func() (any, error) { return nil, p.disk.SyncDir() }, func() (any, error) { return nil, p.model.SyncDir() })
		case k < 69:
			from, to := pick(), pick()
			what = fmt.Sprintf("rename %s to %s", from, to)
			p.both(step, what, func() (any, error) { return nil, p.disk.Rename(from, to) }, func() (any, error) { return nil, p.model.Rename(from, to) })
			if h, ok := p.handles[from]; ok && from != to {
				delete(p.handles, from)
				p.handles[to] = h
			}
		case k < 71:
			name := pick()
			what = "remove " + name
			p.both(step, what, func() (any, error) { return nil, p.disk.Remove(name) }, func() (any, error) { return nil, p.model.Remove(name) })
			delete(p.handles, name)
		case k < 76:
			img := rng.IntN(4)
			what = fmt.Sprintf("crash, image %d", img)
			a, m := rngs(rng)
			p.disk.CrashImage(a, simfs.Image(img))
			p.model.CrashImage(m, ref.Image(img))
			p.setup2()
		case k < 80:
			name := pick()
			h, ok := p.handles[name]
			if !ok {
				continue
			}
			what = "drop the cache of " + name
			p.both(step, what, func() (any, error) {
				return nil, h[0].(interface{ DropCache() error }).DropCache()
			}, func() (any, error) {
				return nil, h[1].(interface{ DropCache() error }).DropCache()
			})
		case k < 83:
			name := pick()
			if n := size(name); n > 0 {
				off := rng.IntN(n)
				what = fmt.Sprintf("rot %s at %d", name, off)
				p.disk.Rot(name, off)
				p.model.Rot(name, off)
			}
		case k < 85:
			name := pick()
			if n := len(p.model.DurableBytes(name)); n > 0 && slices.Contains(p.model.DurableNames(), name) {
				off := rng.IntN(n)
				what = fmt.Sprintf("flip %s at %d", name, off)
				p.disk.FlipDurable(name, off)
				p.model.FlipDurable(name, off)
			}
		case k < 88:
			name := pick()
			a, m := rngs(rng)
			switch rng.IntN(4) {
			case 0:
				what = "corrupt"
				p.both(step, what, func() (any, error) { return p.disk.Corrupt(a), nil }, func() (any, error) { return p.model.Corrupt(m), nil })
			case 1:
				what = "corrupt " + name
				p.both(step, what, func() (any, error) { return p.disk.CorruptFile(a, name), nil }, func() (any, error) { return p.model.CorruptFile(m, name), nil })
			case 2:
				what = "lose a write of " + name
				p.both(step, what, func() (any, error) { return p.disk.LoseWrite(a, name), nil }, func() (any, error) { return p.model.LoseWrite(m, name), nil })
			default:
				what = "a bad sector in " + name
				p.both(step, what, func() (any, error) { return p.disk.BadSector(a, name), nil }, func() (any, error) { return p.model.BadSector(m, name), nil })
			}
		case k < 90:
			name := pick()
			a, m := rngs(rng)
			what = "misread " + name
			p.both(step, what, func() (any, error) { return p.disk.Misread(a, name), nil }, func() (any, error) { return p.model.Misread(m, name), nil })
		case k < 96:
			name := pick()
			h, ok := p.handles[name]
			if !ok {
				continue
			}
			n, off := rng.IntN(2*simfs.PageBytes), rng.IntN(size(name)+2)
			what = fmt.Sprintf("read %s at %d: %d bytes", name, off, n)
			p.both(step, what, func() (any, error) {
				b := make([]byte, n)
				k, err := h[0].ReadAt(b, int64(off))
				return b[:k], err
			}, func() (any, error) {
				b := make([]byte, n)
				k, err := h[1].ReadAt(b, int64(off))
				return b[:k], err
			})
		case k < 98:
			what = "copy"
			p.fork(p.disk.Copy(), p.model.Copy(), rng)
		default:
			what = "the durable copy"
			p.fork(p.disk.Durable(), p.model.Durable(), rng)
		}
		p.same(step, what)
	}
}

// setup2 is setup after the disks changed under the handles, keeping the
// traces.
func (p *pair) setup2() { p.setup() }

// fork keeps one side of a copy unchanged from now on and goes on with
// the other: the copy or the disk it came from.
func (p *pair) fork(disk *simfs.Disk, model *ref.Disk, rng *rand.Rand) {
	// The last few copies are enough, and keep the test quick.
	if len(p.kept) == 4 {
		p.kept = p.kept[1:]
	}
	if rng.IntN(2) == 0 {
		p.kept = append(p.kept, [2]any{disk, model})
		return
	}
	p.kept = append(p.kept, [2]any{p.disk, p.model})
	p.disk, p.model = disk, model
	p.trace, p.modelTrace = nil, nil
	p.setup2()
}

// TestStoreErr checks that a disk whose Store fails reports it.
func TestStoreErr(t *testing.T) {
	d := simfs.New()
	errFull := errors.New("the host disk is full")
	d.KeepIn(&memStore{fail: errFull}, 0)
	f, err := d.Create("a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 3*simfs.PageBytes)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := d.StoreErr(); !errors.Is(err, errFull) {
		t.Errorf("the disk's store error is %v, want %v", err, errFull)
	}
}

// TestRewrittenPagesGoBackToTheStore checks that pages rewritten after
// they went to the Store go there again at the next sync, as a reused log
// segment is rewritten from its start, and that the file reads what was
// written last.
func TestRewrittenPagesGoBackToTheStore(t *testing.T) {
	d := simfs.New()
	store := &memStore{}
	d.KeepIn(store, 0)
	f, err := d.Create("a.wal")
	if err != nil {
		t.Fatal(err)
	}
	page := func(c byte) []byte { return bytes.Repeat([]byte{c}, 8*simfs.PageBytes) }
	for i, c := range []byte{'a', 'b'} {
		if _, err := f.WriteAt(page(c), 0); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		// With no cache, every page of each version goes to the store.
		if got, want := len(store.pages), 8*(i+1); got != want {
			t.Errorf("after version %c, the store holds %d pages, want %d", c, got, want)
		}
	}
	if got := d.Files()["a.wal"]; !bytes.Equal(got, page('b')) {
		t.Error("the file does not read its last version")
	}
	if err := d.StoreErr(); err != nil {
		t.Error(err)
	}
}

// TestStoreReadErr checks that a disk whose Store fails a read reports it.
func TestStoreReadErr(t *testing.T) {
	d := simfs.New()
	store := &memStore{}
	d.KeepIn(store, 0)
	f, err := d.Create("a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{'x'}, 3*simfs.PageBytes)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	errGone := errors.New("the host file is gone")
	store.fail = errGone
	if _, err := f.ReadAt(make([]byte, simfs.PageBytes), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.StoreErr(); !errors.Is(err, errGone) {
		t.Errorf("the disk's store error is %v, want %v", err, errGone)
	}
}

// TestRewrittenTailStaysOut checks that a page that a file alone held and
// replaced, as each sync of a log replaces its last page, never goes to
// the Store: it is dead once replaced.
func TestRewrittenTailStaysOut(t *testing.T) {
	d := simfs.New()
	store := &memStore{}
	d.KeepIn(store, 2)
	f, err := d.Create("a.wal")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := f.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(store.pages); n != 0 {
		t.Errorf("the store holds %d pages, want none: each was replaced", n)
	}
	if got := d.Files()["a.wal"]; len(got) != 100 || got[99] != 99 {
		t.Errorf("the file reads %d bytes", len(got))
	}
}

// TestCutPagesStayOut checks that the pages a truncate drops from a file
// that alone held them never go to the Store: they are dead once cut.
func TestCutPagesStayOut(t *testing.T) {
	d := simfs.New()
	store := &memStore{}
	d.KeepIn(store, 16)
	write := func(name string, n int) wal.File {
		t.Helper()
		f, err := d.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(make([]byte, n*simfs.PageBytes)); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		return f
	}
	a := write("a.log", 8)
	if err := a.Truncate(simfs.PageBytes); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}
	write("b.log", 16)
	if n := len(store.pages); n != 1 {
		t.Errorf("the store holds %d pages, want 1: the 7 cut pages are dead", n)
	}
	if got := d.Files()["a.log"]; len(got) != simfs.PageBytes {
		t.Errorf("a.log reads %d bytes, want %d", len(got), simfs.PageBytes)
	}
}

// TestSyncedPagesLeaveMemory checks that a disk keeps in memory only the
// newest synced pages of its cache, the others in the Store, and reads all
// of them back.
func TestSyncedPagesLeaveMemory(t *testing.T) {
	d := simfs.New()
	store := &memStore{}
	d.KeepIn(store, 8)
	f, err := d.Create("a.ckpt")
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 64*simfs.PageBytes)
	for i := range want {
		want[i] = byte(i / simfs.PageBytes)
	}
	if _, err := f.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := len(store.pages); got != 64-8 {
		t.Errorf("the store holds %d pages, want %d", got, 64-8)
	}
	if got := d.Files()["a.ckpt"]; !bytes.Equal(got, want) {
		t.Error("the file does not read what was written")
	}
	if err := d.StoreErr(); err != nil {
		t.Error(err)
	}
}
