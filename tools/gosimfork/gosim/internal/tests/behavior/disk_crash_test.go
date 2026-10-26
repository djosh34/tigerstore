//go:build sim

package behavior

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/jellevandenhooff/gosim"
)

// XXX: check errors on all file ops?
// XXX: test / cause errors on all file ops?

// XXX: check iterAllCrashes and randomCrashSubset behave the same way:
// add a helper that call Crash (many) times and compares scenarios

func TestCrashDiskBasic(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		a.Write([]byte("a"))
		a.Close()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"file a empty":       {"a": {}},
		"file a single zero": {"a": {0}},
		"file a single a":    {"a": {'a'}},
		"file a missing":     {},
	})
}

func TestCrashDiskSingleFileMultipleWrite(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Truncate(2); err != nil {
			t.Error(err)
			return
		}
		if err := a.Sync(); err != nil {
			t.Error(err)
			return
		}
		d, err := os.OpenFile(".", os.O_RDONLY, 0o600)
		if err != nil {
			t.Error(err)
			return
		}
		d.Sync()
		a.Write([]byte("a"))
		a.Write([]byte("b"))
		a.Close()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"file a 00": {"a": {0, 0}},
		"file a a0": {"a": {'a', 0}},
		"file a 0b": {"a": {0, 'b'}},
		"file a ab": {"a": {'a', 'b'}},
	})
}

func TestCrashDiskMultipleFileMultipleWrite(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Truncate(2); err != nil {
			t.Error(err)
		}
		if err := a.Sync(); err != nil {
			t.Error(err)
		}
		b, err := os.OpenFile("b", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Truncate(2); err != nil {
			t.Error(err)
		}
		if err := b.Sync(); err != nil {
			t.Error(err)
		}
		d, err := os.OpenFile(".", os.O_RDONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		d.Sync()
		a.Write([]byte("a"))
		a.Write([]byte("b"))
		b.Write([]byte("c"))
		b.Write([]byte("d"))
		a.Close()
		b.Close()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, crossProductContents(map[string]map[string][]byte{
		"file a 00": {"a": {0, 0}},
		"file a a0": {"a": {'a', 0}},
		"file a 0b": {"a": {0, 'b'}},
		"file a ab": {"a": {'a', 'b'}},
	}, map[string]map[string][]byte{
		"file b 00": {"b": {0, 0}},
		"file b c0": {"b": {'c', 0}},
		"file b 0d": {"b": {0, 'd'}},
		"file b cd": {"b": {'c', 'd'}},
	}))
}

func TestCrashDiskFileSync(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		a.Write([]byte("a"))
		a.Sync()
		a.Close()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"file a single a": {"a": {'a'}},
		"file a missing":  {},
	})
}

func TestCrashDiskWriteRename(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		a.Write([]byte("a"))
		a.Close()
		os.Rename("a", "b")
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"both missing":       {},
		"file a empty":       {"a": {}},
		"file a single zero": {"a": {0}},
		"file a single a":    {"a": {'a'}},
		"file b empty":       {"b": {}},
		"file b single zero": {"b": {0}},
		"file b single a":    {"b": {'a'}},
	})
}

func TestCrashDiskWriteRenameSyncFile(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// XXX: check errs?
		a.Write([]byte("a"))
		a.Sync()
		a.Close()
		os.Rename("a", "b")
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"both missing":    {},
		"file a single a": {"a": {'a'}},
		"file b single a": {"b": {'a'}},
	})
}

func TestCrashDiskWriteRenameSyncDir(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// XXX: check errs?
		a.Write([]byte("a"))
		a.Close()
		os.Rename("a", "b")
		d, err := os.OpenFile(".", os.O_RDONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		d.Sync()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		// XXX: is this correct?
		"file b empty":       {"b": {}},
		"file b single zero": {"b": {0}},
		"file b single a":    {"b": {'a'}},
	})
}

// XXX: deps for reads/writes to same name??? what if rename a,b -> rename b,c???

func TestCrashDiskWriteRenameSyncFileAndDir(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// XXX: check errs?
		a.Write([]byte("a"))
		a.Sync()
		a.Close()
		os.Rename("a", "b")
		d, err := os.OpenFile(".", os.O_RDONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		d.Sync()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"file b single a": {"b": {'a'}},
	})
}

func TestCrashDiskWriteRenameTwiceSyncFile(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// XXX: check errs?
		a.Write([]byte("a"))
		a.Sync()
		a.Close()
		os.Rename("a", "b")
		os.Rename("b", "c")
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, map[string]map[string][]byte{
		"no file":         {},
		"file a single a": {"a": {'a'}},
		"file b single a": {"b": {'a'}},
		"file c single a": {"c": {'a'}},
	})
}

func TestCrashDiskTwoFiles(t *testing.T) {
	m := gosim.NewSimpleMachine(func() {
		a, err := os.OpenFile("a", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		a.Write([]byte("a"))
		a.Close()

		b, err := os.OpenFile("b", os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		b.Write([]byte("b"))
		b.Close()
		gosim.CurrentMachine().Crash()
	})
	m.Wait()

	disks := extractDisks(t, m)

	expectOutcomesExactly(t, disks, crossProductContents(map[string]map[string][]byte{
		"file a empty":       {"a": {}},
		"file a single zero": {"a": {0}},
		"file a single a":    {"a": {'a'}},
		"file a missing":     {},
	}, map[string]map[string][]byte{
		"file b empty":       {"b": {}},
		"file b single zero": {"b": {0}},
		"file b single b":    {"b": {'b'}},
		"file b missing":     {},
	}))
}

func crossProductContents(a, b map[string]map[string][]byte) map[string]map[string][]byte {
	c := make(map[string]map[string][]byte)
	for a, aa := range a {
		for b, bb := range b {
			cc := make(map[string][]byte)
			for k, v := range aa {
				cc[k] = v
			}
			for k, v := range bb {
				cc[k] = v
			}
			c[a+" and "+b] = cc
		}
	}
	return c
}

func readFiles(t *testing.T) map[string][]byte {
	read := make(map[string][]byte)

	files, err := os.ReadDir(".")
	if err != nil {
		t.Error(err)
		return nil
	}

	for _, file := range files {
		// XXX: os.Open, os.ReadFile?
		f, err := os.OpenFile(file.Name(), os.O_RDONLY, 0o644)
		if err != nil {
			t.Error(err)
			return nil
		}
		bytes, err := io.ReadAll(f)
		if err != nil {
			f.Close()
			t.Error(err)
			return nil
		}

		read[file.Name()] = bytes
	}

	return read
}

func matches(a, b map[string][]byte) bool {
	for k := range a {
		// distinguish missing from equal nils
		if _, ok := b[k]; !ok {
			return false
		}

		if !bytes.Equal(a[k], b[k]) {
			return false
		}
	}

	for k := range b {
		if _, ok := a[k]; !ok {
			return false
		}
	}

	return true
}

// XXX: norace because mu and all are shared, m.Run has is its own context. should fix somehow
//
//go:norace
func extractDisks(t *testing.T, m gosim.Machine) []map[string][]byte {
	var mu sync.Mutex
	var all []map[string][]byte

	iter := m.IterDiskCrashStates(func() {
		// XXX: this lock should (not) be necessary?
		mu.Lock()
		defer mu.Unlock()
		read := readFiles(t)
		all = append(all, read)
	})

	for m := range iter {
		m.Wait()
		// XXX: should stop m here so its background stuff doesnt clog us up
		// m.Crash() // XXX: use m.Shutdown instead
	}
	// XXX: this lock should (not) be necessary?
	mu.Lock()
	defer mu.Unlock()
	return all
}

func expectOutcomesExactly(t *testing.T, disks []map[string][]byte, expected map[string]map[string][]byte) {
	count := make(map[string]int)
	for _, disk := range disks {
		found := false
		for name, contents := range expected {
			if matches(disk, contents) {
				count[name]++
				found = true
				break
			}
		}
		if !found {
			t.Error("unexpected outcome")
			for name, contents := range disk {
				log.Print(name, contents)
			}
			return
		}
	}

	for name := range expected {
		if count[name] == 0 {
			t.Error("missing outcome", name)
		}
		log.Print(name, count[name])
	}
}

/*
func missingFile(t *testing.T, name string) bool {
	f, err := os.OpenFile(name, os.O_RDONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true
		}
		t.Error(err)
		return false
	}
	f.Close()
	return false
}

func fileContents(t *testing.T, name string, contents []byte) bool {
	f, err := os.OpenFile(name, os.O_RDONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		t.Error(err)
		return false
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	f.Close()
	return bytes.Equal(contents, b)
}

func crossProduct(a, b map[string]bool) map[string]bool {
	c := make(map[string]bool)
	for a, aa := range a {
		for b, bb := range b {
			c[a+" and "+b] = aa && bb
		}
	}
	return c
}

func distinguish(t *testing.T, m map[string]bool) {
	var keys []string
	var found []string
	for name, b := range m {
		keys = append(keys, name)
		if b {
			found = append(found, name)
		}
	}
	sort.Strings(keys)
	sort.Strings(found)

	if len(found) != 1 {
		t.Error(fmt.Sprintf("expected exactly 1 found, got %v", found))
		return
	}

	s.Observe("result", "keys", keys)
	s.Observe("result", "found", found[0])
}
*/

// XXX: have a helper that enumerates all possible allowed disk outcomes? since we can clone anyway?
// - can also here include sync w/ crash backdated before in these scenarios for nice perf?

// XXX: "test" or "inspect" disk option, clone machine and then see what happens?
// XXX: "allowed" and "not-allowed" and "expected/demanded" outcomes
// XXX: automatic restart, recovery of crashed machine (in a larger simulation?)

// return or observe disk state

// XXX: test disk deps? want some graph like api...; serialization of ops?

/*
func TestCrashDiskFileSync(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskFileSync)
	combineDistinguish(t, runs)
}

func TestCrashDiskTwoFiles(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskTwoFiles)
	combineDistinguish(t, runs)
}

func TestCrashDiskWriteRename(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskWriteRename)
	combineDistinguish(t, runs)
}

func TestCrashDiskWriteRenameSyncFile(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskWriteRenameSyncFile)
	combineDistinguish(t, runs)
}

func TestCrashDiskWriteRenameTwiceSyncFile(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskWriteRenameTwiceSyncFile)
	combineDistinguish(t, runs)
}

func TestCrashDiskWriteRenameSyncDir(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskWriteRenameSyncDir)
	combineDistinguish(t, runs)
}

func TestCrashDiskWriteRenameSyncFileAndDir(t *testing.T) {
	config := simtesting.ConfigWithNSeeds(1000)

	runs := config.RunSim(t, TestCrashDiskWriteRenameSyncFileAndDir)
	combineDistinguish(t, runs)
}

func combineDistinguish(t *testing.T, results []gosimruntime.RunResult) {
	t.Helper()

	count := make(map[string]int)

	if len(results) == 0 {
		t.Fatal("need at least one run")
	}

	knownKeys := results[0].Observation("result", "keys").([]string)
	for _, key := range knownKeys {
		count[key] = 0
	}

	for _, result := range results {
		keys := result.Observation("result", "keys").([]string)
		found := result.Observation("result", "found").(string)

		if !slices.Equal(keys, knownKeys) {
			t.Errorf("bad keys, got %v expected %v", keys, knownKeys)
		}

		if _, ok := count[found]; !ok {
			t.Errorf("bad found %s", found)
		}

		count[found]++
	}

	for key, val := range count {
		if val == 0 {
			t.Errorf("missing outcome %s", key)
		}
	}

	keys := maps.Keys(count)
	slices.Sort(keys)
	for _, key := range keys {
		t.Log(key, count[key])
	}
}
*/

// things to test:
// - file write then rename
// - partial writes
// - reordered writes (same file, different file)
// - sync works
// - reordered metadata ops
