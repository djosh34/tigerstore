package osfs

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

// TestLogFileAppends appends records of random sizes to a segment opened
// with OpenLog, cuts it back now and then, and opens it again, as the log
// does: after each step the file reads as the appends made it, though
// O_DIRECT writes whole blocks. It needs a file system with O_DIRECT, as
// ext4 and xfs have; on tmpfs OpenLog is Open, and the test says so.
func TestLogFileAppends(t *testing.T) {
	dir, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, err := dir.Create("seg")
	if err != nil {
		t.Fatal(err)
	}
	// A reused segment holds old bytes past the log's end.
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xee}, 3*blockBytes+100), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	open := func() *logFile {
		t.Helper()
		lf, err := dir.OpenLog("seg")
		if err != nil {
			t.Fatal(err)
		}
		l, ok := lf.(*logFile)
		if !ok {
			t.Skip("the file system of the test's temporary directory has no O_DIRECT")
		}
		return l
	}
	l := open()
	rng := rand.New(rand.NewPCG(3, 4))
	model := bytes.Repeat([]byte{0xee}, 3*blockBytes+100)
	end := int64(0)
	check := func(step int) {
		t.Helper()
		got, err := os.ReadFile(filepath.Join(dir.path, "seg"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:end], model[:end]) {
			t.Fatalf("step %d: the file's first %d bytes differ from the appends", step, end)
		}
		if int64(len(got)) < end {
			t.Fatalf("step %d: the file holds %d bytes, the appends %d", step, len(got), end)
		}
	}
	for step := range 300 {
		switch rng.IntN(20) {
		case 0:
			end = rng.Int64N(end + 1)
			if err := l.Truncate(end); err != nil {
				t.Fatal(err)
			}
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
			model = model[:end]
		case 1:
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l = open()
		default:
			p := make([]byte, 1+rng.IntN(3*blockBytes))
			for i := range p {
				p[i] = byte(rng.Uint32())
			}
			if n, err := l.WriteAt(p, end); err != nil || n != len(p) {
				t.Fatalf("step %d: write of %d bytes at %d: %d, %v", step, len(p), end, n, err)
			}
			model = append(model[:end], p...)
			end += int64(len(p))
		}
		check(step)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
