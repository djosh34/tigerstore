package node

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/djosh34/tigerstore/internal/osfs"
)

// TestNextRun counts a node's runs, past a temporary file that a crash
// left behind.
func TestNextRun(t *testing.T) {
	path := t.TempDir()
	dir, err := osfs.New(path)
	if err != nil {
		t.Fatal(err)
	}
	for want := uint64(1); want <= 3; want++ {
		if want == 3 {
			if err := os.WriteFile(filepath.Join(path, runTmp), []byte("torn"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		got, err := nextRun(dir)
		if err != nil || got != want {
			t.Fatalf("run %d, %v; want %d", got, err, want)
		}
	}
}

// TestRunAfterZeros reads a run file that holds the run number followed
// by zeros, as LazyFS left one (#240), and refuses one with other bytes
// after it.
func TestRunAfterZeros(t *testing.T) {
	path := t.TempDir()
	dir, err := osfs.New(path)
	if err != nil {
		t.Fatal(err)
	}
	b := append(binary.LittleEndian.AppendUint64(nil, 6), make([]byte, 16071)...)
	if err := os.WriteFile(filepath.Join(path, runFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := nextRun(dir); err != nil || got != 7 {
		t.Fatalf("run %d, %v; want 7", got, err)
	}
	b = append(binary.LittleEndian.AppendUint64(nil, 6), 0, 1)
	if err := os.WriteFile(filepath.Join(path, runFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := nextRun(dir); err == nil {
		t.Fatal("a run file with stray bytes after the number was read")
	}
}
