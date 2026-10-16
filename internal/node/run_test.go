package node

import (
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
