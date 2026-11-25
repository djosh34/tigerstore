//go:build !sim

package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestAForkReplaysAlone makes TestSyntheticFork's image and runs its fork
// from the image's file, as the driver does (simrun.ImageEach), twice: the
// image made again has the same SHA-256, and the fork run from it gives
// the same hashes, as a lone replay must (#794).
func TestAForkReplaysAlone(t *testing.T) {
	if simrun.Here(t) {
		t.Skip("under the race detector an image's forks run in its process")
	}
	env := []string{"SIM_SEED=1", "SIM_SEEDS=1"}
	var images, hashes []string
	for i := range 2 {
		img := simrun.Do(t, simrun.Run{Test: "TestSyntheticFork", Seed: 1, Env: append(env, "SIM_FORK=image")})
		if img.Failed {
			t.FailNow()
		}
		b := img.Artifacts["image"]
		sum := sha256.Sum256(b)
		images = append(images, hex.EncodeToString(sum[:]))
		path := filepath.Join(t.TempDir(), "image")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		fork := simrun.Do(t, simrun.Run{Test: "TestSyntheticFork", Seed: 1, Env: append(env, "SIM_FORK=0"), Inputs: map[string]string{"image": path}})
		if fork.Failed {
			t.FailNow()
		}
		hashes = append(hashes, string(fork.Artifacts["hashes"]))
		if len(hashes[i]) == 0 {
			t.Fatal("the fork left no hashes")
		}
	}
	if images[0] != images[1] || hashes[0] != hashes[1] {
		t.Fatalf("made again, the image is %s, then %s, and its fork gave\n%s\nthen\n%s", images[0], images[1], hashes[0], hashes[1])
	}
}
