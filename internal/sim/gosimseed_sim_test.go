//go:build sim

package sim

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
)

func TestGosimSeed(t *testing.T) {
	gosim.SetSimulationTimeout(2 * time.Hour)
	seed, err := strconv.ParseUint(os.Getenv("SIM_SEED"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	res, err := runInspect(DefaultConfig(seed), nil)
	if res != nil {
		trace := strings.Join(res.Trace, "\n")
		var b strings.Builder
		for _, r := range res.History {
			fmt.Fprintf(&b, "%d %d %d %d %T %v -> %v %v\n", r.Client, r.Node, r.Call, r.Return, r.req, r.req, r.Resp, r.Err)
		}
		hist := []byte(b.String())
		gosim.WriteArtifact("trace.txt", []byte(trace))
		gosim.WriteArtifact("hashes.txt", []byte(fmt.Sprintf("stats %+v log %d\ntrace %x\nhistory %x\n", res.Stats, len(res.Log), sha256.Sum256([]byte(trace)), sha256.Sum256(hist))))
	}
	if err != nil {
		t.Fatal(err)
	}
	checkers(t, res)
}
