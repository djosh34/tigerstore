//go:build !sim

package node

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestGosimAll(t *testing.T) {
	var tags []string
	if os.Getenv("TAGS") != "" {
		tags = []string{os.Getenv("TAGS")}
	}
	mt := metatesting.ForOtherPackage(t, "github.com/djosh34/tigerstore/internal/node", tags...)
	tests, err := mt.ListTests()
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(os.Getenv("RUN"))
	for _, name := range tests {
		if !re.MatchString(name) {
			continue
		}
		start := time.Now()
		r, err := mt.RunQuiet(&metatesting.RunConfig{Test: name, Seed: 1, ExtraEnv: strings.Fields(os.Getenv("SIMENV"))})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: failed=%v err=%q took %v", name, r.Failed, r.Err, time.Since(start))
		if r.Failed || os.Getenv("KEEPLOG") != "" {
			os.WriteFile("/tmp/w351/fail-node-"+name+".log", r.LogOutput, 0o644)
		}
	}
}
