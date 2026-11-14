// Command benchseeds draws fresh seeds for a bench run (bench.yml), at
// least some of which draw a given recorded cluster shape for k8s
// (bench/recordings), so that the shape a bench is judged by runs in
// enough seeds to judge it.
//
//	benchseeds [-n 8] [-shape crd-heavy] [-min 4] [-traces bench/recordings]
//
// It prints n seeds, space-separated: min that draw the shape, the rest
// whatever they draw.
package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// errNoShape is the error of a shape no recording has.
var errNoShape = errors.New("benchseeds: no recording of the shape")

func main() {
	n := flag.Int("n", 8, "seeds to draw")
	shape := flag.String("shape", "crd-heavy", "the shape that at least -min seeds draw")
	least := flag.Int("min", 4, "seeds that draw -shape")
	traces := flag.String("traces", "bench/recordings", "the recordings")
	flag.Parse()
	seeds, err := draw(*traces, *shape, *n, *least, rand.Uint64)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(strings.Join(seeds, " "))
}

// draw draws n seeds from next, least of which draw shape from the
// recordings in dir. Seeds are below 10^9, as bench.yml's are.
func draw(dir, shape string, n, least int, next func() uint64) ([]string, error) {
	if _, err := os.Stat(filepath.Join(dir, shape+".trace.gz")); err != nil {
		return nil, fmt.Errorf("%w %s in %s: %w", errNoShape, shape, dir, err)
	}
	var seeds []string
	matched := 0
	for len(seeds) < n {
		seed := 1 + next()%999_999_999
		name, err := k8strace.Picks(dir, seed)
		if err != nil {
			return nil, err
		}
		is := filepath.Base(name) == shape+".trace.gz"
		// A seed that draws another shape is skipped once the places
		// left are only enough for the seeds that must draw this one.
		if !is && matched < least && n-len(seeds) <= least-matched {
			continue
		}
		if is {
			matched++
		}
		seeds = append(seeds, fmt.Sprint(seed))
	}
	return seeds, nil
}
