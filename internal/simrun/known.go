//go:build !sim

package simrun

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
)

// A knownSeed is a seed of a test.
type knownSeed struct {
	test string
	seed uint64
}

// knownFailures are the seeds in the package's known-failures.txt, if it
// has one, which fail because of an open bug issue, with the issue.
func knownFailures(t *testing.T) map[knownSeed]string {
	t.Helper()
	known := map[knownSeed]string{}
	f, err := os.Open("known-failures.txt")
	if errors.Is(err, fs.ErrNotExist) {
		return known
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	lines := bufio.NewScanner(f)
	for line := 1; lines.Scan(); line++ {
		text, _, _ := strings.Cut(lines.Text(), "#")
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
		var seed uint64
		if len(fields) == 3 {
			seed, err = strconv.ParseUint(fields[1], 10, 64)
		} else {
			err = errBadLine
		}
		if err != nil {
			t.Fatalf("known-failures.txt:%d: %v: want a test, a seed and an issue", line, err)
		}
		known[knownSeed{fields[0], seed}] = fields[2]
	}
	if err := lines.Err(); err != nil {
		t.Fatal(err)
	}
	return known
}

var errBadLine = errors.New("not three fields")
