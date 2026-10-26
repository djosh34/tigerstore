// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found at https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

package race_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jellevandenhooff/gosim/internal/gosimtool"
	"github.com/jellevandenhooff/gosim/internal/race"
)

// copied and modified from go:
// XXX: use -json?
// TODO: get rid of globals please?

var (
	passedTests = 0
	totalTests  = 0
	falsePos    = 0
	falseNeg    = 0
	// failingPos  = 0
	// failingNeg  = 0
	failed = false
)

const (
	visibleLen = 40
	testPrefix = "=== RUN   Test"
)

// TODO: this test really ought to run things under a couple of seeds
func TestRaceSim(t *testing.T) {
	if !race.Enabled {
		t.Skip("race.Enabled is false")
	}

	path := gosimtool.GetPathForPrecompiledTestBinary(t, gosimtool.Module+"/internal/tests/race/testdata", nil)
	checkTests(t, exec.Command(path, "-test.v"))
}

func TestRaceOriginal(t *testing.T) {
	if !race.Enabled {
		t.Skip("race.Enabled is false")
	}

	modDir, err := gosimtool.FindGoModDir()
	if err != nil {
		t.Fatal(err)
	}

	// TODO: get this from some shared package instead
	path := filepath.Join(modDir, gosimtool.OutputDirectory, "racetest", "testdata.test")

	checkTests(t, exec.Command(path, "-test.v"))
}

func checkTests(t *testing.T, cmd *exec.Cmd) {
	files, err := filepath.Glob("./testdata/*.go")
	if err != nil {
		t.Fatal(err)
	}
	_ = files

	passedTests = 0
	totalTests = 0
	falsePos = 0
	falseNeg = 0
	// failingPos  = 0
	// failingNeg  = 0
	failed = false

	testOutput, err := runTests(cmd)
	if err != nil {
		t.Fatalf("Failed to run tests: %v\n%v", err, string(testOutput))
	}
	reader := bufio.NewReader(bytes.NewReader(testOutput))

	funcName := ""
	for {
		s, err := nextLine(reader)
		if err != nil {
			break
		}
		if strings.HasPrefix(s, testPrefix) {
			funcName = s[len(testPrefix):]
			break
		}
	}

	var tsanLog []string
	for {
		s, err := nextLine(reader)
		if err != nil {
			fmt.Printf("%s\n", processLog(funcName, tsanLog))
			break
		}
		if strings.HasPrefix(s, testPrefix) {
			fmt.Printf("%s\n", processLog(funcName, tsanLog))
			tsanLog = make([]string, 0, 100)
			funcName = s[len(testPrefix):]
		} else {
			tsanLog = append(tsanLog, s)
		}
	}

	if totalTests == 0 {
		t.Fatalf("failed to parse test output:\n%s", testOutput)
	}
	fmt.Printf("\nPassed %d of %d tests (%.02f%%, %d+, %d-)\n",
		passedTests, totalTests, 100*float64(passedTests)/float64(totalTests), falsePos, falseNeg)
	// fmt.Printf("%d expected failures (%d has not fail)\n", failingPos+failingNeg, failingNeg)

	if totalTests == 0 {
		t.Errorf("expected > 0 parsed tests, got %d", totalTests)
	}

	if failed {
		t.Fail()
	}
}

// nextLine is a wrapper around bufio.Reader.ReadString.
// It reads a line up to the next '\n' character. Error
// is non-nil if there are no lines left, and nil
// otherwise.
func nextLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			log.Fatalf("nextLine: expected EOF, received %v", err)
		}
		return s, err
	}
	return s[:len(s)-1], nil
}

// processLog verifies whether the given ThreadSanitizer's log
// contains a race report, checks this information against
// the name of the testcase and returns the result of this
// comparison.
func processLog(testName string, tsanLog []string) string {
	gotRace := false
	for _, s := range tsanLog {
		if strings.Contains(s, "DATA RACE") {
			gotRace = true
			break
		}
	}

	// failing := strings.Contains(testName, "Failing")
	expRace := !strings.HasPrefix(testName, "No")
	for len(testName) < visibleLen {
		testName += " "
	}
	if expRace == gotRace {
		passedTests++
		totalTests++
		// if failing {
		// failed = true
		// failingNeg++
		// }
		return fmt.Sprintf("%s .", testName)
	}
	pos := ""
	if expRace {
		falseNeg++
	} else {
		falsePos++
		pos = "+"
	}
	// if failing {
	// failingPos++
	// } else {
	failed = true
	// }
	totalTests++

	return fmt.Sprintf("%s %s%s", testName, "FAILED", pos)
}

func runTests(cmd *exec.Cmd) ([]byte, error) {
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "GOMAXPROCS=") ||
			strings.HasPrefix(env, "GODEBUG=") ||
			strings.HasPrefix(env, "GORACE=") {
			continue
		}
		cmd.Env = append(cmd.Env, env)
	}
	// We set GOMAXPROCS=1 to prevent test flakiness.
	// There are two sources of flakiness:
	// 1. Some tests rely on particular execution order.
	//    If the order is different, race does not happen at all.
	// 2. Ironically, ThreadSanitizer runtime contains a logical race condition
	//    that can lead to false negatives if racy accesses happen literally at the same time.
	// Tests used to work reliably in the good old days of GOMAXPROCS=1.
	// So let's set it for now. A more reliable solution is to explicitly annotate tests
	// with required execution order by means of a special "invisible" synchronization primitive
	// (that's what is done for C++ ThreadSanitizer tests). This is issue #14119.
	cmd.Env = append(cmd.Env,
		"GOMAXPROCS=1",
		"GORACE=suppress_equal_stacks=0 suppress_equal_addresses=0",
	)

	out, _ := cmd.CombinedOutput()
	log.Println(string(out))
	if bytes.Contains(out, []byte("fatal error:")) {
		// But don't expect runtime to crash.
		return out, fmt.Errorf("runtime fatal error")
	}
	return out, nil
}
