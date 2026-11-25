package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A start that is not up after its time gets one SIGUSR1, with a line in
// its log; one whose log has the "up" line since its start, or one that
// exited, gets none (#780). sleep stands in for the node: SIGUSR1 ends it,
// where cmd/tigerstore writes its stacks and goes on.
func TestASlowStartIsAskedForItsStacks(t *testing.T) {
	for _, c := range []struct {
		name    string
		up      string
		signals bool
	}{
		{"not up", "", true},
		{"up", "tigerstore-wl node node1 (id 1) up: clients [x], peers [y]\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			log, err := os.Create(filepath.Join(t.TempDir(), "node1.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			// An earlier incarnation was up: only the lines since this
			// start count.
			if _, err := log.WriteString("tigerstore-wl node node1 (id 1) up: clients [old], peers [old]\n"); err != nil {
				t.Fatal(err)
			}
			st, err := log.Stat()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("sleep", "30")
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			n := &node{log: log, cmd: cmd}
			if _, err := log.WriteString(c.up); err != nil {
				t.Fatal(err)
			}
			n.stacksIfSlow(cmd, st.Size(), 2, 50*time.Millisecond)
			b, err := os.ReadFile(log.Name())
			if err != nil {
				t.Fatal(err)
			}
			noted := strings.Contains(string(b), "chaos: incarnation 2 is not up")
			waited := make(chan error, 1)
			go func() { waited <- cmd.Wait() }()
			var signaled bool
			select {
			case err := <-waited:
				var ee *exec.ExitError
				signaled = errors.As(err, &ee) && ee.Sys().(syscall.WaitStatus).Signal() == syscall.SIGUSR1
			case <-time.After(time.Second):
				_ = cmd.Process.Kill()
				<-waited
			}
			if noted != c.signals || signaled != c.signals {
				t.Fatalf("noted in the log %v, signaled %v; want %v", noted, signaled, c.signals)
			}
		})
	}
}
