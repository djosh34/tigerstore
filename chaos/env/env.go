// Package env sets up the privileged parts of a real-process chaos run
// (#148): a disk of its own for each node, a cgroup for each node, and
// netem on the loopback ports of single links. Every command that needs
// root runs through sudo -n, so the run itself does not.
//
// A node's disk is a file on a loop device, under a device-mapper target
// that is linear when healthy and can fail every write, delay reads and
// writes, or stall, then a small ext4 file system, which fills for real,
// and on top Jepsen's LazyFS, which holds every write until an fsync
// covers it and drops what none covered on a power loss.
package env

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// sudo runs a command as root and returns its combined output.
func sudo(args ...string) (string, error) {
	return runIn("", append([]string{"sudo", "-n"}, args...)...)
}

// sudoIn runs a command as root with stdin.
func sudoIn(stdin string, args ...string) (string, error) {
	return runIn(stdin, append([]string{"sudo", "-n"}, args...)...)
}

func runIn(stdin string, args ...string) (string, error) {
	cmd := exec.Command(args[0], args[1:]...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	return out.String(), nil
}

// writeRoot writes s to a file only root may write, such as a cgroup's.
func writeRoot(path, s string) error {
	_, err := sudoIn(s, "tee", path)
	return err
}

// waitFor polls cond every 10 ms until it holds or timeout passes.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// mounted reports whether path is a mount point.
func mounted(path string) bool {
	b, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		return false
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if f := strings.Fields(line); len(f) > 1 && f[1] == path {
			return true
		}
	}
	return false
}
