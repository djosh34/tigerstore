package node

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
)

// BufferBytes is the memory a node keeps for what neither the index, the
// entries in memory nor the value cache hold: the buffers of clients,
// peers, the log and checkpoints, responses that wait for slow clients,
// value reads under way and goroutine stacks. It is a guess until the
// memory-limited runs of #148 measure it.
const BufferBytes = 64 << 20

// MemoryBudget returns the memory the node may use, and where that came
// from: flag if it is not 0, else GOMEMLIMIT, else the memory limit of
// the process's cgroup (v2), else the host's memory. root is the file
// system that holds /proc and /sys, os.DirFS("/") outside tests.
func MemoryBudget(root fs.FS, flag int64) (int64, string, error) {
	if flag < 0 {
		return 0, "", fmt.Errorf("node: memory budget of %d bytes", flag)
	}
	if flag > 0 {
		return flag, "flag", nil
	}
	if limit := debug.SetMemoryLimit(-1); limit < math.MaxInt64 {
		return limit, "GOMEMLIMIT", nil
	}
	limit, ok, err := cgroupLimit(root)
	if err != nil {
		return 0, "", err
	}
	if ok {
		return limit, "cgroup", nil
	}
	host, err := hostMemory(root)
	if err != nil {
		return 0, "", err
	}
	return host, "host", nil
}

// CacheMemory returns what the index, the entries in memory and the
// value cache share out of budget (core.Config.MemoryBytes): a quarter
// stays free, so that the garbage collector does not run all the time
// near the limit, and BufferBytes goes to buffers.
func CacheMemory(budget int64) int64 {
	return max(budget-budget/4-BufferBytes, 0)
}

// cgroupLimit returns the lowest memory.max of the process's cgroup and
// the cgroups above it, and false if none has a limit.
func cgroupLimit(root fs.FS) (int64, bool, error) {
	b, err := fs.ReadFile(root, "proc/self/cgroup")
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// In cgroup v2 the process's line is "0::/path".
	var group string
	found := false
	for line := range strings.Lines(string(b)) {
		if p, ok := strings.CutPrefix(strings.TrimSpace(line), "0::"); ok {
			group, found = p, true
		}
	}
	if !found {
		return 0, false, nil
	}
	var limit int64
	ok := false
	for dir := path.Clean("/" + group); ; dir = path.Dir(dir) {
		b, err := fs.ReadFile(root, path.Join("sys/fs/cgroup", dir, "memory.max"))
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return 0, false, err
		case string(bytes.TrimSpace(b)) != "max":
			v, err := strconv.ParseInt(string(bytes.TrimSpace(b)), 10, 64)
			if err != nil {
				return 0, false, fmt.Errorf("node: memory.max of cgroup %s: %w", dir, err)
			}
			if !ok || v < limit {
				limit, ok = v, true
			}
		}
		if dir == "/" {
			return limit, ok, nil
		}
	}
}

// hostMemory returns MemTotal from /proc/meminfo.
func hostMemory(root fs.FS) (int64, error) {
	b, err := fs.ReadFile(root, "proc/meminfo")
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(b)) {
		rest, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		kb, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), "kB")), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("node: MemTotal in /proc/meminfo: %w", err)
		}
		return kb << 10, nil
	}
	return 0, errors.New("node: no MemTotal in /proc/meminfo")
}
