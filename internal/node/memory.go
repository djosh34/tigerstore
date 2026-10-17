package node

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
)

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

// GoMemory returns the Go memory limit for a budget that came from
// from. A cgroup charges the node for more than Go's memory: the log
// segments it maps, the page cache of its writes and kernel memory. With
// Go's limit at the whole budget, a node in a cgroup of that size used
// all of it, and its cgroup killed it under a burst of writes (#202). So
// an eighth of the budget stays for those pages, unless GOMEMLIMIT gave
// the budget, which then stays the Go limit, as the user set it.
func GoMemory(budget int64, from string) int64 {
	if from == "GOMEMLIMIT" {
		return budget
	}
	return budget - budget/8
}

// ProcessMemory returns what the whole process may use out of the Go
// memory limit, its value cache included (core.Config.MemoryBytes): a
// quarter of the limit stays free, so that the garbage collector does not
// run all the time near it.
func ProcessMemory(limit int64) int64 {
	return limit - limit/4
}

// memoryGauge measures what the process uses besides the value cache,
// from the Go runtime's metrics, for core.Node.SetOtherMemory. The heap
// counts as the last collection found it live: garbage that the next one
// frees does not count, or the cache would shrink as garbage builds up
// and grow again after every collection. Memory outside the heap's
// objects, such as stacks, the runtime's own and unused heap spans,
// counts as the runtime holds it.
type memoryGauge struct {
	samples []metrics.Sample
	// gcs is the count of finished collections at the last read, cache
	// the cache's bytes then, and other the result as of the last
	// collection.
	gcs          uint64
	cache, other int64
}

func newMemoryGauge() *memoryGauge {
	names := []string{
		"/gc/cycles/total:gc-cycles",
		"/gc/heap/live:bytes",
		"/memory/classes/total:bytes",
		"/memory/classes/heap/released:bytes",
		"/memory/classes/heap/free:bytes",
		"/memory/classes/heap/objects:bytes",
	}
	g := &memoryGauge{samples: make([]metrics.Sample, len(names))}
	for i, name := range names {
		g.samples[i].Name = name
	}
	return g
}

// read returns what the process uses besides a value cache that holds
// cache bytes now, and whether a collection finished since the last
// read. The live heap changes only when a collection finishes, and it
// held the cache as it was then: somewhere between its size after the
// last read (shrunk) and now. The smaller of the two counts, so that
// bytes the cache let go of before the collection never count as room
// (an overestimate of the rest only shrinks the cache).
func (g *memoryGauge) read(cache int64) (int64, bool) {
	metrics.Read(g.samples)
	v := func(i int) int64 { return int64(g.samples[i].Value.Uint64()) }
	gcs := g.samples[0].Value.Uint64()
	fresh := gcs != g.gcs
	if fresh {
		outside := v(2) - v(3) - v(4) - v(5)
		g.gcs, g.other = gcs, max(v(1)+outside-min(g.cache, cache), 0)
	}
	g.cache = cache
	return g.other, fresh
}

// shrunk records the cache's size once the core fitted it to the last
// read.
func (g *memoryGauge) shrunk(cache int64) { g.cache = min(g.cache, cache) }

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
