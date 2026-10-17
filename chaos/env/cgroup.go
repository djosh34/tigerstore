package env

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupRoot is the cgroup v2 hierarchy, and chaosGroup the parent of
// every node's group.
const (
	cgroupRoot = "/sys/fs/cgroup"
	chaosGroup = "chaos"
)

// Cgroup is the cgroup of one node's process.
type Cgroup struct {
	Path string
}

// NewCgroup makes cgroup name under chaos, with the cpu, memory and io
// controllers.
func NewCgroup(name string) (*Cgroup, error) {
	parent := filepath.Join(cgroupRoot, chaosGroup)
	if err := writeRoot(filepath.Join(cgroupRoot, "cgroup.subtree_control"), "+cpu +memory +io"); err != nil {
		return nil, err
	}
	if _, err := sudo("mkdir", "-p", parent); err != nil {
		return nil, err
	}
	if err := writeRoot(filepath.Join(parent, "cgroup.subtree_control"), "+cpu +memory +io"); err != nil {
		return nil, err
	}
	c := &Cgroup{Path: filepath.Join(parent, name)}
	if _, err := sudo("mkdir", "-p", c.Path); err != nil {
		return nil, err
	}
	return c, nil
}

// Add moves process pid into the group.
func (c *Cgroup) Add(pid int) error {
	return writeRoot(filepath.Join(c.Path, "cgroup.procs"), strconv.Itoa(pid))
}

// LimitCPU limits the group to percent of one CPU; 0 lifts the limit.
func (c *Cgroup) LimitCPU(percent int) error {
	v := "max 100000"
	if percent > 0 {
		v = fmt.Sprintf("%d 100000", percent*1000)
	}
	return writeRoot(filepath.Join(c.Path, "cpu.max"), v)
}

// LimitMemory limits the group's memory to bytes, with no swap.
func (c *Cgroup) LimitMemory(bytes int64) error {
	if err := writeRoot(filepath.Join(c.Path, "memory.max"), strconv.FormatInt(bytes, 10)); err != nil {
		return err
	}
	return writeRoot(filepath.Join(c.Path, "memory.swap.max"), "0")
}

// CgroupStats are the counters a fault on the group shows itself in.
type CgroupStats struct {
	// Throttled counts the periods the CPU limit held the group back.
	Throttled     int64 `json:"throttled"`
	ThrottledUsec int64 `json:"throttled_usec"`
	// OOMKills counts processes the memory limit killed; PeakBytes is
	// the most memory the group took.
	OOMKills  int64 `json:"oom_kills"`
	PeakBytes int64 `json:"peak_bytes"`
}

// Stats reads the group's counters.
func (c *Cgroup) Stats() (CgroupStats, error) {
	var st CgroupStats
	fields := func(file string) (map[string]int64, error) {
		b, err := os.ReadFile(filepath.Join(c.Path, file))
		if err != nil {
			return nil, err
		}
		m := map[string]int64{}
		for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
			if f := strings.Fields(line); len(f) == 2 {
				if m[f[0]], err = strconv.ParseInt(f[1], 10, 64); err != nil {
					return nil, fmt.Errorf("%s: %w", file, err)
				}
			}
		}
		return m, nil
	}
	cpu, err := fields("cpu.stat")
	if err != nil {
		return st, err
	}
	st.Throttled, st.ThrottledUsec = cpu["nr_throttled"], cpu["throttled_usec"]
	mem, err := fields("memory.events")
	if err != nil {
		return st, err
	}
	st.OOMKills = mem["oom_kill"]
	// memory.peak is there from Linux 5.19 on.
	b, err := os.ReadFile(filepath.Join(c.Path, "memory.peak"))
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if st.PeakBytes, err = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err != nil {
		return st, fmt.Errorf("memory.peak: %w", err)
	}
	return st, nil
}

// Close removes the group; its processes must be gone.
func (c *Cgroup) Close() error {
	_, err := sudo("rmdir", c.Path)
	if err == nil {
		return nil
	}
	if _, serr := os.Stat(c.Path); errors.Is(serr, fs.ErrNotExist) {
		// Gone already.
		return nil
	}
	return err
}
