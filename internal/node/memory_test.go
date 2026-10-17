package node

import (
	"math"
	"runtime"
	"runtime/debug"
	"testing"
	"testing/fstest"
)

// TestMemoryBudget checks where a node's memory budget comes from: the
// flag, then GOMEMLIMIT, then the lowest memory.max of its cgroup and
// those above it, then the host's memory.
func TestMemoryBudget(t *testing.T) {
	// CI may run the tests with GOMEMLIMIT.
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(math.MaxInt64))
	meminfo := &fstest.MapFile{Data: []byte("MemTotal:       16384 kB\nMemFree:         1024 kB\n")}
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	cases := []struct {
		name string
		root fstest.MapFS
		flag int64
		want int64
		from string
	}{
		{"flag", fstest.MapFS{"proc/meminfo": meminfo}, 1 << 30, 1 << 30, "flag"},
		{"no cgroup", fstest.MapFS{"proc/meminfo": meminfo}, 0, 16 << 20, "host"},
		{"cgroup without a limit", fstest.MapFS{
			"proc/meminfo":                 meminfo,
			"proc/self/cgroup":             file("0::/a/b\n"),
			"sys/fs/cgroup/a/b/memory.max": file("max\n"),
			"sys/fs/cgroup/a/memory.max":   file("max\n"),
		}, 0, 16 << 20, "host"},
		{"lowest limit above", fstest.MapFS{
			"proc/meminfo":                 meminfo,
			"proc/self/cgroup":             file("0::/a/b\n"),
			"sys/fs/cgroup/a/b/memory.max": file("536870912\n"),
			"sys/fs/cgroup/a/memory.max":   file("268435456\n"),
		}, 0, 256 << 20, "cgroup"},
		{"own limit", fstest.MapFS{
			"proc/meminfo":                 meminfo,
			"proc/self/cgroup":             file("0::/a/b\n"),
			"sys/fs/cgroup/a/b/memory.max": file("134217728\n"),
			"sys/fs/cgroup/a/memory.max":   file("max\n"),
		}, 0, 128 << 20, "cgroup"},
		{"container root", fstest.MapFS{
			"proc/meminfo":             meminfo,
			"proc/self/cgroup":         file("0::/\n"),
			"sys/fs/cgroup/memory.max": file("1073741824\n"),
		}, 0, 1 << 30, "cgroup"},
		{"cgroup v1 only", fstest.MapFS{
			"proc/meminfo":     meminfo,
			"proc/self/cgroup": file("4:memory:/a\n"),
		}, 0, 16 << 20, "host"},
	}
	for _, c := range cases {
		got, from, err := MemoryBudget(c.root, c.flag)
		if err != nil || got != c.want || from != c.from {
			t.Errorf("%s: %d from %s, %v; want %d from %s", c.name, got, from, err, c.want, c.from)
		}
	}

	if _, _, err := MemoryBudget(fstest.MapFS{}, -1); err == nil {
		t.Error("a negative flag gave no error")
	}
	if _, _, err := MemoryBudget(fstest.MapFS{}, 0); err == nil {
		t.Error("no /proc/meminfo gave no error")
	}
	bad := fstest.MapFS{"proc/meminfo": meminfo, "proc/self/cgroup": file("0::/a\n"), "sys/fs/cgroup/a/memory.max": file("lots\n")}
	if _, _, err := MemoryBudget(bad, 0); err == nil {
		t.Error("a malformed memory.max gave no error")
	}

	debug.SetMemoryLimit(3 << 30)
	if got, from, err := MemoryBudget(fstest.MapFS{"proc/meminfo": meminfo}, 0); err != nil || got != 3<<30 || from != "GOMEMLIMIT" {
		t.Errorf("with a Go memory limit: %d from %s, %v", got, from, err)
	}
	if got, from, err := MemoryBudget(fstest.MapFS{}, 1<<30); err != nil || got != 1<<30 || from != "flag" {
		t.Errorf("flag and Go memory limit: %d from %s, %v", got, from, err)
	}
}

func TestGoMemory(t *testing.T) {
	for _, from := range []string{"flag", "cgroup", "host"} {
		if got := GoMemory(640<<20, from); got != 560<<20 {
			t.Errorf("GoMemory(640 MiB, %s) = %d, want 560 MiB", from, got)
		}
	}
	if got := GoMemory(3<<30, "GOMEMLIMIT"); got != 3<<30 {
		t.Errorf("GoMemory(3 GiB, GOMEMLIMIT) = %d, want 3 GiB", got)
	}
}

func TestProcessMemory(t *testing.T) {
	if got := ProcessMemory(560 << 20); got != 420<<20 {
		t.Errorf("ProcessMemory(560 MiB) = %d, want 420 MiB", got)
	}
}

// TestMemoryGauge checks that memory the heap holds counts once a
// collection saw it, less what the cache held then.
func TestMemoryGauge(t *testing.T) {
	g := newMemoryGauge()
	runtime.GC()
	base, fresh := g.read(0)
	if !fresh {
		t.Fatal("the first read saw no collection")
	}
	held := make([]byte, 64<<20)
	for i := range held {
		held[i] = 1
	}
	// The cache holds half of it, at this read and the next.
	g.read(32 << 20)
	runtime.GC()
	other, fresh := g.read(32 << 20)
	runtime.KeepAlive(held)
	if !fresh {
		t.Fatal("the read after a collection saw none")
	}
	if d := other - base; d < 28<<20 || d > 40<<20 {
		t.Errorf("64 MiB with 32 MiB of it in the cache added %d bytes, want about 32 MiB", d)
	}
}

// TestMemoryGaugeAfterEviction checks the gauge's arithmetic on fixed
// readings: 20 MiB besides a 32 MiB cache, which is dropped, collected
// and filled again between two reads, so its size is 32 MiB at both. Only
// its low mark, 0, may count as the cache's in the collection after the
// drop, which no longer saw it.
func TestMemoryGaugeAfterEviction(t *testing.T) {
	const mib = 1 << 20
	g := newMemoryGauge()
	g.take(1, 52*mib, 0, 32*mib)
	if other, _ := g.take(2, 52*mib, 0, 32*mib); other != 20*mib {
		t.Fatalf("with the cache in the live heap: %d bytes besides it, want 20 MiB", other)
	}
	if other, fresh := g.take(3, 20*mib, 0, 0); !fresh || other != 20*mib {
		t.Fatalf("after the drop: %d bytes besides it (fresh %v), want 20 MiB", other, fresh)
	}
	if other, fresh := g.take(3, 0, 0, 32*mib); fresh || other != 20*mib {
		t.Fatalf("without a collection: %d (fresh %v), want the last result", other, fresh)
	}
}
