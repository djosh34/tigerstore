package simfs_test

import (
	"math/rand/v2"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
)

func TestCrashKeepsOnlyDurableState(t *testing.T) {
	d := simfs.New()
	rng := rand.New(rand.NewPCG(1, 1))
	a, err := d.Create("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("synced")); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Create("b"); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "a" {
		t.Fatalf("names after crash = %v, want [a]", names)
	}
	f, err := d.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "synced" {
		t.Fatalf("content after crash = %q", data)
	}

	// A truncate below the synced size is undone by a crash.
	if err := f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	d.Crash(rng)
	f, err = d.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := f.ReadAll(); string(data) != "synced" {
		t.Fatalf("content after unsynced truncate and crash = %q", data)
	}
}

// TestCopiesKeepFileIDs checks that a file created on a copy of a disk
// gets an id no copied file has, so a trace tells them apart.
func TestCopiesKeepFileIDs(t *testing.T) {
	d := simfs.New()
	if _, err := d.Create("a"); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*simfs.Disk{"Durable": d.Durable(), "Copy": d.Copy()} {
		var lines []string
		c.Trace = func(s string) { lines = append(lines, s) }
		if _, err := c.Create("b"); err != nil {
			t.Fatal(err)
		}
		if len(lines) != 1 || lines[0] != "create b (file 2)" {
			t.Errorf("%s: creating b traced %q, want \"create b (file 2)\"", name, lines)
		}
	}
}
