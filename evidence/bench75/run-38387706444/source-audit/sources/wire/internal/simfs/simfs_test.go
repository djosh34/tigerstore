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
