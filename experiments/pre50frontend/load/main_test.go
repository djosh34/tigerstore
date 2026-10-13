package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBarrier(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := barrier(dir, "ready", "release"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready")); err != nil {
		t.Fatal(err)
	}
	if err := barrier("", "unused", "unused"); err != nil {
		t.Fatal(err)
	}
}

func TestBarrierWriteError(t *testing.T) {
	if err := barrier(filepath.Join(t.TempDir(), "missing"), "ready", "release"); err == nil {
		t.Fatal("missing directory accepted")
	}
}
