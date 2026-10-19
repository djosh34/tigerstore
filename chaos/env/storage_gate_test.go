package env

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestStorageGate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CHAOS_STORAGE_GATE", dir)
	if err := os.WriteFile(filepath.Join(dir, "arm"), []byte("checkpoint-sync"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := StorageGate("rotation-rename"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- StorageGate("checkpoint-sync") }()
	if !waitFor(time.Second, func() bool { _, err := os.Stat(filepath.Join(dir, "hit")); return err == nil }) {
		t.Fatal("no hit")
	}
	select {
	case err := <-done:
		t.Fatalf("gate returned before release: %v", err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), []byte("eio"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, syscall.EIO) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release did not wake gate")
	}
	if err := StorageGate("checkpoint-sync"); err != nil {
		t.Fatalf("gate fired twice: %v", err)
	}
}
