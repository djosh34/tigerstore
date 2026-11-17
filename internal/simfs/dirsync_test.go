package simfs

import (
	"math/rand/v2"
	"testing"
)

// A directory's sync makes its own names durable and no other's: a
// rename in ckpt/ that only a sync of wal/ followed can be lost to a
// crash, and one that a sync of ckpt/ followed cannot (#689's area-1
// review, finding 25).
func TestADirectorysSyncMakesItsOwnNamesDurable(t *testing.T) {
	setup := func() *Disk {
		d := New()
		for _, name := range []string{"ckpt/CURRENT.tmp", "wal/0.seg"} {
			h, err := d.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		if err := d.Rename("ckpt/CURRENT.tmp", "ckpt/CURRENT"); err != nil {
			t.Fatal(err)
		}
		return d
	}
	lost := 0
	for seed := range uint64(64) {
		d := setup()
		if err := d.SyncDirOf("wal"); err != nil {
			t.Fatal(err)
		}
		d.Crash(rand.New(rand.NewPCG(seed, 1)))
		if _, err := d.Open("ckpt/CURRENT"); err != nil {
			lost++
		}
	}
	if lost == 0 {
		t.Fatal("a sync of wal/ made a rename in ckpt/ durable in 64 crashes of 64")
	}
	for seed := range uint64(64) {
		d := setup()
		if err := d.SyncDirOf("ckpt"); err != nil {
			t.Fatal(err)
		}
		d.Crash(rand.New(rand.NewPCG(seed, 1)))
		if _, err := d.Open("ckpt/CURRENT"); err != nil {
			t.Fatalf("crash %d lost a rename its directory's sync made durable: %v", seed, err)
		}
	}
}
