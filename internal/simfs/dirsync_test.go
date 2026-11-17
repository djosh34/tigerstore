package simfs

import (
	"errors"
	"io/fs"
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
		for _, dir := range []string{"ckpt", "wal"} {
			if err := d.Mkdir(dir); err != nil {
				t.Fatal(err)
			}
		}
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

// A directory is made in one that is there, and a file only in one that
// is. Its creation is durable with a sync of the directory that holds
// it, and a crash that loses it loses every file in it, synced or not
// (#689's area-1 review, finding 25).
func TestADirectorysCreationIsDurableWithItsParentsSync(t *testing.T) {
	d := New()
	if _, err := d.Create("wal/0.seg"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a file created in a directory not made: %v", err)
	}
	if err := d.Mkdir("wal/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a directory made in one not made: %v", err)
	}
	if err := d.Mkdir("wal"); err != nil {
		t.Fatal(err)
	}
	if err := d.Mkdir("wal"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("a directory made twice: %v", err)
	}
	if _, err := d.Create("f"); err != nil {
		t.Fatal(err)
	}
	if err := d.Rename("f", "ckpt/f"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a rename into a directory not made: %v", err)
	}
	setup := func() *Disk {
		d := New()
		if err := d.Mkdir("wal"); err != nil {
			t.Fatal(err)
		}
		h, err := d.Create("wal/0.seg")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Write([]byte("synced")); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		// The file's name in wal/ is durable; wal/'s own is not yet.
		if err := d.SyncDirOf("wal"); err != nil {
			t.Fatal(err)
		}
		return d
	}
	lost := 0
	for seed := range uint64(64) {
		d := setup()
		d.Crash(rand.New(rand.NewPCG(seed, 1)))
		_, err := d.Open("wal/0.seg")
		if d.IsDir("wal") != (err == nil) {
			t.Fatalf("crash %d: wal/ there %v, its file opens with %v", seed, d.IsDir("wal"), err)
		}
		if err != nil {
			lost++
		}
	}
	if lost == 0 {
		t.Fatal("a directory whose creation no sync of its parent followed was kept in 64 crashes of 64")
	}
	for seed := range uint64(64) {
		d := setup()
		if err := d.SyncDirOf(""); err != nil {
			t.Fatal(err)
		}
		d.Crash(rand.New(rand.NewPCG(seed, 1)))
		h, err := d.Open("wal/0.seg")
		if err != nil {
			t.Fatalf("crash %d lost a directory its parent's sync made durable: %v", seed, err)
		}
		if b, err := h.ReadAll(); err != nil || string(b) != "synced" {
			t.Fatalf("crash %d: the file holds %q %v", seed, b, err)
		}
	}
}
