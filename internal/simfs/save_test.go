package simfs

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

// A saved disk loads as the disk a power loss leaves: its durable files,
// holes and all, and directories, not what was never synced, and it names
// its next file as the durable copy does. A disk whose pages are in a
// Store saves them too (#794).
func TestASavedDiskLoadsAsItsDurableCopy(t *testing.T) {
	for _, store := range []bool{false, true} {
		d := New()
		if store {
			d.KeepIn(&memStore{}, 2)
		}
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(d.Mkdir("wal"))
		f, err := d.Create("wal/ring")
		must(err)
		// A page at 0 and one 10 pages on: the pages between are a hole.
		_, err = f.WriteAt(bytes.Repeat([]byte("r"), 100), 0)
		must(err)
		_, err = f.WriteAt([]byte("tail"), 10*PageBytes)
		must(err)
		must(f.Sync())
		g, err := d.Create("ckpt")
		must(err)
		_, err = g.Write(bytes.Repeat([]byte("c"), 3*PageBytes))
		must(err)
		must(g.Sync())
		must(d.SyncDir())
		must(d.SyncDirOf("wal"))
		// Never synced: neither the write nor the file is durable.
		_, err = g.WriteAt([]byte("lost"), 0)
		must(err)
		_, err = d.Create("unsynced")
		must(err)

		var buf bytes.Buffer
		must(d.Save(&buf))
		loaded, err := Load(bytes.NewReader(buf.Bytes()), nil, 0)
		must(err)
		// Loaded into a Store, it keeps only its cache's pages in memory.
		ms := &memStore{}
		kept, err := Load(bytes.NewReader(buf.Bytes()), ms, 1)
		must(err)
		inMemory := 0
		for _, f := range kept.durable {
			for _, pg := range f.durable.blocks {
				if pg != nil && pg.b != nil {
					inMemory++
				}
			}
		}
		if inMemory > 1 || len(ms.slots) == 0 {
			t.Errorf("store %v: a disk loaded with a cache of 1 holds %d pages in memory, %d in the store", store, inMemory, len(ms.slots))
		}
		for _, name := range kept.DurableNames() {
			if !bytes.Equal(kept.DurableBytes(name), d.Durable().DurableBytes(name)) {
				t.Errorf("store %v: %s loaded into a store reads otherwise", store, name)
			}
		}
		want := d.Durable()
		if got, w := loaded.DurableNames(), want.DurableNames(); !slices.Equal(got, w) {
			t.Fatalf("store %v: files %v, want %v", store, got, w)
		}
		for _, name := range want.DurableNames() {
			if !bytes.Equal(loaded.DurableBytes(name), want.DurableBytes(name)) {
				t.Errorf("store %v: %s differs", store, name)
			}
		}
		if !loaded.IsDir("wal") || loaded.Bytes() != want.Bytes() || loaded.Size() != want.Size() {
			t.Errorf("store %v: wal dir %v, %d bytes in %d, want %d in %d", store, loaded.IsDir("wal"), loaded.Bytes(), loaded.Size(), want.Bytes(), want.Size())
		}
		if loaded.files != want.files || loaded.names["wal/ring"].id != want.names["wal/ring"].id {
			t.Errorf("store %v: file count %d and ring id %d, want %d and %d", store, loaded.files, loaded.names["wal/ring"].id, want.files, want.names["wal/ring"].id)
		}
		// The loaded files hold pages where the durable ones do, and
		// holes where they have holes.
		for _, name := range want.DurableNames() {
			held := func(p pages) []bool {
				var out []bool
				for _, pg := range p.blocks {
					out = append(out, pg != nil)
				}
				return out
			}
			if got, w := held(loaded.names[name].durable), held(want.names[name].durable); !slices.Equal(got, w) {
				t.Errorf("store %v: %s holds pages %v, want %v", store, name, got, w)
			}
		}
		// Saved again, the loaded disk gives the same bytes.
		var again bytes.Buffer
		must(loaded.Save(&again))
		if !bytes.Equal(again.Bytes(), buf.Bytes()) {
			t.Errorf("store %v: a loaded disk saves to other bytes", store)
		}
		for cut := range buf.Len() - 1 {
			if _, err := Load(bytes.NewReader(buf.Bytes()[:cut]), nil, 0); !errors.Is(err, ErrSaved) {
				t.Fatalf("store %v: a save cut at %d of %d bytes loaded: %v", store, cut, buf.Len(), err)
			}
		}
	}
}

// memStore is a Store in memory.
type memStore struct{ slots [][]byte }

func (m *memStore) Put(_ any, p []byte) (int64, error) {
	m.slots = append(m.slots, bytes.Clone(p))
	return int64(len(m.slots) - 1), nil
}

func (m *memStore) Get(off int64, p []byte) error {
	copy(p, m.slots[off])
	return nil
}
