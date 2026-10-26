package cache_test

import (
	"path"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim/cmd/gosim/internal/translate/cache"
)

func mustMiss(t *testing.T, db *cache.DB, key string) {
	t.Helper()
	if _, err := db.Get(key); err != cache.ErrNoSuchKey {
		t.Errorf("get %s: unexpected error %s, expected %s", key, err, cache.ErrNoSuchKey)
	}
}

func mustGet(t *testing.T, db *cache.DB, key string, value string) {
	t.Helper()
	blob, err := db.Get(key)
	if err != nil {
		t.Errorf("get %s: unexpected error %s", key, err)
		return
	}
	if string(blob) != value {
		t.Errorf("get %s: got %s, expected %s", key, string(blob), value)
	}
}

func TestDB(t *testing.T) {
	dir := t.TempDir()

	file := path.Join(dir, "cache.sqlite3")

	db, err := cache.NewDB(file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	timestamp := time.Now()

	mustMiss(t, db, "foo")

	if err := db.Put("foo", timestamp, []byte("hello")); err != nil {
		t.Errorf("put foo: unexpected error %s", err)
	}
	mustGet(t, db, "foo", "hello")

	if err := db.Put("bar", timestamp.Add(2*time.Hour), []byte("goodbye")); err != nil {
		t.Errorf("put bar: unexpected error %s", err)
	}
	mustGet(t, db, "bar", "goodbye")

	if err := db.Clean(timestamp); err != nil {
		t.Errorf("clean: unexpected error %s", err)
	}
	mustGet(t, db, "foo", "hello")
	mustGet(t, db, "bar", "goodbye")

	if err := db.Clean(timestamp.Add(time.Hour)); err != nil {
		t.Errorf("clean: unexpected error %s", err)
	}
	mustMiss(t, db, "foo")
	mustGet(t, db, "bar", "goodbye")

	if err := db.Touch("bar", timestamp.Add(4*time.Hour)); err != nil {
		t.Errorf("touch bar: unexpected error %s", err)
	}
	if err := db.Clean(timestamp.Add(3 * time.Hour)); err != nil {
		t.Errorf("clean: unexpected error %s", err)
	}
	mustGet(t, db, "bar", "goodbye")

	if err := db.Clean(timestamp.Add(5 * time.Hour)); err != nil {
		t.Errorf("clean: unexpected error %s", err)
	}
	mustMiss(t, db, "bar")
}
