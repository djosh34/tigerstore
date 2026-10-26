package behavior_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/sys/unix"

	"github.com/jellevandenhooff/gosim"
)

func setupRealDisk(t *testing.T) {
	if gosim.IsSim() {
		return
	}

	orig, err := os.Getwd()
	if err != nil {
		t.Error(err)
		return
	}

	tmp, err := os.MkdirTemp("", t.Name())
	if err != nil {
		t.Error(err)
		return
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(tmp); err != nil {
			panic(err)
		}
	})

	if err := os.Chdir(tmp); err != nil {
		t.Error(err)
		return
	}

	t.Cleanup(func() {
		if err := os.Chdir(orig); err != nil {
			panic(err)
		}
	})
}

func TestDiskBasic(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Sync(); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskTruncateGrow(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Truncate(10); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("hello\x00\x00\x00\x00\x00")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskTruncateShrink(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Truncate(3); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("hel")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskOpenTruncate(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := f.Write([]byte("goodbye")); err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("goodbye")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskSyncDir(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile(".", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	// XXX: you canc sync a O_RDONLY dir?
	if err := f.Sync(); err != nil {
		t.Error(err)
		return
	}
}

func TestDiskAfterClose(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	checkClosedError := func(err error, op string) {
		t.Helper()
		pathErr, ok := err.(*os.PathError)
		if !ok {
			t.Error("not a path error")
			return
		}
		if pathErr.Op != op {
			t.Errorf("expected op %q, got %q", op, pathErr.Op)
		}
		if pathErr.Err != os.ErrClosed {
			t.Errorf("expected os.ErrClosed, got %v", pathErr.Err)
		}
		if pathErr.Path != "hello" {
			t.Errorf("expected \"hello\", got %q", pathErr.Path)
		}
	}

	err = f.Close()
	checkClosedError(err, "close")
	slog.Info("return value of close after close", "err", err.Error())

	n, err := f.Read(nil)
	checkClosedError(err, "read")
	slog.Info("return value of read after close", "n", n, "err", err.Error())

	n, err = f.Write([]byte("hello"))
	checkClosedError(err, "write")
	slog.Info("return value of write after close", "n", n, "err", err.Error())

	err = f.Sync()
	checkClosedError(err, "sync")
	slog.Info("return value of sync after close", "err", err.Error())
}

func TestDiskWriteAtEndOfFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 3); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("helhello")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskWriteAtAfterEndOfFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 3); err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 3+5+3); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("\x00\x00\x00hello\x00\x00\x00hello")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskReadAtBasic(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	data := make([]byte, 5)
	n, err := f.ReadAt(data, 0)
	if err != nil {
		t.Error(err)
	}
	if n != 5 || !bytes.Equal(data, []byte("hello")) {
		t.Error("bad read", n, data)
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskReadAtEndOfFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	{
		data := make([]byte, 5)
		n, err := f.ReadAt(data, 3)
		if err != io.EOF {
			t.Error(err)
		}
		if n != 2 || !bytes.Equal(data, []byte("lo\x00\x00\x00")) {
			t.Error("bad read", n, data)
		}
	}

	{
		data := make([]byte, 5)
		n, err := f.ReadAt(data, 5)
		if err != io.EOF {
			t.Error(err)
		}
		if n != 0 || !bytes.Equal(data, []byte("\x00\x00\x00\x00\x00")) {
			t.Error("bad read", n, data)
		}
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskReadAtAfterEndOfFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("hello", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	data := make([]byte, 5)
	n, err := f.ReadAt(data, 10)
	if err != io.EOF {
		t.Error(err)
	}
	if n != 0 || !bytes.Equal(data, []byte("\x00\x00\x00\x00\x00")) {
		t.Error("bad read", n, data)
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskSeekBasic(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	ok := func(offset int64, whence int, expected int64) {
		t.Helper()

		got, err := f.Seek(offset, whence)
		if err != nil {
			t.Errorf("seek %d %d: %v", offset, whence, err)
		}
		if got != expected {
			t.Errorf("seek %d %d: got %d, expected %d", offset, whence, got, expected)
		}
	}

	bad := func(offset int64, whence int, expected error) {
		t.Helper()

		got, err := f.Seek(offset, whence)
		if err == nil || !errors.Is(err, expected) {
			t.Errorf("seek %d %d: got err %v, expected %v", offset, whence, err, expected)
		}
		if got != 0 {
			t.Errorf("seek %d %d: expected 0 after failure, got %d", offset, whence, got)
		}
	}

	// get current pos
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Error(err)
	}
	if pos != 0 {
		t.Error(pos)
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	// get current pos
	ok(0, io.SeekCurrent, 5)

	// seek to start
	ok(0, io.SeekStart, 0)
	ok(0, io.SeekCurrent, 0)

	// read from current pos
	buf := make([]byte, 5)
	if n, err := f.Read(buf); err != nil || n != 5 {
		t.Error(n, err)
	}
	if string(buf) != "hello" {
		t.Error(string(buf))
	}
	ok(0, io.SeekCurrent, 5)

	// seek to start
	ok(0, io.SeekStart, 0)
	ok(0, io.SeekCurrent, 0)

	// seek to end
	ok(0, io.SeekEnd, 5)
	ok(0, io.SeekCurrent, 5)

	// seek after end
	ok(5, io.SeekEnd, 10)
	ok(0, io.SeekCurrent, 10)

	// read in nowhere and stay
	if n, err := f.Read(buf); err != io.EOF || n != 0 {
		t.Error(n, err)
	}
	ok(0, io.SeekCurrent, 10)

	// writeat over current and read
	if n, err := f.WriteAt([]byte("world"), 8); err != nil || n != 5 {
		t.Error(n, err)
	}
	if n, err := f.Read(buf); err != nil || n != 3 {
		t.Error(n, err)
	}
	if string(buf[0:3]) != "rld" {
		t.Error(buf[0:3])
	}
	ok(0, io.SeekCurrent, 13)

	// seek from start and read
	ok(8, io.SeekStart, 8)
	if n, err := f.Read(buf); err != nil || n != 5 {
		t.Error(n, err)
	}
	if string(buf) != "world" {
		t.Error(buf)
	}
	ok(0, io.SeekCurrent, 13)

	// seek after end and write
	ok(2, io.SeekEnd, 15)
	ok(0, io.SeekCurrent, 15)
	if n, err := f.Write([]byte("goodbye")); err != nil || n != 7 {
		t.Error(err)
	}
	ok(0, io.SeekCurrent, 22)

	// invalid seek from start
	bad(-2, io.SeekStart, syscall.EINVAL)
	ok(0, io.SeekCurrent, 22)

	// invalid seek from current
	bad(-23, io.SeekCurrent, syscall.EINVAL)
	ok(0, io.SeekCurrent, 22)

	// invalid seek from end
	pos, err = f.Seek(-23, io.SeekEnd)
	bad(-23, io.SeekEnd, syscall.EINVAL)
	ok(0, io.SeekCurrent, 22)

	// ok negative seek
	ok(-2, io.SeekCurrent, 20)
	ok(0, io.SeekCurrent, 20)

	// ok negative seek
	ok(-5, io.SeekEnd, 17)
	ok(0, io.SeekCurrent, 17)
}

func TestDiskOpenMissingFile(t *testing.T) {
	setupRealDisk(t)

	_, err := os.OpenFile("foo", os.O_RDONLY, 0o644)
	if err == nil {
		t.Error(err)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("should be not exist")
	}
	slog.Info("error opening a non-existing file", "err", err.Error())
}

func TestDiskStatMissingFile(t *testing.T) {
	setupRealDisk(t)

	_, err := os.Stat("foo")
	if err == nil {
		t.Error(err)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("should be not exist")
	}
	slog.Info("error stat a non-existing file", "err", err.Error())
}

func TestDiskStatFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	fi, err := os.Stat("foo")
	if err != nil {
		t.Error(err)
		return
	}

	if fi.IsDir() {
		t.Error("should not be dir, not", fi.IsDir())
	}
	if fi.Name() != "foo" {
		t.Error("should be foo, not ", fi.Name())
	}
	expectedSize := int64(len("hello"))
	if gotSize := fi.Size(); gotSize != expectedSize {
		t.Errorf("bad size: got %d, expected %d", gotSize, expectedSize)
	}
}

func TestDiskStatFd(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	fi, err := f.Stat()
	if err != nil {
		t.Error(err)
		return
	}

	if fi.IsDir() {
		t.Error("should not be dir, not", fi.IsDir())
	}
	if fi.Name() != "foo" {
		t.Error("should be foo, not ", fi.Name())
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	fi2, err := os.Stat("foo")
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(fi, fi2) {
		t.Error("different")
	}
}

func TestDiskDeleteMissingFile(t *testing.T) {
	setupRealDisk(t)

	err := os.Remove("foo")
	if err == nil {
		t.Error(err)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("should be not exist", err)
	}
	slog.Info("error deleting a non-existing file", "err", err.Error())
}

func TestDiskDeleteFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	if _, err := os.Stat("foo"); err != nil {
		t.Error("missing file before delete", err)
		return
	}

	if err := os.Remove("foo"); err != nil {
		t.Error("remove failed", err)
		return
	}

	if _, err := os.Stat("foo"); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Error("delete did not work", err)
		return
	}
}

func TestDiskRenameFile(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	if _, err := os.Stat("foo"); err != nil {
		t.Error("missing file before rename", err)
		return
	}

	if err := os.Rename("foo", "bar"); err != nil {
		t.Error("rename failed", err)
		return
	}

	if _, err := os.Stat("foo"); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Error("file still there after rename", err)
		return
	}

	f, err = os.OpenFile("bar", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskRenameMissingFile(t *testing.T) {
	setupRealDisk(t)

	err := os.Rename("foo", "bar")
	if err == nil {
		t.Error(err)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("should be not exist")
	}
	slog.Info("error renaming a non-existing file", "err", err.Error())
}

func TestDiskRenameFileReplace(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	f, err = os.OpenFile("bar", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Write([]byte("old")); err != nil {
		t.Error(err)
		return
	}

	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	if _, err := os.Stat("foo"); err != nil {
		t.Error("missing file before rename", err)
		return
	}

	if _, err := os.Stat("bar"); err != nil {
		t.Error("missing file before rename", err)
		return
	}

	if err := os.Rename("foo", "bar"); err != nil {
		t.Error("rename failed", err)
		return
	}

	if _, err := os.Stat("foo"); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Error("file still there after rename", err)
		return
	}

	f, err = os.OpenFile("bar", os.O_RDONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Error("bad read", data)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}

type dirEntry struct {
	Name  string
	IsDir bool
}

func checkReadDir(t *testing.T, dir string, expected []dirEntry) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Error(err)
		return
	}

	var converted []dirEntry
	for _, entry := range entries {
		converted = append(converted, dirEntry{
			Name:  entry.Name(),
			IsDir: entry.IsDir(),
		})
	}

	if diff := cmp.Diff(expected, converted); diff != "" {
		t.Error(diff)
	}
}

func TestDiskReadDir(t *testing.T) {
	setupRealDisk(t)

	checkReadDir(t, ".", nil)

	f, err := os.OpenFile("foo", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "foo",
			IsDir: false,
		},
	})

	f, err = os.OpenFile("bar", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "bar",
			IsDir: false,
		},
		{
			Name:  "foo",
			IsDir: false,
		},
	})

	if err := os.Rename("bar", "zz"); err != nil {
		t.Error(err)
		return
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "foo",
			IsDir: false,
		},
		{
			Name:  "zz",
			IsDir: false,
		},
	})

	if err := os.Remove("foo"); err != nil {
		t.Error(err)
		return
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "zz",
			IsDir: false,
		},
	})
}

func TestMmapBasic(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Error(err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, 4096, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Munmap(data); err != nil {
			t.Error(err)
		}
	}()

	if got := string(data[0:5]); got != "hello" {
		t.Errorf("expected hello, got %q", got)
	}

	if _, err := f.WriteAt([]byte("goodbye"), 0); err != nil {
		t.Error(err)
	}

	if got := string(data[0:7]); got != "goodbye" {
		t.Errorf("expected goodbye, got %q", got)
	}

	if err := f.Truncate(2048); err != nil {
		t.Error(err)
	}
	n, err := f.WriteAt([]byte("straddle"), 2048-3)
	if n != 8 || err != nil {
		t.Errorf("tried to write, got %d %v", n, err)
	}
	if got := string(data[2048-3 : 2048-3+8]); got != "straddle" {
		t.Errorf("expected straddle, got %q", got)
	}

	if err := f.Truncate(1024); err != nil {
		t.Error(err)
	}

	if got := string(data[2048-3 : 2048-3+8]); got != "\x00\x00\x00\x00\x00\x00\x00\x00" {
		t.Errorf("expected zeroes, got %q", got)
	}
}

func TestDiskDirBasic(t *testing.T) {
	setupRealDisk(t)

	if err := os.Mkdir("foo", 0o755); err != nil {
		t.Error(err)
	}

	if err := os.WriteFile("foo/bar", []byte("baz"), 0o644); err != nil {
		t.Error(err)
	}

	bytes, err := os.ReadFile("foo/bar")
	if err != nil {
		t.Error(err)
	}
	if string(bytes) != "baz" {
		t.Errorf("unexpected read %q", string(bytes))
	}

	if _, err := os.ReadFile("foo/bar/baz"); !errors.Is(err, syscall.ENOTDIR) {
		t.Error(err)
	}
	if _, err := os.ReadFile("foo/bar/baz/bah"); !errors.Is(err, syscall.ENOTDIR) {
		t.Error(err)
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "foo",
			IsDir: true,
		},
	})

	checkReadDir(t, "foo", []dirEntry{
		{
			Name:  "bar",
			IsDir: false,
		},
	})

	before, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	if err := os.Chdir("foo"); err != nil {
		t.Error(err)
	}

	after, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	if filepath.Join(before, "foo") != after {
		t.Error(before, filepath.Join(before, "foo"), after)
	}

	if err := os.Rename("../foo", "../huh"); err != nil {
		t.Error(err)
	}

	after2, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	if filepath.Join(before, "huh") != after2 {
		t.Error(before, filepath.Join(before, "huh"), after2)
	}

	bytes, err = os.ReadFile("bar")
	if err != nil {
		t.Error(err)
	}
	if string(bytes) != "baz" {
		t.Errorf("unexpected read %q", string(bytes))
	}

	checkReadDir(t, ".", []dirEntry{
		{
			Name:  "bar",
			IsDir: false,
		},
	})

	if err := os.Chdir(".."); err != nil {
		t.Error(err)
	}

	if err := os.Remove("huh"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Error(err)
	}

	if err := os.Remove("huh/bar"); err != nil {
		t.Error(err)
	}

	if err := os.Remove("huh"); err != nil {
		t.Error(err)
	}
}

type mmapTester struct {
	file     *os.File
	expected []byte
	mmaps    map[string][]byte
}

func (f *mmapTester) truncate(t *testing.T, n int) {
	t.Logf("truncating %d", n)

	if err := f.file.Truncate(int64(n)); err != nil {
		t.Fatal(n)
	}
	if n > len(f.expected) {
		f.expected = append(f.expected, make([]byte, n-len(f.expected))...)
	}
	if n < len(f.expected) {
		f.expected = f.expected[:n]
	}
}

func (f *mmapTester) readAt(t *testing.T, pos, n int) {
	t.Logf("reading %d %d", pos, n)

	expected := make([]byte, n)
	if pos <= len(f.expected) {
		copy(expected, f.expected[pos:])
	}

	for name, mmap := range f.mmaps {
		t.Logf("checking mmap %s", name)

		if pos >= len(mmap) {
			t.Logf("out of range, skipping")
			continue
		}

		got := mmap[pos:min(pos+n, len(mmap))]
		if !bytes.Equal(expected[:len(got)], got) {
			t.Errorf("expected %v, got %v", expected, got)
		}
	}
}

func (f *mmapTester) writeAt(t *testing.T, pos, n int) {
	t.Logf("writing %d %d", pos, n)

	b := make([]byte, n)
	rand.Read(b)

	m, err := f.file.WriteAt(b, int64(pos))
	if err != nil || m != n {
		t.Errorf("expected ok write, got %d %v", m, err)
	}

	if pos+n > len(f.expected) {
		// append zeroes
		f.expected = append(f.expected, make([]byte, pos+n-len(f.expected))...)
	}
	copy(f.expected[pos:], b)
}

func (f *mmapTester) mmap(t *testing.T, name string, n int) {
	mmap, err := unix.Mmap(int(f.file.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	f.mmaps[name] = mmap
}

func (f *mmapTester) munmap(t *testing.T, name string) {
	if err := unix.Munmap(f.mmaps[name]); err != nil {
		t.Fatal(err)
	}
	delete(f.mmaps, name)
}

func TestMmapComplex(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	mt := &mmapTester{
		file:     f,
		expected: []byte{},
		mmaps:    make(map[string][]byte),
	}

	mt.readAt(t, 0, 10)
	mt.writeAt(t, 0, 10)
	mt.readAt(t, 0, 10)

	mt.mmap(t, "big", 4096)
	mt.mmap(t, "small", 1024)

	mt.readAt(t, 0, 10)
	mt.readAt(t, 0, 4096)

	mt.writeAt(t, 0, 1024)
	mt.readAt(t, 0, 1024)

	mt.munmap(t, "small")
	mt.mmap(t, "small", 1024)

	mt.writeAt(t, 0, 4096)
	mt.readAt(t, 0, 4096)

	mt.writeAt(t, 1024-20, 40)
	mt.readAt(t, 1024-128, 128)

	mt.writeAt(t, 4096-128, 256)
	mt.readAt(t, 4096-128, 256)

	mt.truncate(t, 4000)
	mt.readAt(t, 0, 4096)

	mt.truncate(t, 4096)
	mt.readAt(t, 4000, 96)
	mt.writeAt(t, 4000, 96)

	mt.writeAt(t, 4000, 200)
	mt.readAt(t, 4000, 200)
}

// open dir
// rename dir
// delete open file
// stat dir?
// stat size?

// fix duplication of writes in tests?

// things to test:
// - error codes for...
//   closed, eof, wrong mode
// - concurrent reads/writes (same handle, different handle)
// - read/write at end of file (writeat with offset at file, after file end)
// - read ending at file (err = nil or eof or both?)
// - ???
