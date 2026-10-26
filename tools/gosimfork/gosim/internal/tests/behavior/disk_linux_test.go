package behavior_test

import (
	"os"
	"syscall"
	"testing"
)

func TestDiskFallocate(t *testing.T) {
	setupRealDisk(t)

	f, err := os.OpenFile("hello", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Error(err)
	}

	// fallocate less than current file shouldn't change anything
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, 3); err != nil {
		t.Error(err)
	}

	bytes, err := os.ReadFile("hello")
	if err != nil || string(bytes) != "hello" {
		t.Error(err, string(bytes))
	}

	// fallocate more than current file should add zeroes
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, 12); err != nil {
		t.Error(err)
	}

	bytes, err = os.ReadFile("hello")
	if err != nil || string(bytes) != "hello\x00\x00\x00\x00\x00\x00\x00" {
		t.Error(err, string(bytes))
	}

	if _, err := f.Write([]byte("world")); err != nil {
		t.Error(err)
	}

	bytes, err = os.ReadFile("hello")
	if err != nil || string(bytes) != "helloworld\x00\x00" {
		t.Error(err, string(bytes))
	}

	if _, err := f.Write([]byte("goodbye")); err != nil {
		t.Error(err)
	}

	bytes, err = os.ReadFile("hello")
	if err != nil || string(bytes) != "helloworldgoodbye" {
		t.Error(err, string(bytes))
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}
}
