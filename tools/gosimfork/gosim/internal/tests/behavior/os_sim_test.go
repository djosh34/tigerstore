//go:build sim

package behavior_test

import (
	"os"
	"testing"

	"github.com/jellevandenhooff/gosim"
)

func TestMachineHostname(t *testing.T) {
	name, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	expected := "main"
	if name != expected {
		t.Errorf("bad name %q, expected %q", name, expected)
	}

	m := gosim.NewMachine(gosim.MachineConfig{
		Label: "hello123",
		MainFunc: func() {
			name, err := os.Hostname()
			if err != nil {
				t.Fatal(err)
			}
			expected := "hello123"
			if name != expected {
				t.Errorf("bad name %q, expected %q", name, expected)
			}
		},
	})

	m.Wait()
}

func TestGetpagesize(t *testing.T) {
	if pgsize := os.Getpagesize(); pgsize != 4096 {
		t.Fatalf("bad page size %d", pgsize)
	}
}

var globalHostname string

func init() {
	globalHostname, _ = os.Hostname()
}

func TestSyscallsDuringInit(t *testing.T) {
	// Test that os.Hostname() called in an init func can make syscalls.

	expected := "main"
	if globalHostname != expected {
		t.Errorf("bad name %q, expected %q", globalHostname, expected)
	}

	m := gosim.NewMachine(gosim.MachineConfig{
		Label: "hello123",
		MainFunc: func() {
			expected := "hello123"
			if globalHostname != expected {
				t.Errorf("bad name %q, expected %q", globalHostname, expected)
			}
		},
	})

	m.Wait()
}
