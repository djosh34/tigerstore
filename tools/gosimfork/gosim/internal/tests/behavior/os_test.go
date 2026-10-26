package behavior_test

import (
	"os"
	"testing"
)

func TestGetpid(t *testing.T) {
	pid := os.Getpid()
	if pid == 0 {
		t.Error("got 0 pid")
	}
}

func TestHostname(t *testing.T) {
	name, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(name)
}
