package main

import (
	"slices"
	"testing"
)

// A node's own flag wins over a test workflow's ETCD_* variable of the
// same flag, which etcd's flags refuse to have both of (#751's CI); other
// variables stay.
func TestANodeFlagDropsItsEnvironmentVariable(t *testing.T) {
	env := []string{"PATH=/bin", "ETCD_UNSAFE_LOG_RING_BYTES=536870912", "ETCD_NAME=x"}
	got := nodeEnv(env, []string{"--name", "n1", "--unsafe-log-ring-bytes", "4194304"})
	if want := []string{"PATH=/bin"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := nodeEnv(env, []string{"--data-dir", "d"}); !slices.Equal(got, env) {
		t.Fatalf("got %v, want %v", got, env)
	}
}
