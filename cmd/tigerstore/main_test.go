package main

import (
	"slices"
	"testing"
)

func TestPeerCertAllowedFlags(t *testing.T) {
	base := []string{"--name", "n1", "--initial-cluster", "n1=https://127.0.0.1:2380"}
	f, err := parse(append(base, "--peer-cert-allowed-cn", "old", "--peer-cert-allowed-cn", "node1,node2"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"node1", "node2"}; !slices.Equal(f.peerAllowedCNs, want) {
		t.Errorf("allowed CNs = %q, want %q", f.peerAllowedCNs, want)
	}
	f, err = parse(append(base, "--peer-cert-allowed-hostname", "a.example,10.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.example", "10.0.0.1"}; !slices.Equal(f.peerAllowedHostnames, want) {
		t.Errorf("allowed hostnames = %q, want %q", f.peerAllowedHostnames, want)
	}
	if _, err := parse(append(base, "--peer-cert-allowed-cn", "node1", "--peer-cert-allowed-hostname", "a.example")); err == nil {
		t.Error("both flags set: no error")
	}
}
