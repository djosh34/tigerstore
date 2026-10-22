package sim

import (
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// startOnDamagedVersion starts node 2 of a membership run on node 2's disk
// of testdata/cold-84216, with bit 16 of its oldest segment's format
// version flipped, as a misread or a corruption can do: version 2 reads as
// 65538 (bit 18 on #388). set marks how the run damaged the disk. It
// returns the run, the node and the start's error; the run shuts down, and
// explains itself if t failed, at the end of the test.
func startOnDamagedVersion(t *testing.T, set func(*node)) (*Sim, *node, error) {
	t.Helper()
	cfg := Config{Seed: 388, Membership: true}
	s, err := newSim(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			explainSim(t, cfg, s)
		}
		s.shutdown()
	})
	n := s.nodes[1]
	n.disk = imageDisk(t, "node2")
	n.disk.FlipDurable("000000000000000c.wal", 10)
	s.traceDisk(n)
	set(n)
	return s, n, s.start(n)
}

// A segment's header has no checksum of its own, so a damaged format
// version reads as one this build does not know; recovery refuses the log,
// which is the safe stop for a disk the run damaged on purpose (#388). The
// simulator failed the run on it.
func TestDamagedVersionFences(t *testing.T) {
	if simrun.Drive(t, 388) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		_, n, err := startOnDamagedVersion(t, func(n *node) { n.corrupted = true })
		if err != nil {
			t.Fatal(err)
		}
		if !n.fenced {
			t.Fatalf("node 2 is not fenced: %s", n.why)
		}
	})
}

// TestMisreadVersionIsRefused is the same for a misread: the start refuses
// the log and the node starts again later.
func TestMisreadVersionIsRefused(t *testing.T) {
	if simrun.Drive(t, 388) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		s, n, err := startOnDamagedVersion(t, func(n *node) { n.misread = true })
		if err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		refused := slices.ContainsFunc(s.trace, func(l string) bool { return strings.Contains(l, "refused a misread log") })
		s.mu.Unlock()
		if !refused || n.fenced {
			t.Fatalf("node 2 did not refuse the misread log: fenced %v, %s", n.fenced, n.why)
		}
	})
}
