//go:build difftest

package difftest

import "testing"

// TestDowngrade needs a fresh WAL: etcd v3.7.2's storage migration panics
// while visiting unknown enum values submitted by TestBoundaries (#272).
// Retrying Status cannot recover a crashed server. Keep both sets of probes,
// but never run this against the server used by the boundary suite.
func TestDowngrade(t *testing.T) {
	runCases(t, "etcd-v3.7.2-downgrade.json", []testCase{{"downgrade", downgradeCase}})
}
