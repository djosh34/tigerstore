//go:build sim

package node

import "testing"

// bubble runs f directly: gosim already simulates time and scheduling.
func bubble(t *testing.T, f func(*testing.T)) { f(t) }
