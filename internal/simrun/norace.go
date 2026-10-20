//go:build !race && !sim

package simrun

// Race reports whether the tests run natively under the race detector.
const Race = false
