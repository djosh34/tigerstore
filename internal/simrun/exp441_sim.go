//go:build sim

package simrun

import (
	"io"

	"github.com/jellevandenhooff/gosim"
)

// Mark writes msg with the real time to the real stderr (#441).
func Mark(msg string) { gosim.RealMark(msg) }

// RealCreate creates a file on the real disk, nil if it fails (#441).
func RealCreate(path string) io.WriteCloser {
	f := gosim.RealCreate(path)
	if f == nil {
		return nil
	}
	return f
}

// ProfileNext starts the next window of the run's CPU profile (#473).
func ProfileNext(tag string) { gosim.RealProfileNext(tag) }

// RealNanotime is the real clock, outside the simulation (#473).
func RealNanotime() int64 { return gosim.RealNanotime() }
