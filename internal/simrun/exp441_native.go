//go:build !sim

package simrun

import (
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"time"
)

var markStart = realNow()

func realNow() time.Time {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_REALTIME, &ts); err != nil {
		panic(err)
	}
	return time.Unix(ts.Sec, ts.Nsec)
}

// Mark writes msg with the real time to stderr (#441).
func Mark(msg string) {
	fmt.Fprintf(os.Stderr, "REALMARK t=%.3f unix=%d native %s\n", realNow().Sub(markStart).Seconds(), realNow().UnixNano(), msg)
}

// RealCreate creates a file, nil if it fails (#441).
func RealCreate(path string) io.WriteCloser {
	f, err := os.Create(path)
	if err != nil {
		return nil
	}
	return f
}
