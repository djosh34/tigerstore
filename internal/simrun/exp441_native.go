//go:build !sim

package simrun

import (
	"fmt"
	"io"
	"os"
	"time"
)

var markStart = time.Now()

// Mark writes msg with the real time to stderr (#441).
func Mark(msg string) {
	fmt.Fprintf(os.Stderr, "REALMARK t=%.3f unix=%d native %s\n", time.Since(markStart).Seconds(), time.Now().UnixNano(), msg)
}

// RealCreate creates a file, nil if it fails (#441).
func RealCreate(path string) io.WriteCloser {
	f, err := os.Create(path)
	if err != nil {
		return nil
	}
	return f
}
