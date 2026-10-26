//go:build !sim

package simrun

import (
	"fmt"
	"io"
	"os"
	"time"
)

var markStart = time.Now()
var realClock = make(chan chan time.Time)

func init() {
	go func() {
		for ch := range realClock {
			ch <- time.Now()
		}
	}()
}
func realNow() time.Time { ch := make(chan time.Time); realClock <- ch; return <-ch }

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
