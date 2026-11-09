package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Research #575 only: RESEARCH575_RATE sets the allocation profile's
// sample rate; RESEARCH575_LIMIT_DIR/<pid>, once it appears, holds bytes
// added to this process's Go memory limit. The value cache keeps the
// size it took from the budget at startup, so only the GC's headroom grows.
func init() {
	if s := os.Getenv("RESEARCH575_RATE"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		runtime.MemProfileRate = n
	}
	dir := os.Getenv("RESEARCH575_LIMIT_DIR")
	if dir == "" {
		return
	}
	go func() {
		path := filepath.Join(dir, strconv.Itoa(os.Getpid()))
		for {
			b, err := os.ReadFile(path)
			if err == nil {
				extra, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
				if err != nil {
					panic(err)
				}
				old := debug.SetMemoryLimit(-1)
				debug.SetMemoryLimit(old + extra)
				fmt.Fprintf(os.Stderr, "research575: Go memory limit %d -> %d\n", old, old+extra)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
}
