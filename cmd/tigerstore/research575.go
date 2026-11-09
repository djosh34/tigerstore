package main

import (
	"os"
	"runtime"
	"strconv"
)

func init() {
	if s := os.Getenv("RESEARCH575_RATE"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		runtime.MemProfileRate = n
	}
}
