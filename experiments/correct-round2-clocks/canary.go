package main

import (
	"encoding/json"
	"os"
	"time"
)

func main() {
	origin := time.Now()
	enc := json.NewEncoder(os.Stdout)
	for {
		now := time.Now()
		if err := enc.Encode(map[string]int64{"realtime_ns": now.UnixNano(), "monotonic_elapsed_ns": time.Since(origin).Nanoseconds()}); err != nil {
			os.Exit(1)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
