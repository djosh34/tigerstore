package sim

import (
	"os"
	"strconv"
)

// exp485Duration is the length in seconds of the phase with faults that
// EXP485_DURATION sets for every DefaultConfig (research #485, exp branch
// only), 0 when it is not set.
func exp485Duration() int64 {
	d, err := strconv.ParseInt(os.Getenv("EXP485_DURATION"), 10, 64)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}
