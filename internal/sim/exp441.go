package sim

import (
	"fmt"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// mark writes a timing mark with the real time (#441).
func mark(format string, args ...any) { simrun.Mark(fmt.Sprintf(format, args...)) }
