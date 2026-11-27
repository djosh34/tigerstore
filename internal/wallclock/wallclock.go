// Package wallclock reads host time for diagnostics, never for simulation
// decisions. In gosim, time's normal clock stands still during a search.
package wallclock

import "time" //gosim:notranslate

// Now returns host Unix nanoseconds. Keeping the boundary an integer avoids
// mixing the host's time types with gosim's translated time types.
func Now() int64 { return time.Now().UnixNano() }
