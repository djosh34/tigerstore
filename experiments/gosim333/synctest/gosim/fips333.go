package go123

import "github.com/jellevandenhooff/gosim/gosimruntime"

// Go 1.27.2 src/runtime/runtime1.go:776-783: indicator is per-goroutine state.
// Store by simulated goroutine ID, NOT by host goroutine or process-global byte.
var fips333Indicators = map[int]uint8{}

func CryptoInternalFips140_getIndicator() uint8 {
 gosimruntime.Port333Reached("crypto/internal/fips140.getIndicator")
 return fips333Indicators[gosimruntime.GetGoroutine()]
}
func CryptoInternalFips140_setIndicator(value uint8) {
 gosimruntime.Port333Reached("crypto/internal/fips140.setIndicator")
 fips333Indicators[gosimruntime.GetGoroutine()] = value
}
