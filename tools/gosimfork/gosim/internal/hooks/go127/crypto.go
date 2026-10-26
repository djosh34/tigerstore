package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

func CryptoInternalFips140_getIndicator() uint8 {
	return gosimruntime.FIPSIndicator()
}

func CryptoInternalFips140_setIndicator(indicator uint8) {
	gosimruntime.SetFIPSIndicator(indicator)
}

func CryptoFips140_isBypassed() bool {
	return gosimruntime.FIPSBypassed()
}

func CryptoFips140_setBypass() {
	gosimruntime.SetFIPSBypassed(true)
}

func CryptoFips140_unsetBypass() {
	gosimruntime.SetFIPSBypassed(false)
}

func CryptoInternalFips140depsTime_monoTime() int64 {
	return gosimruntime.Nanotime()
}

func CryptoInternalFips140_fatal(msg string) {
	gosimruntime.Fatal(msg)
}

func CryptoInternalSysrand_fatal(msg string) {
	gosimruntime.Fatal(msg)
}

func CryptoRand_fatal(msg string) {
	gosimruntime.Fatal(msg)
}

// boolToUint8 is a compiler intrinsic in the standard library. The compiler
// does not intrinsify the translated copy, whose body panics.
func CryptoInternalConstanttime_boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
