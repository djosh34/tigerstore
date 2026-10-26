package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

func MathRand_runtime_rand() uint64 {
	return gosimruntime.Fastrand64()
}
