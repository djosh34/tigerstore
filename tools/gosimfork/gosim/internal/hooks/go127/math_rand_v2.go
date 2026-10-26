package go127

import "github.com/jellevandenhooff/gosim/gosimruntime"

func MathRandV2_runtime_rand() uint64 {
	return gosimruntime.Fastrand64()
}
