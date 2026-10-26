package go127

import (
	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func Maps_clone(m any) any {
	return gosimruntime.CloneMap(m)
}
