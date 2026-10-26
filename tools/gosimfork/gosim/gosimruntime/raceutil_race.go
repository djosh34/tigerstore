//go:build race

package gosimruntime

import (
	"unsafe"

	"github.com/jellevandenhooff/gosim/internal/race"
)

const debugRaceToken = false

// A RaceToken is a token that can be used to establish happens-before
// dependencies for go's race detector.
type RaceToken struct {
	elem *int
}

//go:norace
func MakeRaceToken() RaceToken {
	return RaceToken{
		// TODO: we could have a pool of... 1024 of these and randomly pick them?
		// "should" catch most problems?
		elem: new(int),
	}
}

//go:norace
func EmptyRaceToken() RaceToken {
	return RaceToken{
		elem: nil,
	}
}

//go:norace
func (t RaceToken) Empty() bool {
	return t.elem == nil
}

//go:norace
func (t RaceToken) Release() {
	if debugRaceToken && t.elem == nil {
		panic("nil token")
	}
	race.Release(unsafe.Pointer(t.elem))
}

//go:norace
func (t RaceToken) Acquire() {
	if debugRaceToken && t.elem == nil {
		panic("nil token")
	}
	race.Acquire(unsafe.Pointer(t.elem))
}

// NoraceAppend appends to a slice without triggering the race detector.
//
//go:norace
func NoraceAppend[A any](to []A, from ...A) []A {
	for _, x := range from {
		to = append(to, x)
	}
	return to
}

// NoraceCopy copies a slice without triggering the race detector.
func NoraceCopy[A any](a []A) []A {
	if a == nil {
		return nil
	}
	return NoraceAppend([]A(nil), a...)
}
