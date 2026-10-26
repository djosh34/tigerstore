//go:build !race

package gosimruntime

// RaceToken is a zero overhead version of RaceToken for non-race builds.  It
// does not do anything.
type RaceToken struct{}

func MakeRaceToken() RaceToken {
	return RaceToken{}
}

func EmptyRaceToken() RaceToken {
	return RaceToken{}
}

func (t RaceToken) Empty() bool {
	return true
}

func (t RaceToken) Release() {
}

func (r RaceToken) Acquire() {
}

// NoraceAppend is a zero overhead version of NoraceAppend. It wraps append.
func NoraceAppend[A any](to []A, from ...A) []A {
	return append(to, from...)
}

// NoraceCopy is a zero overhead version of NoraceCopy. It shallowly copies a
// slice.
func NoraceCopy[A any](a []A) []A {
	if a == nil {
		return nil
	}
	return append([]A(nil), a...)
}
