package gosimruntime

import "errors"

// ErrUnimplemented ends a run that called a hook gosim does not implement.
var ErrUnimplemented = makeConstErr(errors.New("unimplemented hook"))

// Unimplemented ends the current run with ErrUnimplemented and logs name.
//
// Every hook gosim does not implement calls Unimplemented: the bodies the
// translator generates for functions it has no implementation for, and the
// stubs written by hand in gosim's own packages. The translated program cannot
// recover from it, and it never returns.
func Unimplemented(name string) {
	getg().upcall(func() {
		gs := gs.get()
		gs.logger.slog.Error("unimplemented hook", "name", name)
		if gs.abortDetail == "" {
			gs.abortDetail = name
		}
	})
	SetAbortError(ErrUnimplemented)
}
