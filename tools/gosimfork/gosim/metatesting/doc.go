/*
Package metatesting is a package for writing normal go tests that invoke gosim
tests. Such metatests can run gosim tests with different seeds, assert that some
scenarios happen often or never, etc. With metatesting, gosim tests can be
integrated in a normal go test run, so that 'go test ./...' for a module can
cover both simulated and non-simulated tests.

# Using build constraints to combine gosim tests and metatests

The metatesting API does not work inside gosim. To combine gosim tests and metatests
in a single package, use the '//go:build sim' and '//go:build !sim' contraints
at the top of test files.

# Working with the go test cache

Metatesting has some unfortunate interactions with go test cache. To run
metatests using 'go test', the run needs either be non-cached or the metatests
need to be precompiled.

To run metatests with a non-cached 'go test' invocation, run 'go test -count=1
./package/with/metatests'. Then metatesting will build the gosim tests when
needed. This is convenient, but this metatest will not be cached.

To cache tests, the gosim tests need to be built before running the metatest
with 'gosim build-tests'.  If metatesting is used incorrectly it tries to give a
helpful error message. A simple way to run tests is to use a script that invoke
'gosim build-tests' before running 'go test':

	# build gosim test binaries
	go run github.com/jellevandenhooff/gosim/cmd/gosim build-tests pkgA pkgA/pkgB
	# then run tests
	go test ./...

Tests need to be precompiled when using the test cache because the test cache
should be invalidated when the gosim test changes. Normal go tests get
invalidated when their files change because the go tool knows which files are
compiled into the test. However, for a metatest that invokes 'gosim test' the go
tool does not know what files 'gosim test' accesses, and so does not invalidate
the cache if those files change. To sidestep those issues, run 'gosim
build-tests' before running 'go test'.

TODO: Support fuzzing.

TODO: Support parallelism.
*/
package metatesting
