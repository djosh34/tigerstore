/*
Gosim is a tool for working with gosim simulation testing framework.

Usage: gosim <command> [arguments]

The commands are:

	test           test packages
	debug          debug a test in a package
	build-tests    build packages for metatesting
	translate      translate packages
	help           print this help

The 'translate' command:

Usage: gosim translate [-race] [packages]

The translate command translates packages without running the resulting code.
The output will be placed in <module root>/.gosim/translated. Translation
is cached by package, so re-running translate after modifying some files
in the current package should be fast.

Packages should be listed as if they were arguments to 'go test' or 'go build'
command. All listed packages must be part of the current module. The current
module must have a dependency on gosim by importing
'github.com/jellevandenhooff/gosim' somewhere in the code.

Translate translates code with specific build flags sets. The GOOS is fixed to
linux, and the GOARCH is the one used to compile translate.

The -race flag translates all code with the race build tag set.

The 'test' command:

Usage: gosim test [-race] [-run=...] [-seeds=...] [-v] [packages]

The test command translates and runs tests for the specified packages.  It first
invokes translate, and then invokes 'go test' on the translated code, passing
through the -run and -v flags.

The -seeds flag specifies what seeds to run the tests on as a comma-separated
list of seeds and ranges, such as -seeds=1,2,5-10. The default is -seeds=1.

The 'debug' command:

Usage: gosim debug [-race] [-headless] -package=[package] -test=[test] [-seed=...] -step=[step]

The debug command translates and runs a specific test using the delve debugger.
It first invokes translate, and then runs 'dlv test' on the specific test. The
-step flag is the step to pause at as seen in the logs from running 'gosim test'.

The -seed flag is the seed to run. The default is -seed=1.

The -headless flag optionally runs delve in headless mode for use with an
external interface like an IDE.

The 'build-tests' command:

Usage: gosim build-tests [-race] [packages]

The build-tests command translates and then builds tests for use with the
metatesting package. Metatesting in a cached go test run requires pre-building
tests; see the metatesting package documentation for details.
*/
package main
