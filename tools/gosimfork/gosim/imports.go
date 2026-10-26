//go:build never

package gosim

// This file exists as a dependency for users of gosim. The CLI, through
// cmd/gosim/internal/translate, needs to be able to import all packages below when run
// _inside_ the user's module. By importing the gosim package their go.mod will
// get all the required dependencies.

// TODO: allow fixing these at a different, independent version? what if there
// are multiple go.mod because we run on multiple versions?

// The CLI lives in its own module, github.com/jellevandenhooff/gosim/cmd/gosim,
// so that its dependencies do not raise the versions in the user's go.mod.

import (
	// Packages listed in cmd/gosim/internal/translate.TranslatedRuntimePackages.
	_ "github.com/jellevandenhooff/gosim/internal/hooks/go127"
	_ "github.com/jellevandenhooff/gosim/internal/reflect"
	_ "github.com/jellevandenhooff/gosim/internal/simulation"
	_ "github.com/jellevandenhooff/gosim/internal/testing"
)
