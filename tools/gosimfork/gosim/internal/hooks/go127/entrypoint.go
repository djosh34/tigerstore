package go127

import (
	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/simulation"
	"github.com/jellevandenhooff/gosim/internal/simulation/syscallabi"
	"github.com/jellevandenhooff/gosim/internal/testing"
)

func Runtime() gosimruntime.Runtime {
	return runtimeImpl{}
}

type runtimeImpl struct{}

var _ gosimruntime.Runtime = runtimeImpl{}

func (r runtimeImpl) Run(fn func()) {
	simulation.Runtime(fn)
}

func (r runtimeImpl) Setup() {
	syscallabi.Setup()
}

func (r runtimeImpl) TestEntrypoint(match string, skip string, tests []gosimruntime.Test) bool {
	return testing.Entrypoint(match, skip, tests)
}
