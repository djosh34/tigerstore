//go:build !sim

package nemesis_test

import (
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestMetaDeterministic(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	metatesting.CheckDeterministic(t, mt)
}

func TestMetaSeeds(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	metatesting.CheckSeeds(t, mt, 5)
}

func TestGosim(t *testing.T) {
	runner := metatesting.ForCurrentPackage(t)
	runner.RunAllTests(t)
}
