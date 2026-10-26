//go:build !sim

package behavior_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestTFatalAborts(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestTFatalAborts",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrTestFailed.Error() { // XXX: janky
		t.Error("expected test failed err")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"ERROR help",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestPanicAbortsOther(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestPanicAbortsOther",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrPaniced.Error() { // XXX: janky
		t.Error("expected paniced err")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"ERROR uncaught panic",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestPanic(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestPanic",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrPaniced.Error() { // XXX: janky
		t.Error("expected paniced err")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"ERROR uncaught panic",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestPanicInsideMachine(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestPanicInsideMachine",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrPaniced.Error() { // XXX: janky
		t.Error("expected paniced err")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"ERROR uncaught panic",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestTErrorDoesNotAbort(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestTErrorDoesNotAbort",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrTestFailed.Error() { // XXX: janky
		t.Error("expected test failed err")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"ERROR help",
		"INFO after",
		"INFO done",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestSimulationTimeout(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestSimulationTimeout",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrTimeout.Error() { // XXX: janky
		t.Error("expected timeout")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"INFO 30s",
		"INFO 1m30s",
		"INFO 2m30s",
		"INFO 3m30s",
		"INFO 4m30s",
		"INFO 5m30s",
		"INFO 6m30s",
		"INFO 7m30s",
		"INFO 8m30s",
		"INFO 9m30s",
		"ERROR test timeout",
	}); diff != "" {
		t.Error(diff)
	}
}

func TestSimulationTimeoutOverride(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)

	run, err := mt.Run(t, &metatesting.RunConfig{
		Test:     "TestSimulationTimeoutOverride",
		Seed:     1,
		ExtraEnv: []string{"TESTINGFAIL=1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !run.Failed {
		t.Error("expected failure")
	}
	if run.Err != gosimruntime.ErrTimeout.Error() { // XXX: janky
		t.Error("expected timeout")
	}

	if diff := cmp.Diff(metatesting.SimplifyParsedLog(metatesting.ParseLog(run.LogOutput)), []string{
		"INFO before",
		"INFO 30s",
		"INFO 1m30s",
		"INFO 2m30s",
		"INFO 3m30s",
		"INFO 4m30s",
		"ERROR test timeout",
	}); diff != "" {
		t.Error(diff)
	}
}
