//go:build !sim

package behavior_test

import (
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestWriteArtifact(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestWriteArtifact", Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if run.Failed || string(run.Artifacts["artifact.txt"]) != "hello" {
		t.Errorf("got failed %v, artifacts %q", run.Failed, run.Artifacts)
	}
}

func TestUnimplementedHook(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestUnimplementedHook", Seed: 1, ExtraEnv: []string{"TESTINGFAIL=1"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "unimplemented hook: reflect.MapOf"; !run.Failed || run.Err != want {
		t.Errorf("got failed %v, err %q; want err %q", run.Failed, run.Err, want)
	}
}

func TestFatalUnlock(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestFatalUnlock", Seed: 1, ExtraEnv: []string{"TESTINGFAIL=1"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "fatal error: sync: unlock of unlocked mutex"; !run.Failed || run.Err != want {
		t.Errorf("got failed %v, err %q; want err %q", run.Failed, run.Err, want)
	}
}

func TestEmbedBytesPerRun(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	for range 2 {
		run, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestEmbedBytesPerRun", Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		if run.Failed {
			t.Errorf("a run failed: %s", run.LogOutput)
		}
	}
}

func TestMaphashComparable(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	run, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestMaphashComparable", Seed: 1, ExtraEnv: []string{"TESTINGFAIL=1"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "unimplemented hook: hash/maphash.comparableHashUnimplemented"; !run.Failed || run.Err != want {
		t.Errorf("got failed %v, err %q; want err %q\n%s", run.Failed, run.Err, want, run.LogOutput)
	}
}
