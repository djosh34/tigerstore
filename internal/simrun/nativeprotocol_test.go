//go:build !sim

package simrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/metatesting"
)

// TestStubMetatest is a tiny protocol responder, not a simulator. It
// writes a few image bytes and synthetic results for the real driver.
func TestStubMetatest(t *testing.T) {
	if os.Getenv("SIMRUN_PROTOCOL_FIXTURE") == "" {
		t.Skip("only used by the driver protocol fixtures")
	}
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	forkCalls := 0
	for {
		var config metatesting.RunConfig
		if err := decoder.Decode(&config); err != nil {
			if errors.Is(err, io.EOF) {
				os.Exit(0) // Do not append Go's test verdict to the protocol.
			}
			t.Fatal(err)
		}
		for _, path := range config.Outputs {
			if err := os.WriteFile(path, []byte("fixture image"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		hash := "fixture hash"
		if os.Getenv("SIMRUN_PROTOCOL_FIXTURE") == "fork-mismatch" {
			for _, env := range config.ExtraEnv {
				if env == "SIM_FORK=0" {
					forkCalls++
					if forkCalls == 2 {
						hash = "different replay hash"
					}
				}
			}
		}
		if _, err := fmt.Fprintln(os.Stderr, gosimruntime.MetatestResultMark); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Encode(metatesting.RunResult{Artifacts: map[string][]byte{
			"hashes": []byte(hash), "seed.json": []byte("1"),
		}}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDriverProtocolChild uses actual seed/image/fork driver functions,
// but all gosim requests go to TestStubMetatest's synthetic responder.
func TestDriverProtocolChild(t *testing.T) {
	mode := os.Getenv("SIMRUN_PROTOCOL_FIXTURE")
	if mode == "" {
		t.Skip("only used as a protocol test's subprocess")
	}
	if mode == "filtered" {
		for seed := uint64(1); seed <= 4; seed++ {
			seedSubtest(t, "TestCampaign", seed, false, func(*testing.T) {})
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("SIMRUN_STUB_EXE", executable)
	t.Setenv("GOSIMTOOL", "/bin/true") // The fake binary is already prepared.
	t.Setenv("SIM_OUT", filepath.Join(root, "out"))
	t.Setenv("SIM_FORK", "")
	t.Setenv("SIM_IMAGE_SHA256", "")
	if mode == "image" || mode == "0" {
		t.Setenv("SIM_FORK", mode)
	}
	if mode == "wrong-image-sha" {
		t.Setenv("SIM_IMAGE_SHA256", "not the fixture image's hash")
	}
	if err := os.WriteFile("go.mod", []byte("module example.test/fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Match gosim's prepared-binary layout. -test.count=1 makes it use
	// the no-op GOSIMTOOL rather than inspect a cached binary's .info.
	config := "linux_" + runtime.GOARCH
	if Race {
		config += "_race"
	}
	if len(tags) > 0 {
		config += "_" + strings.Join(tags, "_")
	}
	pkg, _ := pkgPath(t)
	pkg = strings.ReplaceAll(pkg, "/internal/", "/internal_/")
	path := filepath.Join(root, ".gosim", "metatest", config,
		"translated-"+strings.ReplaceAll(pkg, "/", "-")+".test")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec \"$SIMRUN_STUB_EXE\" -test.run=^TestStubMetatest$\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	seedSubtest(t, "TestCampaign", 390, false, func(t *testing.T) {
		imageAndForks(t, Run{Test: "TestCampaign", Seed: 390}, 4, mode == "fork-mismatch",
			func(uint64, int) bool { return false })
	})
}

func driverProtocolOutput(t *testing.T, mode, pattern string, wantFailure bool) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run="+pattern, "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "SIMRUN_PROTOCOL_FIXTURE="+mode)
	out, err := cmd.CombinedOutput()
	if (err != nil) != wantFailure || ctx.Err() != nil {
		t.Fatalf("fixture %s: err=%v, context=%v\n%s", mode, err, ctx.Err(), out)
	}
	return string(out)
}

func TestFilteredSeedHasNoVerdict(t *testing.T) {
	out := driverProtocolOutput(t, "filtered", "^TestDriverProtocolChild$/^1$", false)
	if !strings.Contains(out, "simrun: seed TestCampaign/1 PASS\n") {
		t.Fatalf("missing the executed seed's verdict:\n%s", out)
	}
	if strings.Count(out, "simrun: seed ") != 1 {
		t.Fatalf("filtered-out seeds received verdicts:\n%s", out)
	}
}

func TestPartialReplayHasNoWholeSeedVerdict(t *testing.T) {
	for _, mode := range []string{"image", "0"} {
		t.Run(mode, func(t *testing.T) {
			out := driverProtocolOutput(t, mode, "^TestDriverProtocolChild$", false)
			if !strings.Contains(out, "simrun: image TestCampaign/390 PASS\n") {
				t.Fatalf("fixture never completed its image:\n%s", out)
			}
			if mode == "0" && !strings.Contains(out, "simrun: fork TestCampaign/390/0 PASS\n") {
				t.Fatalf("fixture never completed its selected fork:\n%s", out)
			}
			if strings.Contains(out, "simrun: seed TestCampaign/390 PASS\n") {
				t.Fatalf("partial replay claimed a whole-seed PASS:\n%s", out)
			}
		})
	}
	out := driverProtocolOutput(t, "all", "^TestDriverProtocolChild$", false)
	if strings.Count(out, "simrun: fork ") != 4 || !strings.Contains(out, "simrun: seed TestCampaign/390 PASS\n") {
		t.Fatalf("a complete image and all four forks must report the whole seed:\n%s", out)
	}
}

func TestWrongImageSHAReportsImageFailure(t *testing.T) {
	out := driverProtocolOutput(t, "wrong-image-sha", "^TestDriverProtocolChild$", true)
	if !strings.Contains(out, "not the fixture image's hash") ||
		!strings.Contains(out, "simrun: image TestCampaign/390 FAIL\n") ||
		strings.Contains(out, "simrun: image TestCampaign/390 PASS\n") {
		t.Fatalf("fatal image validation did not produce an image FAIL:\n%s", out)
	}
}

func TestForkReplayMismatchReportsForkFailure(t *testing.T) {
	out := driverProtocolOutput(t, "fork-mismatch", "^TestDriverProtocolChild$", true)
	if !strings.Contains(out, "played out differently the second time") ||
		!strings.Contains(out, "simrun: fork TestCampaign/390/0 FAIL\n") ||
		strings.Contains(out, "simrun: fork TestCampaign/390/0 PASS\n") {
		t.Fatalf("replay mismatch did not produce a fork FAIL:\n%s", out)
	}
	if !strings.Contains(out, "simrun: fork TestCampaign/390/1 PASS\n") {
		t.Fatalf("an earlier fork's failure contaminated a later successful fork:\n%s", out)
	}
}
