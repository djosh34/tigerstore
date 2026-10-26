package gosimtool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/mod/modfile"

	"github.com/jellevandenhooff/gosim/internal/race"
)

const (
	Module          = "github.com/jellevandenhooff/gosim"
	OutputDirectory = ".gosim"
)

type BuildConfig struct {
	GOOS   string
	GOARCH string
	Race   bool
	// Tags are extra build tags, on top of the sim tag translate always sets.
	Tags []string
}

func (b BuildConfig) AsDirname() string {
	name := fmt.Sprintf("%s_%s", b.GOOS, b.GOARCH)
	if b.Race {
		name += "_race"
	}
	for _, tag := range b.Tags {
		name += "_" + tag
	}
	return name
}

type TranslateOutput struct {
	RootOutputDir string
	Packages      []string                        // TODO: combine all this in a struct? also include input name?
	Deps          map[string]map[string]time.Time // pkg -> path -> time
	// Unimplemented lists, sorted, the hooks in the translated code that
	// call gosimruntime.Unimplemented.
	Unimplemented []UnimplementedHook
}

// An UnimplementedHook is a function in translated code that fails through
// gosimruntime.Unimplemented.
type UnimplementedHook struct {
	// Name is the name passed to gosimruntime.Unimplemented, such as
	// "internal/cpu.getisar1".
	Name string
	// Symbol is the function's symbol in the compiled test binary.
	Symbol string
}

func FindGoMod() (string, *modfile.File, error) {
	baseDir, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}

	for i := 0; i < 20; i++ {
		p := path.Join(baseDir, "go.mod")
		bytes, err := os.ReadFile(p)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return "", nil, err
			}

			baseDir = path.Dir(baseDir)
			if baseDir != "." {
				continue
			}
			return "", nil, errors.New("could not find go.mod file, hit root")
		}

		file, err := modfile.Parse(p, bytes, nil)
		if err != nil {
			return "", nil, err
		}

		return p, file, nil
	}

	return "", nil, errors.New("could not find go.mod file, tried too many dirs")
}

func FindGoModDir() (string, error) {
	modFile, _, err := FindGoMod()
	if err != nil {
		return "", err
	}

	modDir := filepath.Dir(modFile)
	return modDir, nil
}

// MakeGoModForTest makes a go.mod and go.sum for tests that use the `gosim` CLI
// outside of the gosim module. It takes the go.mod from the gosim module strips
// it, and adds a `replace` directive pointing to the gosim module.
//
// Only dependencies that are listed in the original go.mod work.
func MakeGoModForTest(goSimModDir, testModDir string, requires []string) error {
	origModPath := filepath.Join(goSimModDir, "go.mod")
	origModBytes, err := os.ReadFile(origModPath)
	if err != nil {
		return err
	}

	origFile, err := modfile.Parse(origModPath, origModBytes, nil)
	if err != nil {
		return err
	}

	origSumBytes, err := os.ReadFile(filepath.Join(goSimModDir, "go.sum"))
	if err != nil {
		return err
	}

	origRequires := make(map[string]*modfile.Require)
	for _, mod := range origFile.Require {
		origRequires[mod.Mod.Path] = mod
	}

	newFile := &modfile.File{}
	newFile.AddModuleStmt("test")
	newFile.AddRequire(origFile.Module.Mod.Path, "v1.0.0")
	newFile.AddGoStmt(origFile.Go.Version)
	for _, pkg := range requires {
		require, ok := origRequires[pkg]
		if !ok {
			return fmt.Errorf("could not find pkg %s", pkg)
		}
		newFile.AddRequire(require.Mod.Path, require.Mod.Version)
	}
	newFile.AddReplace(origFile.Module.Mod.Path, "", goSimModDir, "")
	newModBytes, err := newFile.Format()
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(testModDir, "go.mod"), newModBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(testModDir, "go.sum"), origSumBytes, 0o644); err != nil {
		return err
	}
	return nil
}

func isUncachedTestRun() bool {
	// TODO: fallback only allowed if -count=1 (which bans caching)
	flags := []string{"-test.count="}

	for _, arg := range os.Args {
		for _, flag := range flags {
			// if we have this flag and it looks non-empty, assume non-cached test run
			if strings.HasPrefix(arg, flag) && len(arg) > len(flag) {
				return true
			}
		}
	}

	return false
}

func ReplaceSpecialPackages(s string) string {
	changed := false
	parts := strings.Split(s, "/")
	for i, part := range parts {
		if part == "vendor" || part == "internal" {
			parts[i] = part + "_"
			changed = true
		}
	}
	if !changed {
		return s
	}
	return strings.Join(parts, "/")
}

func PreparedTestBinName(pkg string) string {
	return strings.ReplaceAll(pkg, "/", "-") + ".test"
}

func PreparedTestInfoName(pkg string) string {
	return strings.ReplaceAll(pkg, "/", "-") + ".info"
}

func GetNewPackageName(pkg string) string {
	return "translated/" + ReplaceSpecialPackages(pkg)
}

var (
	buildOnceMu sync.Mutex
	buildOnce   = make(map[string]*sync.Once)
)

func GetPathForPrecompiledTestBinary(t *testing.T, pkg string, tags []string) string {
	didBuild := false

	// TODO: test this race stuff somehow. make test output if race is enabled?
	cfg := BuildConfig{
		GOOS:   "linux",
		GOARCH: runtime.GOARCH,
		Race:   race.Enabled,
		Tags:   tags,
	}

	if isUncachedTestRun() {
		// TODO: figure out if we should pass -race or not?
		buildOnceMu.Lock()
		key := cfg.AsDirname() + " " + pkg
		once, ok := buildOnce[key]
		if !ok {
			once = new(sync.Once)
			buildOnce[key] = once
		}
		buildOnceMu.Unlock()

		didBuild = true
		once.Do(func() {
			var name string
			var flags []string
			// The command is a module of its own, so go run works only
			// in a module that requires it; GOSIMTOOL names a built one.
			if prebuilt, ok := os.LookupEnv("GOSIMTOOL"); ok {
				name = prebuilt
			} else {
				name = "go"
				flags = []string{"run", Module + "/cmd/gosim"}
			}
			flags = append(flags, "build-tests")
			if race.Enabled {
				flags = append(flags, "-race")
			}
			if len(tags) > 0 {
				flags = append(flags, "-tags", strings.Join(tags, ","))
			}
			flags = append(flags, pkg)
			cmd := exec.Command(name, flags...)
			t.Logf("looks like an uncached test run... running `%s`", strings.Join(cmd.Args, " "))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("gosim build-tests failed: %s\n\n%s\nSet GOSIMTOOL to the path of a gosim binary to use it instead of go run.", err, string(out))
			}
		})
	}

	goModDir, err := FindGoModDir()
	if err != nil {
		t.Fatalf("Could not find containing go module: %s", err)
	}

	outDir := filepath.Join(goModDir, OutputDirectory, "metatest", cfg.AsDirname())
	path := filepath.Join(outDir, PreparedTestBinName(GetNewPackageName(pkg)))
	infoPath := filepath.Join(outDir, PreparedTestInfoName(GetNewPackageName(pkg)))

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Could not find pre-built gosim test binary for package %q. Pre-building is necessary to support the go test cache. "+
				"Pre-build the test binary using `go run %s/cmd/gosim build-tests %s` or "+
				"run `go test` with `-count=1` to disable the go test cache and build during the test.", pkg, Module, pkg)
		}
	}

	if !didBuild {
		t.Logf("Using pre-built gosim test binary %q", path)

		// TODO: only do this once per binary?
		// TODO: maybe this is less than optimal... now if files get touched but the binary doesn't change
		// we still re-run tests... worthwhile trade-off to catch misuse?

		// check if the precompiled test binary is up-to-date "enough"
		bytes, err := os.ReadFile(infoPath)
		if err != nil {
			t.Fatalf("failed to read info file: %s", err)
		}
		var deps map[string]time.Time
		if err := json.Unmarshal(bytes, &deps); err != nil {
			t.Fatalf("failed to parse info file: %s", err)
		}
		for dep, ts := range deps {
			info, err := os.Stat(dep)
			if err != nil {
				t.Fatalf("failed to stat dependency %q: %s", dep, err)
			}
			if !ts.Equal(info.ModTime()) {
				t.Fatalf("Pre-built gosim test binary is out of date. Update binaries with gosim build-tests or run an uncached run with -count=1. See the documentation for metatesting. Input %q changed: %s vs %s.", dep, ts, info.ModTime())
			}
		}
	}

	return path
}
