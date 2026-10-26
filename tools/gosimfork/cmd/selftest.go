package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"

	"github.com/jellevandenhooff/gosim/cmd/gosim/internal/translate"
	"github.com/jellevandenhooff/gosim/internal/gosimtool"
)

// prepareSelftest prepares for running tests in the gosim module.
func prepareSelftest() {
	modDir, err := gosimtool.FindGoModDir()
	if err != nil {
		log.Fatal(err)
	}

	buildDir, err := os.MkdirTemp("", "gosimbuild")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(buildDir)

	// Prepare the racetest binary
	raceBuildPath := filepath.Join(buildDir, "testdata.test")
	raceFinalPath := filepath.Join(modDir, gosimtool.OutputDirectory, "racetest", "testdata.test")
	if err := os.MkdirAll(filepath.Dir(raceFinalPath), 0o755); err != nil {
		log.Fatal(err)
	}
	// try to reuse the old binary
	if err := copyFile(raceFinalPath, raceBuildPath, 0o755); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("copying source: %s", err)
		}
	}
	// do the build
	name := "go"
	args := []string{"test"}
	args = append(args, "-ldflags=-checklinkname=0", "-tags=linkname")
	args = append(args, "-race")
	args = append(args, "-c")
	args = append(args, "-o", buildDir)
	args = append(args, "./internal/tests/race/testdata")
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		log.Fatal(err)
	}
	// place the binary
	if err := copyFileIfChanged(raceBuildPath, raceFinalPath, 0o755); err != nil {
		log.Fatalf("copying final: %s", err)
	}

	// Prepare the gosim binary
	// TODO: should we have a -race version as well?
	gosimBuildPath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	gosimFinalPath := filepath.Join(modDir, gosimtool.OutputDirectory, "scripttest", "bin", "gosim")
	if err := os.MkdirAll(filepath.Dir(gosimFinalPath), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := copyFileIfChanged(gosimBuildPath, gosimFinalPath, 0o755); err != nil {
		log.Fatalf("copying final: %s", err)
	}

	// Prepare the module
	copyModuleForScriptTest(modDir)
}

// copyModuleForScriptTest copies the source code of this module to a slimmed-down
// version for the script tests in ./internal/tests to make them interact nicely
// with the go test cache.
func copyModuleForScriptTest(modDir string) {
	pkgs, err := packages.Load(&packages.Config{
		Mode:  packages.NeedImports | packages.NeedFiles | packages.NeedDeps | packages.NeedName,
		Tests: false,
	}, gosimtool.Module+"/...")
	if err != nil {
		log.Fatal(err)
	}

	keep := make(map[string]bool)
	for _, pkg := range translate.TranslatedRuntimePackages {
		keep[pkg] = true
	}

	var mark func(pkg *packages.Package)
	seen := make(map[*packages.Package]bool)
	mark = func(pkg *packages.Package) {
		if !strings.HasPrefix(pkg.PkgPath, gosimtool.Module) {
			return
		}

		if seen[pkg] {
			return
		}
		seen[pkg] = true

		for _, dep := range pkg.Imports {
			mark(dep)
		}
	}

	for _, pkg := range pkgs {
		if (strings.Contains(pkg.PkgPath, "/internal/") || strings.HasSuffix(pkg.PkgPath, "/internal")) && !keep[pkg.PkgPath] {
			// log.Println("skipping internal", pkg.PkgPath)
			continue
		}
		if pkg.Name == "main" {
			// log.Println("skipping main", pkg.PkgPath)
			continue
		}
		// log.Println(pkg.PkgPath)
		mark(pkg)
	}

	var files []string
	wanted := make(map[string]bool)

	for pkg := range seen {
		// log.Println("marked", pkg.PkgPath)
		// log.Println(pkg.GoFiles)
		files = append(files, pkg.GoFiles...)
		files = append(files, pkg.IgnoredFiles...)
	}

	files = append(files, filepath.Join(modDir, "go.mod"))
	files = append(files, filepath.Join(modDir, "go.sum"))

	extractedModDir := filepath.Join(modDir, gosimtool.OutputDirectory, "scripttest/mod")

	for _, file := range files {
		newP := filepath.Join(extractedModDir, strings.TrimPrefix(file, modDir))
		if err := os.MkdirAll(filepath.Dir(newP), 0o755); err != nil {
			log.Fatal(err)
		}
		copyFileIfChanged(file, newP, 0o644)
		wanted[newP] = true
	}

	err = filepath.WalkDir(extractedModDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if !wanted[path] {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// TODO: delete unwanted directories?
}
